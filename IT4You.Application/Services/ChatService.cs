using IT4You.Application.DTOs;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System;
using IT4You.Application.Interfaces;
using IT4You.Domain.Entities;
using IT4You.Application.Plugins;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.SemanticKernel;
using Microsoft.Extensions.AI;
// 👇 Importação do novo Microsoft Agent Framework
using Microsoft.Agents.AI;
using IT4You.Application.Data;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Text.Json;
using OpenAI;
using System.ClientModel;
using IT4You.Application.AI.Routing;

namespace IT4You.Application.Services;

public class ChatService : IChatService
{
    private readonly AppDbContext _context;
    private readonly IConfiguration _configuration;
    private readonly ILogger<ChatService> _logger;
    private readonly IFinancialAgentFactory _agentFactory;
    private readonly IBudgetAgentFactory _budgetAgentFactory;
    private readonly ErpPlugin _erpPlugin;
    private readonly BudgetPlugin _budgetPlugin;
    private readonly IMemoryCache _cache;
    private readonly ChatModuleRouter _chatModuleRouter;

    public ChatService(AppDbContext context, IConfiguration configuration, ILogger<ChatService> logger,
        IFinancialAgentFactory agentFactory, IBudgetAgentFactory budgetAgentFactory, ErpPlugin erpPlugin, BudgetPlugin budgetPlugin, IMemoryCache cache, ChatModuleRouter chatModuleRouter)
    {
        _context = context;
        _configuration = configuration;
        _logger = logger;
        _agentFactory = agentFactory;
        _budgetAgentFactory = budgetAgentFactory;
        _erpPlugin = erpPlugin;
        _budgetPlugin = budgetPlugin;
        _cache = cache;
        _chatModuleRouter = chatModuleRouter;
    }

    public async Task<IT4You.Application.DTOs.ChatResponse> ProcessMessageAsync(
            ChatRequest request,
            string userId,
            string tenantId)
            {
                _logger.LogInformation("Processing message for User: {UserId}, Tenant: {TenantId}", userId, tenantId);

                var user = await _context.Users.FindAsync(userId);
                if (user == null) throw new Exception("Usuário não encontrado.");

                if (user.BlockedUntil.HasValue && user.BlockedUntil.Value > DateTime.UtcNow)
                {
                    var dataFormatada = user.BlockedUntil.Value.ToString("dd/MM/yyyy");
                    throw new Exception($"sem token para utilização, liberação após o dia {dataFormatada}");
                }

                var tenant = await _context.Tenants.FindAsync(tenantId);
                if (tenant == null)
                {
                    Console.WriteLine($"!!! ERROR: Tenant {tenantId} not found in database.");
                    throw new Exception("Tenant não encontrado.");
                }

                Console.WriteLine($"[ProcessMessage] Found Tenant: {tenant.Name}. ChatAiToken check (length): {(tenant.ChatAiToken?.Length ?? 0)}");

                // ================================
                // 1️⃣ GERENCIAMENTO DE SESSÃO
                // ================================
                var sessionId = request.SessionId;

                if (string.IsNullOrEmpty(sessionId))
                {
                    var newSession = new ChatSession
                    {
                        UserId = userId,
                        TenantId = tenantId,
                        Title = request.Message.Length > 30
                            ? request.Message.Substring(0, 30) + "..."
                            : request.Message
                    };

                    _context.ChatSessions.Add(newSession);
                    await _context.SaveChangesAsync();
                    sessionId = newSession.Id;

                    _logger.LogInformation("Created new ChatSession: {SessionId}", sessionId);
                }

                // ================================
                // 2️⃣ SALVA MENSAGEM DO USUÁRIO
                // ================================
                var lastModuleText = await _context.ChatMessages
                    .Where(m => m.SessionId == sessionId)
                    .OrderByDescending(m => m.CreatedAt)
                    .Select(m => m.Module)
                    .FirstOrDefaultAsync();

                ChatModule? previousModule = lastModuleText switch
                {
                    "Orcamento" => ChatModule.Orcamento,
                    "Financeiro" => ChatModule.Financeiro,
                    _ => null
                };

                var route = _chatModuleRouter.Route(request.Message, previousModule);
                var moduleLabel = route.Module switch
                {
                    ChatModule.Orcamento => "Orcamento",
                    ChatModule.Financeiro => "Financeiro",
                    _ => "Sistema"
                };

                var userMsg = new IT4You.Domain.Entities.ChatMessage
                {
                    SessionId = sessionId,
                    Role = "user",
                    Content = request.Message,
                    Module = moduleLabel
                };

                _context.ChatMessages.Add(userMsg);

                // var user = await _context.Users.FindAsync(userId); // Já buscado acima
                if (user != null)
                    user.QueryCount++;

                await _context.SaveChangesAsync();

                try
                {
                    // ================================
                    // 3️⃣ CRIA O AGENTE
                    // ================================
                    if (route.NeedsClarification)
                    {
                        var clarificationReply = route.ClarificationPrompt ?? "Voce quer consultar Financeiro ou Orcamento?";

                        var clarificationMsg = new IT4You.Domain.Entities.ChatMessage
                        {
                            SessionId = sessionId,
                            Role = "assistant",
                            Content = clarificationReply,
                            Module = "Sistema"
                        };

                        _context.ChatMessages.Add(clarificationMsg);
                        await _context.SaveChangesAsync();

                        return new IT4You.Application.DTOs.ChatResponse(clarificationReply, sessionId, null, 0);
                    }

                    bool isFullAdmin = user?.Role == UserRole.SUPER_ADMIN || 
                                     user?.Role == UserRole.TENANT_ADMIN || 
                                     user?.Role == UserRole.ADMIN;

                    if (route.Module == ChatModule.Orcamento && !(isFullAdmin || (user?.HasBudgetChatAccess ?? false)))
                    {
                        const string blockedReply = "Esse questionamento e somente para usuarios do modulo de orcamento";

                        var blockedMsg = new IT4You.Domain.Entities.ChatMessage
                        {
                            SessionId = sessionId,
                            Role = "assistant",
                            Content = blockedReply,
                            Module = "Orcamento"
                        };

                        _context.ChatMessages.Add(blockedMsg);
                        await _context.SaveChangesAsync();

                        return new IT4You.Application.DTOs.ChatResponse(blockedReply, sessionId, null, 0);
                    }

                    IChatQueryPlugin queryPlugin;
                    AIAgent agent;

                    if (route.Module == ChatModule.Orcamento)
                    {
                        queryPlugin = _budgetPlugin;
                        agent = await _budgetAgentFactory.CreateAgentAsync(
                            tenant.ChatAiToken,
                            tenant.IaToken,
                            isFullAdmin || (user?.HasBudgetChatAccess ?? false),
                            request.Message,
                            userId);
                    }
                    else
                    {
                        queryPlugin = _erpPlugin;
                        agent = await _agentFactory.CreateAgentAsync(
                            tenant.ChatAiToken,
                            tenant.IaToken,
                            isFullAdmin || (user?.HasPayableChatAccess ?? false),
                            isFullAdmin || (user?.HasReceivableChatAccess ?? false),
                            isFullAdmin || (user?.HasBankingChatAccess ?? false),
                            request.Message, userId);
                    }

                    // ================================
                    // 4️⃣ MONTA HISTÓRICO
                    // ================================
                    var messages = new List<Microsoft.Extensions.AI.ChatMessage>();

                    if (request.History != null && request.History.Any())
                    {
                        foreach (var msg in request.History)
                        {
                            messages.Add(new Microsoft.Extensions.AI.ChatMessage(
                                msg.Role == "user"
                                    ? Microsoft.Extensions.AI.ChatRole.User
                                    : Microsoft.Extensions.AI.ChatRole.Assistant,
                                msg.Content
                            ));
                        }
                    }

                    // Adiciona a nova pergunta
                    messages.Add(new Microsoft.Extensions.AI.ChatMessage(
                        Microsoft.Extensions.AI.ChatRole.User,
                        request.Message
                    ));

                    // ================================
                    // 5️⃣ EXECUTA A IA (TOOLS + ORQUESTRAÇÃO AUTOMÁTICA)
                    // ================================
                    _logger.LogInformation("Calling agent with {Count} messages", messages.Count);

                    queryPlugin.ClearExecutedQueries();
                    queryPlugin.ClearExportMetadata(); // limpa estado do export anterior

                    var response = await agent.RunAsync(messages);
                    var sqlJson = queryPlugin.GetExecutedQueriesJson();

                    var reply = response.Messages.LastOrDefault()?.Text ?? "(Sem resposta)";

                    // Lê export metadata diretamente do plugin (confiável, sem regex no texto da IA)
                    string? exportId = queryPlugin.LastExportId;
                    int exportTotal = queryPlugin.LastExportTotalLinhas;
                    decimal exportValor = queryPlugin.LastExportValorTotal;
                    int metricsTotal = queryPlugin.AggregateTotalLinhas;
                    decimal metricsValor = queryPlugin.AggregateValorTotal;

                    // ================================
                    // 6️⃣ SALVA RESPOSTA DO MODELO
                    // ================================
                    var modelMsg = new IT4You.Domain.Entities.ChatMessage
                    {
                        SessionId = sessionId,
                        Role = "assistant",
                        Content = reply,
                        SqlQueries = sqlJson,
                        Module = moduleLabel
                    };

                    _context.ChatMessages.Add(modelMsg);
                    await _context.SaveChangesAsync();

                    // ================================
                    // CALCULO DO SCORE DE CONTEXTO %
                    // ================================
                    // Estimativa segura: 128k tokens ≈ 512k caracteres (Limite do gpt-oss-120b)
                    int totalChars = messages.Sum(m => m.Text?.Length ?? 0) + reply.Length;
                    int contextPercent = (int)Math.Min(100, Math.Round((double)totalChars / 512000 * 100));

                    var rightRail = await BuildRightRailAsync(
                        userId,
                        tenant.ChatAiToken,
                        moduleLabel,
                        request.Message,
                        reply,
                        sqlJson,
                        exportId,
                        exportTotal,
                        exportValor);

                    return new IT4You.Application.DTOs.ChatResponse(reply, sessionId, isFullAdmin ? sqlJson : null, contextPercent, exportId, exportTotal, exportValor, metricsTotal, metricsValor, rightRail);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "ERROR IN IA AGENT: {Message}", ex.Message);

                    return new IT4You.Application.DTOs.ChatResponse(
                        "Erro ao processar sua solicitação: " + ex.Message,
                        sessionId,
                        null,
                        0
                    );
                }
            }

    public async Task<IT4You.Application.DTOs.ChatResponse> ProcessChartAnalysisAsync(ChartAnalysisRequest request, string userId, string tenantId)
    {
        _logger.LogInformation("Processing chart analysis for User: {UserId}, Chart: {ChartId}", userId, request.ChartId);

        var user = await _context.Users.FindAsync(userId);
        if (user == null) throw new Exception("Usuário não encontrado.");

        if (user.BlockedUntil.HasValue && user.BlockedUntil.Value > DateTime.UtcNow)
        {
            var dataFormatada = user.BlockedUntil.Value.ToString("dd/MM/yyyy");
            throw new Exception($"sem token para utilização, liberação após o dia {dataFormatada}");
        }

        var tenant = await _context.Tenants.FindAsync(tenantId);

        // ================================
        // GERENCIAMENTO DE SESSÃO (por usuário + gráfico)
        // ================================
        var sessionId = request.SessionId;
        if (string.IsNullOrEmpty(sessionId))
        {
            sessionId = request.ChartId != null 
                ? $"chart-{request.ChartId}-{userId}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}" 
                : $"chart-unknown-{userId}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        }

        var existingSession = await _context.ChatSessions.FindAsync(sessionId);
        if (existingSession == null)
        {
            var newSession = new ChatSession
            {
                Id = sessionId,
                UserId = userId,
                TenantId = tenantId,
                Title = $"Análise: {request.ChartTitle}"
            };
            _context.ChatSessions.Add(newSession);
            await _context.SaveChangesAsync();
        }

        // SALVA MENSAGEM DO USUÁRIO
        var userMsg = new IT4You.Domain.Entities.ChatMessage
        {
            SessionId = sessionId,
            Role = "user",
            Content = request.Message,
            Module = "Financeiro"
        };
        _context.ChatMessages.Add(userMsg);
        await _context.SaveChangesAsync();

        try
        {
            bool isFullAdmin = user?.Role == UserRole.SUPER_ADMIN || 
                             user?.Role == UserRole.TENANT_ADMIN || 
                             user?.Role == UserRole.ADMIN;

            var agent = await _agentFactory.CreateAgentAsync(
                tenant.ChatAiToken,
                tenant.IaToken,
                isFullAdmin || (user?.HasPayableChatAccess ?? false),
                isFullAdmin || (user?.HasReceivableChatAccess ?? false),
                isFullAdmin || (user?.HasBankingChatAccess ?? false),
                request.Message, userId);

            var dateContext = "";
            if (!string.IsNullOrEmpty(request.StartDate) || !string.IsNullOrEmpty(request.EndDate))
            {
                dateContext = $"\n# FILTRO DE PERÍODO ATIVO\nO usuário está filtrando os dados de {request.StartDate ?? "(sem início)"} até {request.EndDate ?? "(sem fim)"}. Priorize consultas neste intervalo de datas.";
            }

            var chartContext = $@"
                                    # CONTEXTO DO GRÁFICO ATUAL (Ground Truth)
                                    Você está analisando o gráfico: {request.ChartTitle}
                                    Descrição do gráfico: {request.ChartDescription}
                                    {dateContext}

                                    # DADOS DO GRÁFICO (JSON)
                                    {System.Text.Json.JsonSerializer.Serialize(request.ChartData)}

                                    # INSTRUÇÕES DE ANÁLISE
                                    1. O usuário está visualizando estes dados agora no dashboard. Priorize responder com base nestes dados.
                                    2. Se a pergunta for sobre tendências, faça cálculos simples com base no JSON fornecido.
                                    3. Se o usuário perguntar algo que transgrida o gráfico (ex: detalhes de um documento específico que não está no resumo), use suas ferramentas para consultar o ERP, MAS sempre relacione com o contexto do gráfico atual.
                                    4. Responda de forma executiva, clara e em Português (Brasil).";

            var messages = new List<Microsoft.Extensions.AI.ChatMessage>();
            
            // Adiciona o contexto como mensagem de sistema para orientar o agente
            messages.Add(new Microsoft.Extensions.AI.ChatMessage(Microsoft.Extensions.AI.ChatRole.System, chartContext));

            if (request.History != null)
            {
                foreach (var msg in request.History)
                {
                    messages.Add(new Microsoft.Extensions.AI.ChatMessage(
                        msg.Role == "user" ? Microsoft.Extensions.AI.ChatRole.User : Microsoft.Extensions.AI.ChatRole.Assistant,
                        msg.Content));
                }
            }

            messages.Add(new Microsoft.Extensions.AI.ChatMessage(Microsoft.Extensions.AI.ChatRole.User, request.Message));

            _logger.LogInformation("Calling agent for chart analysis with {Count} messages", messages.Count);

            _erpPlugin.ClearExecutedQueries();
            _erpPlugin.ClearExportMetadata();
            var response = await agent.RunAsync(messages);
            var sqlJson = _erpPlugin.GetExecutedQueriesJson();
            var reply = response.Messages.LastOrDefault()?.Text ?? "(Sem resposta)";

            // Lê export metadata diretamente do plugin
            string? exportId = _erpPlugin.LastExportId;
            int exportTotal = _erpPlugin.LastExportTotalLinhas;
            decimal exportValor = _erpPlugin.LastExportValorTotal;
            int metricsTotal = _erpPlugin.AggregateTotalLinhas;
            decimal metricsValor = _erpPlugin.AggregateValorTotal;

            // SALVA RESPOSTA DO MODELO COM SQL
            var modelMsg = new IT4You.Domain.Entities.ChatMessage
            {
                SessionId = sessionId,
                Role = "assistant",
                Content = reply,
                SqlQueries = sqlJson,
                Module = "Financeiro"
            };
            _context.ChatMessages.Add(modelMsg);
            await _context.SaveChangesAsync();

            // ================================
            // CALCULO DO SCORE DE CONTEXTO %
            // ================================
            // Estimativa segura: 128k tokens ≈ 512k caracteres (Limite do gpt-oss-120b)
            int totalChars = messages.Sum(m => m.Text?.Length ?? 0) + reply.Length;
            int contextPercent = (int)Math.Min(100, Math.Round((double)totalChars / 512000 * 100));

            var rightRail = await BuildRightRailAsync(
                userId,
                tenant.ChatAiToken,
                request.ChartTitle,
                request.Message,
                reply,
                sqlJson,
                exportId,
                exportTotal,
                exportValor);

            return new IT4You.Application.DTOs.ChatResponse(reply, sessionId, isFullAdmin ? sqlJson : null, contextPercent, exportId, exportTotal, exportValor, metricsTotal, metricsValor, rightRail);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ERROR IN CHART ANALYSIS: {Message}", ex.Message);
            return new IT4You.Application.DTOs.ChatResponse("Erro ao analisar gráfico: " + ex.Message, sessionId);
        }
    }

    public async Task<List<ChatSession>> GetSessionsAsync(string userId, string tenantId)
    {
        return await _context.ChatSessions
            .Where(s => s.UserId == userId && s.TenantId == tenantId && s.IsVisible)
            .OrderByDescending(s => s.CreatedAt)
            .ToListAsync();
    }

    public async Task<List<IT4You.Domain.Entities.ChatMessage>> GetMessagesAsync(string sessionId)
    {
        return await _context.ChatMessages
            .Where(m => m.SessionId == sessionId)
            .OrderBy(m => m.CreatedAt)
            .ToListAsync();
    }

    public async Task<List<SqlLogDto>> GetSqlLogsAsync(string tenantId, string? filterUserId = null, DateTime? startDate = null, DateTime? endDate = null)
    {
        var query = from msg in _context.ChatMessages
                    join session in _context.ChatSessions on msg.SessionId equals session.Id
                    join usr in _context.Users on session.UserId equals usr.Id
                    where session.TenantId == tenantId && msg.SqlQueries != null
                    select new { msg, session, usr };

        if (!string.IsNullOrEmpty(filterUserId))
            query = query.Where(x => x.session.UserId == filterUserId);

        if (startDate.HasValue)
            query = query.Where(x => x.msg.CreatedAt >= startDate.Value);

        if (endDate.HasValue)
            query = query.Where(x => x.msg.CreatedAt <= endDate.Value);

        var results = await query.OrderByDescending(x => x.msg.CreatedAt).Take(200).ToListAsync();

        var logs = new List<SqlLogDto>();
        foreach (var r in results)
        {
            // Find the user question that preceded this AI message in the same session
            var prevUserMsg = await _context.ChatMessages
                .Where(m => m.SessionId == r.session.Id && m.Role == "user" && m.CreatedAt < r.msg.CreatedAt)
                .OrderByDescending(m => m.CreatedAt)
                .FirstOrDefaultAsync();

            logs.Add(new SqlLogDto(
                r.msg.Id,
                DateTime.SpecifyKind(r.msg.CreatedAt, DateTimeKind.Utc),
                r.usr.Name,
                r.usr.Email,
                prevUserMsg?.Content ?? "(pergunta não encontrada)",
                r.msg.Content,
                r.msg.SqlQueries!
            ));
        }
        return logs;
    }

    public async Task<UsageHistoryDto> GetUsageHistoryAsync(string tenantId, int? month = null, int? year = null, DateTime? startDate = null, DateTime? endDate = null)
    {
        var query = from msg in _context.ChatMessages
                    join session in _context.ChatSessions on msg.SessionId equals session.Id
                    join usr in _context.Users on session.UserId equals usr.Id
                    where session.TenantId == tenantId && msg.Role == "user"
                    select new { msg, session, usr };

        if (startDate.HasValue)
            query = query.Where(x => x.msg.CreatedAt >= startDate.Value);

        if (endDate.HasValue)
            query = query.Where(x => x.msg.CreatedAt <= endDate.Value);

        var allResults = await query.ToListAsync();

        var modules = new[] { "Financeiro", "Estoque", "Vendas", "Produção", "Contrato", "Projetos" };

        // 1. Resumo Mensal (Summary Cards) - Sorted correctly by Year/Month
        var monthlyUsage = allResults
            .GroupBy(r => new { r.msg.CreatedAt.Year, r.msg.CreatedAt.Month })
            .Select(g => new { 
                Year = g.Key.Year, 
                Month = g.Key.Month,
                Dto = new MonthlyUsageDto(
                    $"{GetMonthName(g.Key.Month)}/{g.Key.Year.ToString().Substring(2)}",
                    g.Count(),
                    modules.ToDictionary(m => m, m => g.Count(x => x.msg.Module == m))
                )
            })
            .OrderByDescending(x => x.Year)
            .ThenByDescending(x => x.Month)
            .Select(x => x.Dto)
            .ToList();

        // 2. Histórico Detalhado (Details Table) - Potentially filtered by specific month/year
        var detailedResults = allResults;
        if (month.HasValue && year.HasValue)
        {
            detailedResults = allResults
                .Where(x => x.msg.CreatedAt.Month == month.Value && x.msg.CreatedAt.Year == year.Value)
                .ToList();
        }

        var detailedUsage = detailedResults
            .GroupBy(r => (r.usr.Name ?? r.usr.Email))
            .Select(g => new UserUsageDto(
                g.Key,
                g.Count(),
                modules.ToDictionary(m => m, m => g.Count(x => x.msg.Module == m))
            ))
            .OrderByDescending(x => x.TotalCount)
            .ToList();

        return new UsageHistoryDto(monthlyUsage, detailedUsage);
    }

    private string GetMonthName(int month)
    {
        return month switch
        {
            1 => "jan", 2 => "fev", 3 => "mar", 4 => "abr", 5 => "mai", 6 => "jun",
            7 => "jul", 8 => "ago", 9 => "set", 10 => "out", 11 => "nov", 12 => "dez",
            _ => ""
        };
    }

    public async Task DeleteSessionAsync(string sessionId, string tenantId)
    {
        var session = await _context.ChatSessions
            .FirstOrDefaultAsync(s => s.Id == sessionId && s.TenantId == tenantId);

        if (session != null)
        {
            // Soft delete: marca como invisível para o usuário, mas mantém os dados para análise da gestão
            session.IsVisible = false;
            await _context.SaveChangesAsync();
        }
    }

    private async Task<ChatRightRail> BuildRightRailAsync(
        string userId,
        string chatAiToken,
        string contextLabel,
        string userMessage,
        string reply,
        string? sqlJson,
        string? exportId,
        int exportTotal,
        decimal exportValor)
    {
        var aiRightRail = await GenerateRightRailWithAiAsync(
            chatAiToken,
            contextLabel,
            userMessage,
            reply,
            sqlJson,
            exportId,
            exportTotal,
            exportValor);

        var suggestions = aiRightRail?.Suggestions?.Count > 0
            ? aiRightRail.Suggestions
            : BuildSuggestions(contextLabel, userMessage, reply, sqlJson, exportId);

        var insights = aiRightRail?.Insights?.Count > 0
            ? aiRightRail.Insights
            : BuildInsights(contextLabel, userMessage, reply, sqlJson, exportId, exportTotal, exportValor);

        var favoriteQuestionsRaw = await _context.FavoriteQuestions
            .Where(f => f.UserId == userId)
            .OrderByDescending(f => f.CreatedAt)
            .Take(3)
            .Select(f => f.QuestionText)
            .ToListAsync();

        var favoriteQuestions = favoriteQuestionsRaw
            .Select(questionText => new ChatRightRailActionItem(questionText, "favorite:run"))
            .ToList();

        return new ChatRightRail(suggestions, insights, favoriteQuestions);
    }

    private async Task<ChatRightRail?> GenerateRightRailWithAiAsync(
        string chatAiToken,
        string contextLabel,
        string userMessage,
        string reply,
        string? sqlJson,
        string? exportId,
        int exportTotal,
        decimal exportValor)
    {
        if (string.IsNullOrWhiteSpace(chatAiToken))
            return null;

        try
        {
            var groqOptions = new OpenAIClientOptions
            {
                Endpoint = new Uri("https://api.groq.com/openai/v1")
            };

            IChatClient chatClient = new OpenAI.Chat.ChatClient(
                "openai/gpt-oss-120b",
                new ApiKeyCredential(chatAiToken),
                groqOptions
            ).AsIChatClient();

            var instructions = """
                Você é responsável por gerar itens de navegação da lateral direita de um chat corporativo.
                Sua saída deve ser SOMENTE JSON válido, sem markdown, sem explicações e sem texto fora do JSON.

                Gere:
                - exatamente 5 sugestões
                - exatamente 3 insights

                Regras:
                - as sugestões devem ser perguntas curtas, naturais e clicáveis
                - os insights devem ser coerentes com a pergunta e com os dados da resposta
                - não invente métricas que não estejam implícitas ou explícitas na resposta
                - se houver números, percentuais, clientes, títulos, risco ou valores monetários, use-os para dar contexto
                - prefira linguagem executiva em português do Brasil
                - evite sugestões genéricas quando houver dados concretos
                - é proibido gerar insights ou CTAs sobre SQL, query, script, consulta técnica, metadados internos, botões de download ou detalhes de implementação
                - os insights devem focar somente em leitura de negócio, risco, valor, concentração, tendência, prioridade e próxima ação

                Formato obrigatório:
                {
                  "suggestions": [
                    { "label": "..." }
                  ],
                  "insights": [
                    {
                      "title": "...",
                      "description": "...",
                      "ctaLabel": "...",
                      "ctaAction": "chat:ask:..."
                    }
                  ]
                }
                """;

            var prompt = $"""
                Contexto atual: {contextLabel}
                Pergunta do usuário: {userMessage}

                Resposta gerada:
                {reply}

                Exportação disponível: {(string.IsNullOrWhiteSpace(exportId) ? "não" : "sim")}
                Total exportado: {exportTotal}
                Valor exportado: {exportValor}
                """;

            var agent = chatClient.AsAIAgent(new ChatClientAgentOptions
            {
                Name = "RightRailGenerator",
                ChatOptions = new ChatOptions
                {
                    Temperature = 0.2f,
                    Instructions = instructions
                }
            });

            var response = await agent.RunAsync(new[]
            {
                new Microsoft.Extensions.AI.ChatMessage(Microsoft.Extensions.AI.ChatRole.User, prompt)
            });

            var content = response.Messages.LastOrDefault()?.Text;
            if (string.IsNullOrWhiteSpace(content))
                return null;

            return ParseAiRightRail(content);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Falha ao gerar right rail por IA. Aplicando fallback heurístico.");
            return null;
        }
    }

    private ChatRightRail? ParseAiRightRail(string content)
    {
        try
        {
            var json = ExtractJsonObject(content);
            if (string.IsNullOrWhiteSpace(json))
                return null;

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var suggestions = new List<ChatRightRailActionItem>();
            if (root.TryGetProperty("suggestions", out var suggestionsElement) && suggestionsElement.ValueKind == JsonValueKind.Array)
            {
                suggestions = suggestionsElement.EnumerateArray()
                    .Select(item => item.TryGetProperty("label", out var labelProp) ? labelProp.GetString() : null)
                    .Where(label => !string.IsNullOrWhiteSpace(label))
                    .Select(label => new ChatRightRailActionItem(label!, "chat:ask"))
                    .DistinctBy(item => item.Label)
                    .Take(5)
                    .ToList();
            }

            var insights = new List<ChatRightRailInsightItem>();
            if (root.TryGetProperty("insights", out var insightsElement) && insightsElement.ValueKind == JsonValueKind.Array)
            {
                insights = insightsElement.EnumerateArray()
                    .Select(item =>
                    {
                        var title = item.TryGetProperty("title", out var titleProp) ? titleProp.GetString() : null;
                        var description = item.TryGetProperty("description", out var descriptionProp) ? descriptionProp.GetString() : null;
                        var ctaLabel = item.TryGetProperty("ctaLabel", out var ctaLabelProp) ? ctaLabelProp.GetString() : null;
                        var ctaAction = item.TryGetProperty("ctaAction", out var ctaActionProp) ? ctaActionProp.GetString() : null;

                        if (string.IsNullOrWhiteSpace(title) || string.IsNullOrWhiteSpace(description))
                            return null;

                        if (!string.IsNullOrWhiteSpace(ctaAction) && !ctaAction.StartsWith("chat:ask:", StringComparison.OrdinalIgnoreCase))
                            ctaAction = $"chat:ask:{ctaAction}";

                        return new ChatRightRailInsightItem(title!, description!, ctaLabel, ctaAction, "neutral");
                    })
                    .Where(item => item != null)
                    .Select(item => item!)
                    .Where(item => !IsTechnicalInsight(item))
                    .DistinctBy(item => item.Title)
                    .Take(3)
                    .ToList();
            }

            if (suggestions.Count == 0 && insights.Count == 0)
                return null;

            return new ChatRightRail(suggestions, insights, new List<ChatRightRailActionItem>());
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Falha ao interpretar JSON de right rail gerado por IA.");
            return null;
        }
    }

    private string? ExtractJsonObject(string content)
    {
        var start = content.IndexOf('{');
        var end = content.LastIndexOf('}');

        if (start < 0 || end <= start)
            return null;

        return content.Substring(start, end - start + 1);
    }

    private List<ChatRightRailActionItem> BuildSuggestions(
        string contextLabel,
        string userMessage,
        string reply,
        string? sqlJson,
        string? exportId)
    {
        var normalizedContext = NormalizeContext($"{contextLabel} {userMessage} {reply}");
        var suggestions = new List<string>();

        if (ContainsAny(normalizedContext, "receber", "cliente", "inadimpl", "cobranc", "titulo"))
        {
            suggestions.Add("Quais clientes mais atrasam?");
            suggestions.Add("Qual a previsao de recebimento para os proximos 7 dias?");
            suggestions.Add("Compare com a semana anterior.");
            suggestions.Add("Mostre os titulos vencidos e nao pagos.");
            suggestions.Add("Quais recebimentos concentram maior risco?");
        }

        if (ContainsAny(normalizedContext, "pagar", "fornecedor", "pagamento"))
        {
            suggestions.Add("Quais fornecedores concentram mais pagamentos?");
            suggestions.Add("O que vence nos proximos 7 dias?");
            suggestions.Add("Compare os pagamentos com o periodo anterior.");
            suggestions.Add("Existe risco de aperto de caixa?");
            suggestions.Add("Quais despesas merecem prioridade?");
        }

        if (ContainsAny(normalizedContext, "caixa", "fluxo", "saldo", "banc"))
        {
            suggestions.Add("Qual o fluxo de caixa projetado para 30 dias?");
            suggestions.Add("Em que data o caixa fica mais pressionado?");
            suggestions.Add("Quais entradas sustentam o saldo previsto?");
            suggestions.Add("Quais saidas pesam mais no periodo?");
            suggestions.Add("Qual acao reduz o risco de caixa no curto prazo?");
        }

        if (!string.IsNullOrWhiteSpace(sqlJson))
        {
            suggestions.Add("Explique de forma executiva o que os dados mostram.");
            suggestions.Add("Quais registros merecem acompanhamento imediato?");
        }

        if (!string.IsNullOrWhiteSpace(exportId))
        {
            suggestions.Add("Resuma os dados exportados em 3 pontos.");
        }

        suggestions.Add("Qual a principal recomendacao para essa analise?");

        return suggestions
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Take(5)
            .Select(s => new ChatRightRailActionItem(s, "chat:ask"))
            .ToList();
    }

    private List<ChatRightRailInsightItem> BuildInsights(
        string contextLabel,
        string userMessage,
        string reply,
        string? sqlJson,
        string? exportId,
        int exportTotal,
        decimal exportValor)
    {
        var normalizedContext = NormalizeContext($"{contextLabel} {userMessage} {reply}");
        var insights = new List<ChatRightRailInsightItem>();
        var currencyValues = ExtractCurrencyValues(reply);
        var percentageValues = ExtractPercentageValues(reply);
        var titleCount = ExtractCountNearKeyword(reply, "titulos", "títulos");
        var clientCount = ExtractCountNearKeyword(reply, "clientes", "cliente");
        decimal? totalValue = currencyValues.Count > 0 ? currencyValues[0] : null;
        var riskPercent = ExtractRiskPercentage(reply, percentageValues);

        if (ContainsAny(normalizedContext, "receber", "contas a receber", "recebimento"))
        {
            if (totalValue.HasValue)
            {
                insights.Add(new ChatRightRailInsightItem(
                    "Volume total a receber no período",
                    $"A resposta indica um montante de {totalValue.Value.ToString("C", new System.Globalization.CultureInfo("pt-BR"))} dentro do recorte consultado.",
                    "Ver previsão",
                    "chat:ask:Qual a previsão de recebimento desse valor?",
                    "positive"));
            }

            if (titleCount.HasValue || clientCount.HasValue)
            {
                var distributionParts = new List<string>();
                if (titleCount.HasValue) distributionParts.Add($"{titleCount.Value} títulos");
                if (clientCount.HasValue) distributionParts.Add($"{clientCount.Value} clientes");

                insights.Add(new ChatRightRailInsightItem(
                    "Carteira distribuída na base",
                    $"O valor consultado está distribuído entre {string.Join(" e ", distributionParts)}, o que ajuda a avaliar dispersão e esforço de cobrança.",
                    "Ver clientes",
                    "chat:ask:Quais clientes concentram mais valor nesta carteira?",
                    "neutral"));
            }

            if (riskPercent.HasValue)
            {
                var tone = riskPercent.Value >= 20 ? "warning" : "positive";
                insights.Add(new ChatRightRailInsightItem(
                    "Risco de atraso da carteira",
                    $"{riskPercent.Value}% do montante possui sinal de risco ou atraso, o que pede priorização dos clientes mais sensíveis.",
                    "Ver análise",
                    "chat:ask:Quais clientes representam o maior risco de atraso?",
                    tone));
            }
        }

        if (ContainsAny(normalizedContext, "atras", "inadimpl", "vencid", "risco"))
        {
            insights.Add(new ChatRightRailInsightItem(
                "Risco de atraso identificado",
                "A conversa atual indica sinais de inadimplencia ou atraso. Vale aprofundar clientes, titulos vencidos e concentracao do risco.",
                "Ver analise",
                "chat:ask:Quais clientes concentram o maior risco de atraso?",
                "warning"));
        }

        if (exportTotal > 0 || exportValor > 0)
        {
            var description = exportValor > 0
                ? $"A ultima consulta retornou {exportTotal} registros com valor total de {exportValor.ToString("C", new System.Globalization.CultureInfo("pt-BR"))}."
                : $"A ultima consulta retornou {exportTotal} registros prontos para detalhamento e exportacao.";

            insights.Add(new ChatRightRailInsightItem(
                "Consulta com volume relevante",
                description,
                "Ver detalhes",
                "chat:ask:Mostre os principais destaques desta listagem.",
                "positive"));
        }

        if (ContainsAny(normalizedContext, "caixa", "saldo", "fluxo"))
        {
            insights.Add(new ChatRightRailInsightItem(
                "Pressao de caixa deve ser acompanhada",
                "Quando a conversa aborda fluxo e saldo, o proximo passo natural e revisar vencimentos proximos e concentracao de entradas.",
                "Ver impacto",
                "chat:ask:Qual o maior risco para o caixa nos proximos dias?",
                "warning"));
        }

        if (ContainsAny(normalizedContext, "cliente", "receber", "faturamento"))
        {
            insights.Add(new ChatRightRailInsightItem(
                "Receita pode estar concentrada",
                "Conversas sobre clientes e recebimentos costumam se beneficiar de uma leitura de concentracao e previsibilidade da carteira.",
                "Ver clientes",
                "chat:ask:Quais clientes concentram mais valor nesta analise?",
                "positive"));
        }

        insights.Add(new ChatRightRailInsightItem(
            "Proximo passo sugerido",
            "Aprofunde a leitura com comparativo de periodo, ranking e recomendacao pratica para transformar a resposta em acao.",
            "Continuar",
            "chat:ask:Qual deve ser minha proxima acao com base nessa analise?",
            "neutral"));

        return insights
            .Where(i => !IsTechnicalInsight(i))
            .DistinctBy(i => i.Title)
            .Take(3)
            .ToList();
    }

    private bool IsTechnicalInsight(ChatRightRailInsightItem item)
    {
        var combined = $"{item.Title} {item.Description} {item.CtaLabel} {item.CtaAction}";
        var normalized = NormalizeContext(combined);

        return ContainsAny(
            normalized,
            "sql",
            "query",
            "queries",
            "script",
            "consulta tecnica",
            "tecnico",
            "download",
            "excel",
            "pdf",
            "botao",
            "botoes",
            "arquivo",
            "arquivos"
        );
    }

    private List<decimal> ExtractCurrencyValues(string text)
    {
        var matches = Regex.Matches(text, @"R\$\s*([\d\.\,]+)");
        var values = new List<decimal>();

        foreach (Match match in matches)
        {
            if (match.Groups.Count < 2) continue;
            var raw = match.Groups[1].Value.Replace(".", "").Replace(",", ".");
            if (decimal.TryParse(raw, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var value))
            {
                values.Add(value);
            }
        }

        return values;
    }

    private List<int> ExtractPercentageValues(string text)
    {
        return Regex.Matches(text, @"(\d+)\s*%")
            .Select(match => int.TryParse(match.Groups[1].Value, out var value) ? value : -1)
            .Where(value => value >= 0)
            .ToList();
    }

    private int? ExtractCountNearKeyword(string text, params string[] keywords)
    {
        foreach (var keyword in keywords)
        {
            var escapedKeyword = Regex.Escape(keyword);
            var directMatch = Regex.Match(text, $@"(\d+)\s+{escapedKeyword}", RegexOptions.IgnoreCase);
            if (directMatch.Success && int.TryParse(directMatch.Groups[1].Value, out var directValue))
                return directValue;

            var reverseMatch = Regex.Match(text, $@"{escapedKeyword}[^\d]*(\d+)", RegexOptions.IgnoreCase);
            if (reverseMatch.Success && int.TryParse(reverseMatch.Groups[1].Value, out var reverseValue))
                return reverseValue;
        }

        return null;
    }

    private int? ExtractRiskPercentage(string reply, List<int> percentageValues)
    {
        var riskMatch = Regex.Match(reply, @"(\d+)\s*%[^\.\n]*(risco|atraso|inadimpl)", RegexOptions.IgnoreCase);
        if (riskMatch.Success && int.TryParse(riskMatch.Groups[1].Value, out var riskValue))
            return riskValue;

        return percentageValues.Count > 0 ? percentageValues[0] : null;
    }

    private string NormalizeContext(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var normalized = value.ToLowerInvariant();
        normalized = normalized
            .Replace("á", "a")
            .Replace("à", "a")
            .Replace("ã", "a")
            .Replace("â", "a")
            .Replace("é", "e")
            .Replace("ê", "e")
            .Replace("í", "i")
            .Replace("ó", "o")
            .Replace("ô", "o")
            .Replace("õ", "o")
            .Replace("ú", "u")
            .Replace("ç", "c");

        return Regex.Replace(normalized, "\\s+", " ").Trim();
    }

    private bool ContainsAny(string value, params string[] terms)
    {
        return terms.Any(term => value.Contains(term, StringComparison.OrdinalIgnoreCase));
    }
}

public static class ToolRegistry
{
    public static List<AITool> FromPlugin(object pluginInstance)
    {
        var tools = new List<AITool>();

        var methods = pluginInstance
            .GetType()
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m =>
                m.ReturnType == typeof(Task<string>) &&
                !m.IsSpecialName);

        foreach (var method in methods)
        {
            tools.Add(AIFunctionFactory.Create(method, pluginInstance));
        }

        return tools;
    }
}

public class SimpleAgent
{
    private readonly Microsoft.Extensions.AI.IChatClient _client;
    private readonly List<Microsoft.Extensions.AI.AITool> _tools;
    private readonly string _instructions;

    public SimpleAgent(Microsoft.Extensions.AI.IChatClient client, List<Microsoft.Extensions.AI.AITool> tools, string instructions)
    {
        _client = client;
        _tools = tools;
        _instructions = instructions;
    }

    public async Task<string> SendAsync(string userMessage, List<IT4You.Application.DTOs.ChatMessageDto>? history = null)
    {
        // 👇 CORREÇÃO: No Microsoft Agent Framework usamos o .AsAIAgent() para "encapar" o Client
        AIAgent agent = _client.AsAIAgent(
            name: "FinancialExpertAgent", // Opcional, mas recomendado para logs e rastreio
            instructions: _instructions,
            tools: _tools
        );

        var messages = new List<Microsoft.Extensions.AI.ChatMessage>();

        if (history != null)
        {
            foreach (var msg in history)
            {
                messages.Add(new Microsoft.Extensions.AI.ChatMessage(
                    msg.Role == "user" ? Microsoft.Extensions.AI.ChatRole.User : Microsoft.Extensions.AI.ChatRole.Assistant,
                    msg.Content));
            }
        }

        // Adicionando a última mensagem do usuário
        messages.Add(new Microsoft.Extensions.AI.ChatMessage(Microsoft.Extensions.AI.ChatRole.User, userMessage));

        // 👇 CORREÇÃO: O Agent Framework orquestra as tools e retorna um AgentResponse
        var response = await agent.RunAsync(messages);

        // Acessamos o texto diretamente pela propriedade Message gerada pelo LLM
        return response.Messages.LastOrDefault()?.Text ?? "(Sem resposta)";
    }
}
