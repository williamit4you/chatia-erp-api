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

namespace IT4You.Application.Services;

public class ChatService : IChatService
{
    private readonly AppDbContext _context;
    private readonly IConfiguration _configuration;
    private readonly ILogger<ChatService> _logger;
    private readonly IFinancialAgentFactory _agentFactory;
    private readonly ErpPlugin _erpPlugin;
    private readonly IMemoryCache _cache;

    public ChatService(AppDbContext context, IConfiguration configuration, ILogger<ChatService> logger,
        IFinancialAgentFactory agentFactory, ErpPlugin erpPlugin, IMemoryCache cache)
    {
        _context = context;
        _configuration = configuration;
        _logger = logger;
        _agentFactory = agentFactory;
        _erpPlugin = erpPlugin;
        _cache = cache;
    }

    public async Task<IT4You.Application.DTOs.ChatResponse> ProcessMessageAsync(
            ChatRequest request,
            string userId,
            string tenantId)
            {
                _logger.LogInformation("Processing message for User: {UserId}, Tenant: {TenantId}", userId, tenantId);

                var user = await _context.Users.FindAsync(userId);
                if (user == null) throw new Exception("Usuário não encontrado.");

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
                var userMsg = new IT4You.Domain.Entities.ChatMessage
                {
                    SessionId = sessionId,
                    Role = "user",
                    Content = request.Message,
                    Module = "Financeiro"
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

                    _erpPlugin.ClearExecutedQueries();
                    _erpPlugin.ClearExportMetadata(); // limpa estado do export anterior

                    var response = await agent.RunAsync(messages);
                    var sqlJson = _erpPlugin.GetExecutedQueriesJson();

                    var reply = response.Messages.LastOrDefault()?.Text ?? "(Sem resposta)";

                    // Lê export metadata diretamente do plugin (confiável, sem regex no texto da IA)
                    string? exportId = _erpPlugin.LastExportId;
                    int exportTotal = _erpPlugin.LastExportTotalLinhas;
                    decimal exportValor = _erpPlugin.LastExportValorTotal;

                    // ================================
                    // 6️⃣ SALVA RESPOSTA DO MODELO
                    // ================================
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

                    return new IT4You.Application.DTOs.ChatResponse(reply, sessionId, isFullAdmin ? sqlJson : null, contextPercent, exportId, exportTotal, exportValor);
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

            return new IT4You.Application.DTOs.ChatResponse(reply, sessionId, isFullAdmin ? sqlJson : null, contextPercent, exportId, exportTotal, exportValor);
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
                r.msg.CreatedAt,
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

        if (month.HasValue && year.HasValue)
        {
            query = query.Where(x => x.msg.CreatedAt.Month == month.Value && x.msg.CreatedAt.Year == year.Value);
        }

        var results = await query.ToListAsync();

        var modules = new[] { "Financeiro", "Estoque", "Vendas", "Produção", "Contrato", "Projetos" };

        var monthlyUsage = results
            .GroupBy(r => new { r.msg.CreatedAt.Year, r.msg.CreatedAt.Month })
            .Select(g => new MonthlyUsageDto(
                $"{GetMonthName(g.Key.Month)}/{g.Key.Year.ToString().Substring(2)}",
                g.Count(),
                modules.ToDictionary(m => m, m => g.Count(x => x.msg.Module == m))
            ))
            .OrderByDescending(x => x.Month)
            .ToList();

        var detailedUsage = results
            .GroupBy(r => r.usr.Name ?? r.usr.Email)
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