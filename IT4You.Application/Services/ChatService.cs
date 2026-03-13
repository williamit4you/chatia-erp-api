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
    public ChatService(AppDbContext context, IConfiguration configuration, ILogger<ChatService> logger, IFinancialAgentFactory agentFactory)
    {
        _context = context;
        _configuration = configuration;
        _logger = logger;
        _agentFactory = agentFactory;
    }

    public async Task<IT4You.Application.DTOs.ChatResponse> ProcessMessageAsync(
            ChatRequest request,
            string userId,
            string tenantId)
            {
                _logger.LogInformation("Processing message for User: {UserId}, Tenant: {TenantId}", userId, tenantId);

                var tenant = await _context.Tenants.FindAsync(tenantId);
                if (tenant == null)
                {
                    Console.WriteLine($"!!! ERROR: Tenant {tenantId} not found in database.");
                    throw new Exception("Tenant não encontrado.");
                }

                Console.WriteLine($"[ProcessMessage] Found Tenant: {tenant.Name}. IaToken check (length): {(tenant.IaToken?.Length ?? 0)}");

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
                    Content = request.Message
                };

                _context.ChatMessages.Add(userMsg);

                var user = await _context.Users.FindAsync(userId);
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
                        tenant.IaToken, 
                        isFullAdmin || (user?.HasPayableChatAccess ?? false), 
                        isFullAdmin || (user?.HasReceivableChatAccess ?? false),
                        isFullAdmin || (user?.HasBankingChatAccess ?? false));

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

                    var response = await agent.RunAsync(messages);

                    var reply = response.Messages.LastOrDefault()?.Text ?? "(Sem resposta)";

                    // ================================
                    // 6️⃣ SALVA RESPOSTA DO MODELO
                    // ================================
                    var modelMsg = new IT4You.Domain.Entities.ChatMessage
                    {
                        SessionId = sessionId,
                        Role = "assistant",
                        Content = reply
                    };

                    _context.ChatMessages.Add(modelMsg);
                    await _context.SaveChangesAsync();

                    return new IT4You.Application.DTOs.ChatResponse(reply, sessionId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "ERROR IN IA AGENT: {Message}", ex.Message);

                    return new IT4You.Application.DTOs.ChatResponse(
                        "Erro ao processar sua solicitação: " + ex.Message,
                        sessionId
                    );
                }
            }

    public async Task<IT4You.Application.DTOs.ChatResponse> ProcessChartAnalysisAsync(ChartAnalysisRequest request, string userId, string tenantId)
    {
        _logger.LogInformation("Processing chart analysis for User: {UserId}, Chart: {ChartId}", userId, request.ChartId);

        var tenant = await _context.Tenants.FindAsync(tenantId);
        if (tenant == null) throw new Exception("Tenant não encontrado.");

        var user = await _context.Users.FindAsync(userId);

        try
        {
            bool isFullAdmin = user?.Role == UserRole.SUPER_ADMIN || 
                             user?.Role == UserRole.TENANT_ADMIN || 
                             user?.Role == UserRole.ADMIN;

            var agent = await _agentFactory.CreateAgentAsync(
                tenant.IaToken,
                isFullAdmin || (user?.HasPayableChatAccess ?? false),
                isFullAdmin || (user?.HasReceivableChatAccess ?? false),
                isFullAdmin || (user?.HasBankingChatAccess ?? false));

            var chartContext = $@"
# CONTEXTO DO GRÁFICO ATUAL (Ground Truth)
Você está analisando o gráfico: {request.ChartTitle}
Descrição do gráfico: {request.ChartDescription}

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

            var response = await agent.RunAsync(messages);
            var reply = response.Messages.LastOrDefault()?.Text ?? "(Sem resposta)";

            return new IT4You.Application.DTOs.ChatResponse(reply, "chart-analysis-" + request.ChartId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ERROR IN CHART ANALYSIS: {Message}", ex.Message);
            return new IT4You.Application.DTOs.ChatResponse("Erro ao analisar gráfico: " + ex.Message, "error");
        }
    }

    public async Task<List<ChatSession>> GetSessionsAsync(string userId, string tenantId)
    {
        return await _context.ChatSessions
            .Where(s => s.UserId == userId && s.TenantId == tenantId)
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