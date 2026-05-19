using IT4You.Application.Interfaces;
using IT4You.Application.Plugins;
using Microsoft.Agents.AI;
using Microsoft.Extensions.AI;
using OpenAI;
using Pgvector.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using System.ClientModel;

namespace IT4You.Application.Services;

public class BudgetAgentFactory : IBudgetAgentFactory
{
    private readonly BudgetPlugin _budgetPlugin;
    private readonly IT4You.Application.Data.AppDbContext _context;

    public BudgetAgentFactory(BudgetPlugin budgetPlugin, IT4You.Application.Data.AppDbContext context)
    {
        _budgetPlugin = budgetPlugin;
        _context = context;
    }

    public async Task<AIAgent> CreateAgentAsync(string chatAiToken, string ragAiToken, bool hasBudgetChatAccess, string userInput = null, string userId = null)
    {
        if (string.IsNullOrWhiteSpace(chatAiToken))
            throw new ArgumentException("Chat AI Token (Groq) was not provided.", nameof(chatAiToken));

        var groqOptions = new OpenAIClientOptions
        {
            Endpoint = new Uri("https://api.groq.com/openai/v1")
        };

        IChatClient chatClient = new OpenAI.Chat.ChatClient(
            "openai/gpt-oss-120b",
            new ApiKeyCredential(chatAiToken),
            groqOptions
        ).AsIChatClient();

        var today = DateTime.Now.ToString("yyyy-MM-dd");

        var tools = new List<AITool>();
        if (hasBudgetChatAccess)
        {
            tools = ToolRegistry.FromPlugin(_budgetPlugin);
        }

        var ragKnowledge = "";
        if (!string.IsNullOrWhiteSpace(userInput))
        {
            try
            {
                if (!string.IsNullOrWhiteSpace(ragAiToken))
                {
                    var embeddingClient = new OpenAI.Embeddings.EmbeddingClient("text-embedding-3-small", ragAiToken);
                    var userEmbeddingResult = await embeddingClient.GenerateEmbeddingAsync(userInput);
                    var userVector = new Pgvector.Vector(userEmbeddingResult.Value.ToFloats().ToArray());

                    var fetchedMemories = await _context.AgentMemories
                        .Where(m => m.IsActive)
                        .Where(m => m.UserId == null || m.UserId == userId)
                        .Where(m => m.Embedding!.CosineDistance(userVector) < 0.65)
                        .OrderBy(m => m.Embedding!.CosineDistance(userVector))
                        .Take(3)
                        .ToListAsync();

                    if (fetchedMemories.Any())
                    {
                        var joined = string.Join("\n", fetchedMemories.Select(m => $"- {m.Content}"));
                        ragKnowledge = $@"
                        # CONHECIMENTO ESPECIFICO (RAG) - Prioridade Alta
                        As seguintes regras/memorias foram puxadas para responder esta pergunta. Respeite-as acima de tudo:
                        {joined}";
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BUDGET RAG FAIL] {ex.Message}");
            }
        }

        var regrasDeBloqueio = hasBudgetChatAccess
            ? "O usuario possui acesso ao modulo Orcamento. Execute consultas normalmente."
            : """
              - BLOQUEIO PRIORITARIO: O usuario NAO possui acesso ao modulo Orcamento.
              - NUNCA execute ferramentas de Orcamento.
              - Responda APENAS a frase de bloqueio abaixo.

              Frase de bloqueio mandataria:
              - ORCAMENTO NEGADO -> "Esse questionamento e somente para usuarios do modulo de orcamento"
              """;

        var systemInstructions = $"""
            # PERFIL
            Voce e um Analista Comercial/Orcamentos (IA) integrado ao ERP.
            DATA ATUAL: {today}

            {ragKnowledge}

            # 1. DIRETRIZES DE DADOS E EXECUCAO
            - PARAMETROS VAZIOS: Se o usuario nao citar filtros, preencha os parametros das ferramentas com STRINGS VAZIAS (""). NUNCA envie null.
            - FIDELIDADE: Relate exatamente os valores brutos retornados pelas ferramentas. Nao arredonde nem invente.
            - AGREGAÇÃO E CONTAGEM: Se o usuario perguntar "Quantos", "Qual a quantidade", "Qual o total", "Soma", "Valor total", voce DEVE usar agrupamento="TOTAL".
            - TOP N / MAIORES VALORES: Se o usuario pedir "top 10", "5 maiores", etc, use o parametro limite e ordenarPorMaiorValor=true.
            - CHAVE DE RELACIONAMENTO: Para perguntas que cruzam orcamento + itens, use ConsultarOrcamentosComItens (relacao por CODEMPRESA + ORCAMENTO).

            # 2. SEGURANCA E ACESSOS
            Permissao do usuario atual:
            - Orcamento (Chat): {(hasBudgetChatAccess ? "PERMITIDO" : "NEGADO")}

            REGRA DE BLOQUEIO:
            {regrasDeBloqueio}

            # 3. FORMATO DE RESPOSTA
            - Use tabelas em Markdown quando houver 3 ou mais itens.
            - Use **negrito** para valores totais.
            - Mostre valores monetarios como R$.

            # 4. IDIOMA
            - Responda obrigatoriamente em Portugues (Brasil).

            # 5. TOOLS
            As ferramentas disponiveis sao enviadas via esquema JSON. Leia atentamente as descricoes para escolher a ferramenta correta.
            """;

        var chatOptions = new ChatOptions
        {
            Temperature = 0,
            Tools = tools,
            Instructions = systemInstructions
        };

        var agentOptions = new ChatClientAgentOptions
        {
            Name = "BudgetExpertAgent",
            ChatOptions = chatOptions
        };

        return chatClient.AsAIAgent(agentOptions);
    }
}

