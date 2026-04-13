
using IT4You.Application.Interfaces;
using IT4You.Application.Plugins;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI;
using Microsoft.Extensions.AI;
using OpenAI;
using Pgvector.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace IT4You.Application.Services
{
    public class FinancialAgentFactory : IFinancialAgentFactory
    {
        private readonly ErpPlugin _erpPlugin;
        private readonly IT4You.Application.Data.AppDbContext _context;

        // Injetamos o plugin aqui para ele já vir com a Configuration/DB injetada
        public FinancialAgentFactory(ErpPlugin erpPlugin, IT4You.Application.Data.AppDbContext context)
        {
            _erpPlugin = erpPlugin;
            _context = context;
        }

        public async Task<AIAgent> CreateAgentAsync(string iaToken, bool hasPayableChatAccess, bool hasReceivableChatAccess, bool hasBankingChatAccess, string userInput = null, string userId = null)
        {
            if (string.IsNullOrWhiteSpace(iaToken))
                throw new ArgumentException("IA Token was not provided.", nameof(iaToken));

            IChatClient chatClient = new OpenAI.Chat.ChatClient("gpt-4o-mini", iaToken).AsIChatClient();
            var allTools = ToolRegistry.FromPlugin(_erpPlugin);
            var today = DateTime.Now.ToString("yyyy-MM-dd");

            string dominio = "INDEFINIDO";
            if (!string.IsNullOrEmpty(userInput))
            {
                string pergunta = userInput.ToLower();
                if (pergunta.Contains("fornecedor") || pergunta.Contains("pagar") || pergunta.Contains("despesa") || pergunta.Contains("saída") || pergunta.Contains("saida"))
                {
                    dominio = "PAGAR";
                }
                else if (pergunta.Contains("cliente") || pergunta.Contains("receber") || pergunta.Contains("faturamento") || pergunta.Contains("entrada"))
                {
                    dominio = "RECEBER";
                }
            }

            var tools = new List<AITool>();
            foreach (var t in allTools)
            {
                // Isola a ferramenta baseada no nome (cortamos as ferramentas inversas ao domínio)
                if (dominio == "PAGAR" && t.Name.Contains("Receber", StringComparison.OrdinalIgnoreCase)) continue;
                if (dominio == "RECEBER" && t.Name.Contains("Pagar", StringComparison.OrdinalIgnoreCase)) continue;
                tools.Add(t);
            }

            string avisoDominio = dominio switch
            {
                "PAGAR" => "\nATENÇÃO: O domínio atual, identificado com base na mensagem do usuário, é **PAGAR**. FOQUE EXCLUSIVAMENTE em contabilidade de SAÍDAS/DESPESAS/FORNECEDORES e execute a ferramenta correta.",
                "RECEBER" => "\nATENÇÃO: O domínio atual, identificado com base na mensagem do usuário, é **RECEBER**. FOQUE EXCLUSIVAMENTE em contabilidade de ENTRADAS/RECEBIMENTOS/CLIENTES e execute a ferramenta correta.",
                _ => "\nO domínio exato (Pagar ou Receber) não pôde ser pré-determinado ou abrange ambos. Avalie com cuidado ou foque em ferramentas neutras (como Fluxo de Caixa) se aplicável."
            };

            // ============================================
            // 🧠 MOTOR DE RETRIEVAL (RAG) LOCAL P/ OPENAI
            // ============================================
            string ragKnowledge = "";
            if (!string.IsNullOrWhiteSpace(userInput))
            {
                try
                {
                    // 1. Gera Embedding da mensagem
                    var embeddingClient = new OpenAI.Embeddings.EmbeddingClient("text-embedding-3-small", iaToken);
                    var userEmbeddingResult = await embeddingClient.GenerateEmbeddingAsync(userInput);
                    var userVector = new Pgvector.Vector(userEmbeddingResult.Value.ToFloats().ToArray());

                    // 2. Busca Vetorial via EF Core + Cosine Distance
                    var fetchedMemories = await _context.AgentMemories
                        .Where(m => m.IsActive)
                        .Where(m => m.UserId == null || m.UserId == userId)
                        .Where(m => m.Embedding!.CosineDistance(userVector) < 0.65) // Só traz o que for REALMENTE similar ao contexto
                        .OrderBy(m => m.Embedding!.CosineDistance(userVector))
                        .Take(3)
                        .ToListAsync();

                    // 3. Monta string RAG
                    if (fetchedMemories.Any())
                    {
                        var joined = string.Join("\n", fetchedMemories.Select(m => $"- {m.Content}"));
                        ragKnowledge = $@"
                        # CONHECIMENTO ESPECÍFICO (RAG) - Prioridade Alta
                        As seguintes regras/memórias foram puxadas do seu cérebro para responder esta pergunta. Respeite-as acima de tudo:
                        {joined}";
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[RAG FAIL] {ex.Message}");
                }
            }

            // PROMPT OTIMIZADO: Focado em ação, precisão e resolução.
            var systemInstructions = @$"# PERFIL
                Você é um Analista Financeiro Sênior (IA) integrado ao ERP. Sua missão é fornecer dados precisos e confiavéis.
                DATA ATUAL: {today}
                {avisoDominio}
                
                {ragKnowledge}

                # 1. DIRETRIZES DE DADOS E EXECUÇÃO
                - PERÍODO: Se o usuário não citar datas, preencha os parâmetros da ferramenta com valores nulos. Apenas defina Data Fim se o usuário explicitamente fechar o escopo.
                - FIDELIDADE: Relate exatamente os valores brutos. Não arredonde e não faça cálculos manuais além do básico. Se a ferramenta retornar nada, diga R$ 0,00.
                - ORQUESTRAÇÃO: A ferramenta é flexível! Preencha APENAS os parâmetros que fizerem sentido para a pergunta. O C# montará a query ignorando os nulos. Se o usuário quiser agrupar por FORNECEDOR/ANO/MES forneça isso no parâmetro agrupamento. Se o usuário pedir apenas um valor total global absoluto, use o agrupamento TOTAL.

                # 2. SEGURANÇA E ACESSOS
                Você deve respeitar os status de acesso abaixo. Se tentar acessar um domínio NEGADO, retorne apenas a frase indicada:
                - Contas a PAGAR: {(hasPayableChatAccess ? "PERMITIDO" : "NEGADO")} -> (Frase: ""Esse questionamento é somente para usuários do conta a pagar"")
                - Contas a RECEBER: {(hasReceivableChatAccess ? "PERMITIDO" : "NEGADO")} -> (Frase: ""Esse questionamento é somente para usuários do conta a receber"")
                - Bancário/Saldos: {(hasBankingChatAccess ? "PERMITIDO" : "NEGADO")} -> (Frase: ""Esse questionamento é somente para usuários do departamento bancário"")

                # 3. FORMATO DE RESPOSTA
                - Use tabelas formatadas em Markdown sempre que houver 3 ou mais itens ou quando for retornado um agrupamento.
                - Use **negrito** para destacar valores totais.
                - Mostre o montante no formato de moeda local (R$).";

            AIAgent agent = chatClient.AsAIAgent(
                name: "FinancialExpertAgent",
                instructions: systemInstructions,
                tools: tools
            );

            return agent;
        }

        /*public Task<AIAgent> CreateAgentAsync(string iaToken, bool hasPayableChatAccess, bool hasReceivableChatAccess, bool hasBankingChatAccess)
        {
            if (string.IsNullOrWhiteSpace(iaToken))
                throw new ArgumentException("IA Token was not provided by the current Tenant.", nameof(iaToken));

            Console.WriteLine($"[AgentFactory] Creating ChatClient with token: {(string.IsNullOrEmpty(iaToken) ? "NULL/EMPTY" : iaToken.Substring(0, Math.Min(iaToken.Length, 12)) + "...")}");

            IChatClient chatClient =
                new OpenAI.Chat.ChatClient("gpt-4o-mini", iaToken)
                .AsIChatClient(); 
            
            Console.WriteLine("[AgentFactory] IChatClient created successfully.");
            // 2. Extrai as Tools do Plugin
            var tools = ToolRegistry.FromPlugin(_erpPlugin);

            // 3. Define as instruções (Otimizado: Com injeção de data dinâmica)
            var today = DateTime.Now.ToString("yyyy-MM-dd");
            var systemInstructions = @$"# ROLE & OBJECTIVE
                                        DATA ATUAL DO SISTEMA (HOJE): {today}

                                        Você atua como um Analista Financeiro Sênior (IA) integrado a um ERP corporativo. Seu objetivo é orquestrar a execução exata das ferramentas disponíveis para consultar o banco de dados financeiro.

                                        # CORE ARCHITECTURE (VIEWS)
                                        Sempre mapeie a intenção do usuário para a fonte de dados correta ANTES de acionar uma ferramenta:

                                        FORNECEDOR É PAG
										CLIENTE É REC

										DOCUMENTOS/TITULOS/NOTAS A PAGAR/PAGO É PAG
										DOCUMENTOS/TITULOS/NOTAS A RECEBER/RECEBIDO É REC

										# DESAMBIGUAÇÃO DE DOMÍNIO (CRÍTICO - REGRA DETERMINÍSTICA)

                                        - Faça uma verificação direta por palavras-chave na pergunta do usuário:

                                        SE a pergunta contém ""fornecedor"" ou ""fornecedores"":
                                          → DEFINA domínio como PAGAR (PAG)
                                          → PROIBIDO pedir desambiguação

                                        SE a pergunta contém ""cliente"" ou ""clientes"":
                                          → DEFINA domínio como RECEBER (REC)
                                          → PROIBIDO pedir desambiguação

                                        SE contém ambos:
                                          → Pergunte: ""Favor especificar se é fornecedor ou cliente""

                                        SE não contém nenhum:
                                          → Pergunte: ""Favor especificar se é fornecedor ou cliente""

                                        # REGRA DE EXECUÇÃO (CRÍTICO)

                                        - Após identificar o domínio (PAG ou REC), você DEVE seguir diretamente para a execução da consulta.

                                        - É PROIBIDO pedir esclarecimento se o domínio já foi identificado.

                                        - Perguntar desambiguação nesses casos é considerado erro.

                                        # REGRA CRÍTICA (ANTI-ERRO)

                                        - A presença da palavra ""fornecedor"" ou ""fornecedores"" SEMPRE define o domínio como PAGAR.
                                        - A presença da palavra ""cliente"" ou ""clientes"" SEMPRE define o domínio como RECEBER.
                                        - NÃO interprete, NÃO deduza, NÃO ignore essas palavras.
                                        - NÃO peça desambiguação nesses casos.

                                        # EXEMPLOS OBRIGATÓRIOS (USE COMO REFERÊNCIA)

                                        Entrada:
                                        ""quantos documentos de fornecedores em aberto""
                                        → PAGAR (NÃO perguntar)

                                        Entrada:
                                        ""quantos documentos de clientes em aberto""
                                        → RECEBER (NÃO perguntar)

                                        - REGRA DE PRIORIDADE: 
                                        Se existir QUALQUER indicação textual (mesmo indireta) de fornecedor ou cliente, 
                                        você NÃO deve pedir esclarecimento e deve seguir com a execução.

                                        1. SAÍDAS PENDENTES DOMÍNIO PAGAR ABERTO: View `VW_DOC_FIN_PAG_ABERTO`. (Acesso: {(hasPayableChatAccess ? "PERMITIDO" : "NEGADO")})
                                        2. SAÍDAS LIQUIDADAS DOMÍNIO PAGAR PAGO: View `VW_DOC_FIN_PAG_PAGO`. (Acesso: {(hasPayableChatAccess ? "PERMITIDO" : "NEGADO")})
                                        3. ENTRADAS PENDENTES DOMÍNIO RECEBER ABERTO: View `VW_DOC_FIN_REC_ABERTO`. (Acesso: {(hasReceivableChatAccess ? "PERMITIDO" : "NEGADO")})
                                        4. ENTRADAS LIQUIDADAS DOMÍNIO RECEBER PAGO: View `VW_DOC_FIN_REC_PAGO`. (Acesso: {(hasReceivableChatAccess ? "PERMITIDO" : "NEGADO")})
                                        5. MOVIMENTAÇÃO BANCÁRIA / SALDOS: (Acesso: {(hasBankingChatAccess ? "PERMITIDO" : "NEGADO")})

                                        # REGRAS DE ACESSO CRÍTICAS (DEVE SEGUIR À RISCA)
                                        - Se o usuário solicitar informações sobre Contas a PAGAR (Views 1 e 2) e o acesso estiver NEGADO, você NÃO deve executar nenhuma ferramenta. Em vez disso, retorne EXATAMENTE esta frase: ""Esse questionamento é somente para usuários do conta a pagar"".
                                        - Se o usuário solicitar informações sobre Contas a RECEBER (Views 3 e 4) e o acesso estiver NEGADO, você NÃO deve executar nenhuma ferramenta. Em vez disso, retorne EXATAMENTE esta frase: ""Esse questionamento é somente para usuários do conta a receber"".
                                        - Se o usuário solicitar informações sobre BANCÁRIO / SALDOS e o acesso estiver NEGADO, você NÃO deve executar nenhuma ferramenta. Em vez disso, retorne EXATAMENTE esta frase: ""Esse questionamento é somente para usuários do departamento bancário"".
                                        - Se o usuário for ADMIN (Acesso PERMITIDO para ambos), ignore estas restrições.

                                        # FIDELIDADE DE DADOS (CRÍTICO - PREVENÇÃO DE ALUCINAÇÃO)
                                        - Você NÃO DEVE realizar cálculos matemáticos, somas ou deduções lógicas por conta própria na sua geração de texto ao relatar métricas financeiras ou contagens de títulos. 
                                        - Você deve repassar EXATAMENTE os valores e quantidades brutas que as ferramentas de banco de dados retornarem. 
                                        - NÃO altere, NÃO arredonde e NÃO some resultados de ferramentas com informações de mensagens anteriores.

                                        # DIRETRIZES DE DECISÃO (IMPORTANTE)
                                        - BUSCA HISTÓRICA:
                                              Para perguntas sem período definido, utilize obrigatoriamente o intervalo COMPLETO:
                                              - Data inicial: 1900-01-01 (ou menor disponível)
                                              - Data final: 2100-12-31 (ou maior disponível)

                                              Nunca limite até a data atual.
                                        - DATA DE REFERÊNCIA: Use a data {today} para converter termos como ""hoje"", ""amanhã"", ""este mês"" ou ""ontem"" para o formato ISO 8601.
                                        - DESAMBIGUAÇÃO AVANÇADA:
                                              Só aplicar quando NÃO houver presença explícita das palavras:
                                              ""fornecedor"", ""fornecedores"", ""cliente"", ""clientes""
                                        - PRECISÃO DE BUSCA: Ao procurar por nomes, ignore termos genéricos (LTDA, SA). As ferramentas já realizam busca parcial (LIKE).
                                        - AGREGAÇÃO: Se a pergunta for sobre ""Total"", ""Quantidade"" ou ""Soma"", use as ferramentas agregadoras (GetSoma... ou GetContagem...). Jamais some manualmente.

                                        # CONTEXTO DE CONVERSA (CRÍTICO)

                                        - Após identificar o domínio inicial da conversa (PAGAR ou RECEBER), mantenha esse contexto nas próximas interações.

                                        - Se a conversa iniciou com termos relacionados a FORNECEDOR (PAGAR), assuma que as próximas perguntas continuam nesse domínio, mesmo que não seja mencionado novamente.

                                        - Se a conversa iniciou com termos relacionados a CLIENTE (RECEBER), assuma o mesmo comportamento.

                                        - SOMENTE altere o domínio atual se o usuário mencionar explicitamente o outro contexto:
                                            Exemplo:
                                            - ""e clientes?""
                                            - ""agora sobre clientes""
                                            - ""no contas a receber""

                                        - Se a nova pergunta for ambígua, priorize o contexto atual e NÃO peça desambiguação.

                                        - REGRA DE PRIORIDADE:
                                            Contexto atual > interpretação isolada da frase

                                        - EXCEÇÃO:
                                            Se a nova pergunta contiver indicação clara e explícita de outro domínio, ignore o contexto anterior e atualize o contexto.
                                        
                                        # FORMATAÇÃO DE RESPOSTA (CRÍTICO)

                                        - Você NÃO deve agrupar dados por ano, mês ou qualquer período, a menos que o usuário peça explicitamente.

                                        - Se o usuário NÃO solicitar agrupamento:
                                          → Retorne apenas o total geral solicitado

                                        - Perguntas como:
                                          ""quantos documentos""
                                          → devem retornar apenas a quantidade total, sem detalhamento por ano

                                        # PROIBIÇÃO DE INFERÊNCIA TEMPORAL

                                        - Você NÃO deve associar documentos a anos específicos, a menos que:
                                          - o usuário peça explicitamente
                                          - ou a ferramenta retorne já agrupado por ano

                                        - Termos como:
                                          ""vencidos"" e ""a vencer""
                                          → NÃO indicam nenhum ano específico

                                        # REGRA DE PERÍODO (CRÍTICO)

                                        - Se o usuário NÃO informar explicitamente um intervalo de datas, você DEVE considerar TODO o período disponível na base de dados.

                                        - ""TODO o período"" significa:
                                          - Desde o início dos registros (ex: 1900-01-01 ou menor data disponível)
                                          - Até o limite futuro (ex: 2100-12-31 ou maior data disponível)

                                        - Nunca limite automaticamente por anos recentes (ex: últimos 3 ou 5 anos).

                                        - Nunca ignore registros futuros (""a vencer"").

                                        - Termos como:
                                          ""quantos"", ""quantidade"", ""total"", ""todos"", ""geral""
                                          → indicam obrigatoriamente consulta completa (sem corte de período)

                                        - Se o usuário quiser um período específico, ele deve informar explicitamente:
                                          Exemplo:
                                          - ""em 2024""
                                          - ""nos últimos 2 anos""
                                          - ""de janeiro até março""

                                        - REGRA DE OURO:
                                          Na ausência de período explícito → SEMPRE consultar passado + presente + futuro.

                                        # TOOLS
                                        As ferramentas disponíveis são enviadas via esquema JSON. Leia atentamente a [Description] de cada uma para escolher a mais específica para a view identificada acima.";

            AIAgent agent = chatClient.AsAIAgent(
                name: "FinancialExpertAgent",
                instructions: systemInstructions,
                tools: tools
            );

            return Task.FromResult(agent);
        }*/
    }
}
