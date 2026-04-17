
using IT4You.Application.Interfaces;
using IT4You.Application.Plugins;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI;
using Microsoft.Extensions.AI;
using OpenAI;
using Pgvector.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using System.ClientModel;

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

        public async Task<AIAgent> CreateAgentAsync(string chatAiToken, string ragAiToken, bool hasPayableChatAccess, bool hasReceivableChatAccess, bool hasBankingChatAccess, string userInput = null, string userId = null)
        {
            if (string.IsNullOrWhiteSpace(chatAiToken))
                throw new ArgumentException("Chat AI Token (Groq) was not provided.", nameof(chatAiToken));

            //IChatClient chatClient = new OpenAI.Chat.ChatClient("gpt-4o-mini", iaToken).AsIChatClient();

            // 1. Redirecionamos a URL base da OpenAI para a URL de compatibilidade do Groq
            var groqOptions = new OpenAIClientOptions
            {
                Endpoint = new Uri("https://api.groq.com/openai/v1")
            };

            // 2. Instanciamos usando o nome do modelo no Groq e as novas opções
            IChatClient chatClient = new OpenAI.Chat.ChatClient(
                "openai/gpt-oss-120b", // O ID do modelo conforme a documentação do Groq
                new ApiKeyCredential(chatAiToken), // Usando o token do inquilino para o Groq
                groqOptions
            ).AsIChatClient();

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
            bool hasFullAccess = hasPayableChatAccess && hasReceivableChatAccess;

            foreach (var t in allTools)
            {
                // 1. HARD SECURITY: Se o usuário NÃO tem acesso, a ferramenta NEM É REGISTRADA (Impedindo bypass por prompt)
                if (!hasPayableChatAccess && t.Name.Contains("Pagar", StringComparison.OrdinalIgnoreCase)) continue;
                if (!hasReceivableChatAccess && t.Name.Contains("Receber", StringComparison.OrdinalIgnoreCase)) continue;
                
                // Ferramentas de Saldos/Bancário/Liquidez
                bool isBankingTool = t.Name.Contains("Saldo", StringComparison.OrdinalIgnoreCase) || 
                                     t.Name.Contains("Liquidez", StringComparison.OrdinalIgnoreCase) ||
                                     t.Name.Contains("FluxoCaixa", StringComparison.OrdinalIgnoreCase);

                if (!hasBankingChatAccess && (t.Name.Contains("Saldo", StringComparison.OrdinalIgnoreCase))) continue;
                
                // Ferramentas que cruzam dados (AMBOS) exigem acesso total para evitar vazamento
                if (!hasFullAccess && (t.Name.Contains("Liquidez", StringComparison.OrdinalIgnoreCase) || t.Name.Contains("FluxoCaixa", StringComparison.OrdinalIgnoreCase))) continue;

                // 2. CONTEXT OPTIMIZATION: Se já sabemos o domínio, ocultamos o oposto (economia de tokens)
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


            string ragKnowledge = "";
            if (!string.IsNullOrWhiteSpace(userInput))
            {
                try
                {
                if (!string.IsNullOrWhiteSpace(ragAiToken))
                {
                    // 1. Gera Embedding da mensagem (OpenAI)
                    var embeddingClient = new OpenAI.Embeddings.EmbeddingClient("text-embedding-3-small", ragAiToken);
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

                    if (fetchedMemories.Any())
                    {
                        // ... seu código atual ...
                        Console.WriteLine("=== MEMÓRIAS RECUPERADAS ===");
                        Console.WriteLine(ragKnowledge);
                    }
                    else
                    {
                        Console.WriteLine("=== NENHUMA MEMÓRIA PASSOU NO CORTE DE 0.65 ===");
                    }
                }
                else
                {
                    Console.WriteLine("=== RAG PULADO: Token Memória RAG não configurado ===");
                }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[RAG FAIL] {ex.Message}");
                }
            }

            var regrasDeBloqueio = hasPayableChatAccess && hasReceivableChatAccess && hasBankingChatAccess
                ? "O usuário possui TODOS OS ACESSOS. IGNORE qualquer regra de bloqueio. Execute todas as consultas normalmente."
                : """
              - BLOQUEIO PRIORITÁRIO: Se o domínio solicitado (Pagar, Receber ou Bancário) está marcado como NEGADO abaixo, você deve interromper o raciocínio e bloquear IMEDIATAMENTE.
              - NUNCA execute ferramentas de domínios negados.
              - Se o usuário perguntar sobre um domínio negado, responda APENAS a frase de bloqueio correspondente.
              - Se o domínio for ambíguo, peça esclarecimento ANTES de qualquer ação.

              Frases de bloqueio mandatário:
              - PAGAR NEGADO → "Esse questionamento é somente para usuários do conta a pagar"
              - RECEBER NEGADO → "Esse questionamento é somente para usuários do conta a receber"
              - BANCÁRIO NEGADO → "Esse questionamento é somente para usuários do departamento bancário"
              """;

            // 2. Use $""" (3 aspas) para permitir aspas duplas no texto sem quebrar o código
            var systemInstructions = $"""
                # PERFIL
                Você é um Analista Financeiro Sênior (IA) integrado ao ERP. Sua missão é fornecer dados precisos e confiavéis.
                DATA ATUAL: {today}
                {avisoDominio}

                {ragKnowledge}

                # 1. DIRETRIZES DE DADOS E EXECUÇÃO
                - PARÂMETROS VAZIOS: Se o usuário não citar datas ou filtros (como nome, UF, filial, cnpj), preencha os parâmetros da ferramenta OBRIGATORIAMENTE com STRINGS VAZIAS (""). NUNCA envie valores null.
                - BUSCA POR DOCUMENTO: Se o usuário fornecer um número de documento (ex: "0000001541"), utilize o parâmetro "numeroDocumento" da ferramenta. Isso garantirá que você localize apenas o registro desejado, evitando que o sistema gere um relatório PDF/Excel desnecessário por excesso de registros.
                - PERÍODO: Apenas defina Data Fim se o usuário explicitamente fechar o escopo temporal.
                - FIDELIDADE: Relate exatamente os valores brutos. Não arredonde e não faça cálculos manuais além do básico. Se a ferramenta retornar nada, diga R$ 0,00.
                - AGREGAÇÃO E CONTAGEM (REGRA DE OURO): Se o usuário perguntar "Quantos", "Qual a quantidade", "Saldo total", "Soma de valores", "Quanto tenho", "Qual o total" ou qualquer variação de volume/soma/contagem, você DEVE OBRIGATORIAMENTE usar o parâmetro agrupamento="TOTAL". É estritamente proibido listar documentos individuais para contar ou somar manualmente.
                - TOP N / MAIORES VALORES: Se o usuário pedir pelos "10 maiores", "top 5", "quais os 8 principais", etc., você DEVE usar o parâmetro `limite` (ex: 10) e `ordenarPorMaiorValor=true`. Isso garantirá que o sistema mostre os dados diretamente no chat em forma de tabela, permitindo a visualização imediata dos itens mais relevantes, mesmo que o total de registros no banco seja muito grande.
                - ATENÇÃO CRÍTICA AO JSON DE LISTAGEM: Quando você fizer uma listagem (agrupamento="NENHUM"), o retorno conterá o campo "TotalDeDocumentosNoBanco" que é uma QUANTIDADE DE DOCUMENTOS (número inteiro de registros), NÃO é um valor em Reais. NUNCA apresente esse número formatado como R$. Se precisar do valor financeiro total, chame a ferramenta novamente com agrupamento="TOTAL".
                - LISTAGEM E LIMITES: Quando você listar documentos (agrupamento="NENHUM"), o sistema retornará no máximo 50 registros. Se o campo "AlertaParaIA" indicar que a listagem está parcial, informe o usuário que existem mais documentos e que para valores exatos é necessário usar agrupamento="TOTAL".
                - RODAPÉ OBRIGATÓRIO: Toda listagem de documentos virá com o campo "ValorTotalConfirmado" no JSON. Este é o SUM() calculado pelo banco para TODOS os documentos do filtro. Você DEVE exibir este valor como rodapé da tabela no formato "**Total: R$ X.XXX,XX (N documentos)**". NUNCA some os valores individuais das linhas — use SEMPRE "ValorTotalConfirmado".

                # 2. SEGURANÇA E ACESSOS
                Permissões do usuário atual:
                - Contas a PAGAR: {(hasPayableChatAccess ? "PERMITIDO" : "NEGADO")}
                - Contas a RECEBER: {(hasReceivableChatAccess ? "PERMITIDO" : "NEGADO")}
                - Bancário/Saldos: {(hasBankingChatAccess ? "PERMITIDO" : "NEGADO")}

                REGRA DE BLOQUEIO — aplique com precisão cirúrgica:
                {regrasDeBloqueio}

                # 3. GERAÇÃO DE DOCUMENTOS (PDF/EXCEL)
                - Você POSSUI a capacidade de gerar relatórios em PDF e Excel.
                - REGRA DE 10 LINHAS (VISUALIZAÇÃO):
                    - Se o resultado solicitado (através do parâmetro `limite`) for 10 ou menos, ou se o total encontrado no banco for 10 ou menos, o sistema enviará os dados para você exibir em uma tabela no chat.
                    - Se o total de registros ultrapassar 10 e você NÃO tiver definido um `limite` pequeno, o sistema gerará AUTOMATICAMENTE os arquivos PDF e Excel para o usuário e você receberá um aviso de 'EXPORT_PRONTO'.
                - O que dizer ao usuário:
                    - Se o usuário pedir explicitamente um PDF / Excel, confirme que o sistema fornecerá o arquivo se a lista for extensa.
                    - Jamais diga "Não consigo gerar PDF diretamente".

                # 4. FORMATO DE RESPOSTA
                - Use tabelas formatadas em Markdown sempre que houver 3 ou mais itens ou quando for retornado um agrupamento.
                - Use **negrito** para destacar valores totais.
                - Mostre o montante no formato de moeda local (R$).

                # 5. IDIOMA E ESTILO
                - IDIOMA: Responda OBRIGATORIAMENTE em Português (Brasil).
                - PROIBIÇÃO DE INGLÊS: É estritamente proibido iniciar frases com termos em inglês (ex: "For", "Regarding", "The"). Use sempre construções naturais do Português (ex: "Para a empresa...", "Foram encontrados...", "Referente a...").
                """;

                            var chatOptions = new ChatOptions
                            {
                                Temperature = 0,
                                Tools = tools,
                                Instructions = systemInstructions
                            };

            // 2. Configure as opções do Agente
            var agentOptions = new ChatClientAgentOptions
            {
                Name = "FinancialExpertAgent",
                ChatOptions = chatOptions
            };

            // 3. Crie o agente usando a sobrecarga que aceita as opções
            AIAgent agent = chatClient.AsAIAgent(agentOptions);

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

                                        1. SAÍDAS PENDENTES DOMÍNIO PAGAR ABERTO: View `VW_SWIA_DOC_FIN_PAG_ABERTO`. (Acesso: {(hasPayableChatAccess ? "PERMITIDO" : "NEGADO")})
                                        2. SAÍDAS LIQUIDADAS DOMÍNIO PAGAR PAGO: View `VW_SWIA_DOC_FIN_PAG_PAGO`. (Acesso: {(hasPayableChatAccess ? "PERMITIDO" : "NEGADO")})
                                        3. ENTRADAS PENDENTES DOMÍNIO RECEBER ABERTO: View `VW_SWIA_DOC_FIN_REC_ABERTO`. (Acesso: {(hasReceivableChatAccess ? "PERMITIDO" : "NEGADO")})
                                        4. ENTRADAS LIQUIDADAS DOMÍNIO RECEBER PAGO: View `VW_SWIA_DOC_FIN_REC_PAGO`. (Acesso: {(hasReceivableChatAccess ? "PERMITIDO" : "NEGADO")})
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
