
using IT4You.Application.Interfaces;
using IT4You.Application.Plugins;
using Microsoft.Agents.AI;
using Microsoft.Extensions.AI;

namespace IT4You.Application.Services
{
    public class FinancialAgentFactory : IFinancialAgentFactory
    {
        private readonly ErpPlugin _erpPlugin;

        // Injetamos o plugin aqui para ele já vir com a Configuration/DB injetada
        public FinancialAgentFactory(ErpPlugin erpPlugin)
        {
            _erpPlugin = erpPlugin;
        }

        public Task<AIAgent> CreateAgentAsync(string iaToken, bool hasPayableChatAccess, bool hasReceivableChatAccess, bool hasBankingChatAccess)
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

										# DESAMBIGUAÇÃO DE DOMÍNIO (CRÍTICO)

                                        Antes de pedir esclarecimento ao usuário, você DEVE tentar identificar o domínio com base nas palavras da pergunta:

                                        - Se a frase contiver termos como:
                                          ""fornecedor"", ""fornecedores"", ""pagamento"", ""pagar"", ""compras""
                                          → ASSUMA automaticamente CONTAS A PAGAR (PAG)

                                        - Se a frase contiver termos como:
                                          ""cliente"", ""clientes"", ""recebimento"", ""receber"", ""vendas""
                                          → ASSUMA automaticamente CONTAS A RECEBER (REC)

                                        - Se a frase contiver ambos ou for realmente ambígua
                                          → Pergunte: ""Favor especificar se é fornecedor ou cliente""

                                        - SOMENTE faça essa pergunta se NÃO houver NENHUM indicativo claro no texto.

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
                                        - BUSCA HISTÓRICA: Para perguntas sem período definido (ex: ""tem algum"", ""já pagamos""), utilize obrigatoriamente um intervalo amplo (ex: 2020-01-01 até HOJE) nos parâmetros de data das ferramentas.
                                        - DATA DE REFERÊNCIA: Use a data {today} para converter termos como ""hoje"", ""amanhã"", ""este mês"" ou ""ontem"" para o formato ISO 8601.
                                        - DESAMBIGUAÇÃO: Se a entidade for ambígua (ex: Seguradoras), verifique tanto os fluxos de Pagar quanto de Receber (respeitando as permissões acima).
                                        - PRECISÃO DE BUSCA: Ao procurar por nomes, ignore termos genéricos (LTDA, SA). As ferramentas já realizam busca parcial (LIKE).
                                        - CONTEXTO: Se a conversa iniciar falando de PAGAR ABERTO, o contexto não pode ser alterado nas outras perguntas, isso deve ser regra para os outros DOMÍNIOS como PAGAR PAGO, RECEBER ABERTO E RECEBER PAGO.
                                        - AGREGAÇÃO: Se a pergunta for sobre ""Total"", ""Quantidade"" ou ""Soma"", use as ferramentas agregadoras (GetSoma... ou GetContagem...). Jamais some manualmente.

                                        # TOOLS
                                        As ferramentas disponíveis são enviadas via esquema JSON. Leia atentamente a [Description] de cada uma para escolher a mais específica para a view identificada acima.";

            AIAgent agent = chatClient.AsAIAgent(
                name: "FinancialExpertAgent",
                instructions: systemInstructions,
                tools: tools
            );

            return Task.FromResult(agent);
        }
    }
}
