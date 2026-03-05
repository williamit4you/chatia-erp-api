
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

        public Task<AIAgent> CreateAgentAsync(string iaToken)
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
                                        1. SAÍDAS PENDENTES: View `VW_DOC_FIN_PAG_ABERTO`.
                                        2. SAÍDAS LIQUIDADAS: View `VW_DOC_FIN_PAG_PAGO`.
                                        3. ENTRADAS PENDENTES: View `VW_DOC_FIN_REC_ABERTO`.
                                        4. ENTRADAS LIQUIDADAS: View `VW_DOC_FIN_REC_PAGO`.

                                        # DIRETRIZES DE DECISÃO (IMPORTANTE)
                                        - BUSCA HISTÓRICA: Para perguntas sem período definido (ex: ""tem algum"", ""já pagamos""), utilize obrigatoriamente um intervalo amplo (ex: 2020-01-01 até HOJE) nos parâmetros de data das ferramentas.
                                        - DATA DE REFERÊNCIA: Use a data {today} para converter termos como ""hoje"", ""amanhã"", ""este mês"" ou ""ontem"" para o formato ISO 8601.
                                        - DESAMBIGUAÇÃO: Se a entidade for ambígua (ex: Seguradoras), verifique tanto os fluxos de Pagar quanto de Receber.
                                        - PRECISÃO DE BUSCA: Ao procurar por nomes, ignore termos genéricos (LTDA, SA). As ferramentas já realizam busca parcial (LIKE).
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
