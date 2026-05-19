using System.Text.RegularExpressions;

namespace IT4You.Application.AI.Routing;

public class ChatModuleRouter
{
    public ChatModuleRouteResult Route(string userMessage, ChatModule? previousModule = null)
    {
        var text = Normalize(userMessage);

        if (string.IsNullOrWhiteSpace(text))
        {
            return new ChatModuleRouteResult(ChatModule.Indefinido, true,
                "Sua pergunta ficou vazia. Voce quer consultar Financeiro (pagar/receber/bancos) ou Orcamento (orcamentos e itens)?");
        }

        var isBudget = ContainsAny(text,
            "orcamento", "orcamentos", "orcament", "proposta", "propostas", "cotacao", "cotacoes", "orcnum",
            "item orcamento", "itens do orcamento", "itens de orcamento", "item do orcamento");

        var isFinance = ContainsAny(text,
            "pagar", "contas a pagar", "fornecedor", "fornecedores", "despesa", "saida", "saidas",
            "receber", "contas a receber", "recebimento", "recebimentos",
            "documento", "documentos", "titulo", "titulos", "vencido", "vencimento",
            "banco", "bancario", "saldo", "fluxo de caixa", "liquidez");

        if (isBudget && isFinance)
        {
            return new ChatModuleRouteResult(ChatModule.Indefinido, true,
                "Sua pergunta mistura termos de Financeiro e Orcamento. Voce quer consultar Financeiro (pagar/receber/bancos) ou Orcamento (orcamentos e itens)?");
        }

        if (isBudget)
            return new ChatModuleRouteResult(ChatModule.Orcamento, false);

        if (isFinance)
            return new ChatModuleRouteResult(ChatModule.Financeiro, false);

        // Ambiguo: tenta manter o contexto anterior quando a mensagem e curta e nao contem palavras fortes do outro modulo.
        if (previousModule.HasValue && text.Length <= 80)
            return new ChatModuleRouteResult(previousModule.Value, false);

        return new ChatModuleRouteResult(ChatModule.Indefinido, true,
            "Nao consegui identificar se sua pergunta e de Financeiro ou Orcamento. Voce quer consultar Financeiro (pagar/receber/bancos) ou Orcamento (orcamentos e itens)?");
    }

    private static bool ContainsAny(string text, params string[] terms)
        => terms.Any(t => text.Contains(t, StringComparison.OrdinalIgnoreCase));

    private static string Normalize(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var normalized = value.ToLowerInvariant();
        normalized = normalized
            .Replace("á", "a").Replace("à", "a").Replace("ã", "a").Replace("â", "a")
            .Replace("é", "e").Replace("ê", "e")
            .Replace("í", "i")
            .Replace("ó", "o").Replace("ô", "o").Replace("õ", "o")
            .Replace("ú", "u")
            .Replace("ç", "c");

        return Regex.Replace(normalized, "\\s+", " ").Trim();
    }
}

