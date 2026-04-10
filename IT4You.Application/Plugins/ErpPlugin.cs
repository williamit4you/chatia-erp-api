using System.ComponentModel;
using Microsoft.SemanticKernel;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;
using System.Text.Json;
using System.Data;

namespace IT4You.Application.Plugins;

public class ErpPlugin
{
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;

    // SQL query tracking (safe: ErpPlugin is Scoped per-request)
    public List<string> ExecutedQueries { get; } = new();

    public void ClearExecutedQueries() => ExecutedQueries.Clear();

    public string? GetExecutedQueriesJson()
    {
        if (ExecutedQueries.Count == 0) return null;
        return JsonSerializer.Serialize(ExecutedQueries);
    }

    public ErpPlugin(IConfiguration configuration)
    {
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection") ?? "";
    }
    private const string BASE_COLUMNS = "EMPRESA, CLIENTE, NOMEFANTASIA, CPFCNPJ, CIDADE, UF, DOCUMENTO, EMISSAO, VALORDOC, PARCELA, VALORORIG, VALORPAG, DATAVENCIMENTO, DATAPAGAMENTO, CONDPAG, TIPOPAG, SITUACAO";

    // --- VW_DOC_FIN_PAG_ABERTO ---

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca contas a PAGAR em aberto que vencem em um período. Ex: O que temos para pagar hoje? O que vence amanhã? Próxima semana?")]
    public async Task<string> GetVencendoNoPeriodo(
        [Description("Data inicial Vencimento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Vencimento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca contas a PAGAR em aberto de um Fornecedor específico que vencem em um determinado período. Ex: Quais contas da 'TechCorp' vencem na próxima semana?")]
    public async Task<string> GetPagarAbertoPorFornecedorEPeriodo(
        [Description("Nome do Fornecedor ou Nome Fantasia")] string fornecedor,
        [Description("Data inicial Vencimento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Vencimento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE (UPPER(CLIENTE) LIKE UPPER(@nf) OR UPPER(NOMEFANTASIA) LIKE UPPER(@nf)) AND DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@nf", $"%{fornecedor}%"),
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) 
        });
    }

   

    [Description("[DOMÍNIO: RECEBER EM ABERTO] Busca clientes de um Estado (UF) específico que estão com pagamentos ATRASADOS há mais de X dias. Ex: Clientes de MG com atraso maior que 30 dias.")]
    public async Task<string> GetReceberAtrasadosPorEstadoEDias(
        [Description("Sigla do Estado. Ex: MG, SP, RJ")] string uf,
        [Description("Quantidade mínima de dias em atraso. Ex: 30")] int diasAtraso)
    {
        var sq = $@"SELECT 
                      {BASE_COLUMNS}, 
                      DATEDIFF(day, DATAVENCIMENTO, CAST(GETDATE() AS DATE)) as DiasAtraso
                   FROM VW_DOC_FIN_REC_ABERTO 
                   WHERE UF = @uf 
                     AND DATAVENCIMENTO <= DATEADD(day, -@dias, CAST(GETDATE() AS DATE))
                   ORDER BY DiasAtraso DESC";
                     
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@uf", uf),
            new SqlParameter("@dias", diasAtraso) 
        });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Retorna o Valor Total pendente e a Quantidade de contas a pagar AGRUPADOS POR FORNECEDOR. Use EXCLUSIVAMENTE quando o usuário pedir 'quem são os maiores credores', 'resumo por fornecedor' ou 'ranking de fornecedores a pagar'.")]
    public async Task<string> GetResumoPagarAgrupadoPorFornecedor(
        [Description("Critério de ordenação. Valores permitidos: 'VALOR_DESC' (Maiores credores - Padrão), 'VALOR_ASC' (Menores credores), 'QTD_DESC' (Maior volume de boletos), 'FORNECEDOR_ASC' (Ordem alfabética A-Z).")] string ordenacao = "VALOR_DESC")
    {
        // Trava de Segurança para evitar SQL Injection no ORDER BY
        string orderByClause = (ordenacao?.ToUpper()) switch
        {
            "VALOR_ASC"      => "TotalPendente ASC",
            "QTD_DESC"       => "QuantidadeTitulos DESC",
            "FORNECEDOR_ASC" => "CLIENTE ASC", // No seu schema, o Fornecedor fica na coluna CLIENTE
            _                => "TotalPendente DESC" // Fallback seguro: maiores valores no topo
        };

        var sq = $@"SELECT CLIENTE as NomeFornecedor, 
                      SUM(VALORORIG) as TotalPendente, 
                      COUNT(*) as QuantidadeTitulos 
                   FROM VW_DOC_FIN_PAG_ABERTO 
                   GROUP BY CLIENTE 
                   ORDER BY {orderByClause}";
                   
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER EM ABERTO] Retorna a previsão de recebimento (títulos em aberto) AGRUPADA POR MÊS dentro de um ANO específico. Use EXCLUSIVAMENTE quando o usuário pedir visões mês a mês, previsão mensal ou distribuição do fluxo de caixa no ano.")]
    public async Task<string> GetResumoReceberAgrupadoPorMesNoAno(
        [Description("Ano com 4 digitos. Ex: 2026")] string ano)
    {
        var sq = $@"SELECT 
                      MONTH(DATAVENCIMENTO) as Mes, 
                      SUM(VALORORIG - ISNULL(VALORPAG, 0)) as TotalPrevisto, 
                      COUNT(*) as QuantidadeTitulos 
                   FROM VW_DOC_FIN_REC_ABERTO 
                   WHERE YEAR(DATAVENCIMENTO) = @ano
                   GROUP BY MONTH(DATAVENCIMENTO)
                   ORDER BY Mes ASC";
                   
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Retorna o Valor Total recebido e a Quantidade de títulos AGRUPADOS POR MEIO DE PAGAMENTO (PIX, Boleto, Cartão, etc) em um período específico. Use EXCLUSIVAMENTE quando o usuário pedir 'representatividade por forma de pagamento', 'resumo por método', ou perguntar 'quanto recebemos de cada tipo' em um mês/período.")]
    public async Task<string> GetResumoRecebidoAgrupadoPorMetodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = @"SELECT 
                      ISNULL(TIPOPAG, 'NÃO INFORMADO') as MetodoPagamento, 
                      SUM(VALORPAG) as TotalRecebido, 
                      COUNT(*) as QuantidadeTitulos 
                   FROM VW_DOC_FIN_REC_PAGO 
                   WHERE DATAPAGAMENTO >= @dF 
                     AND DATAPAGAMENTO <= @dT
                   GROUP BY TIPOPAG
                   ORDER BY TotalRecebido DESC";
                   
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) 
        });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Retorna o Valor Total faturado/recebido e a Quantidade de títulos AGRUPADOS POR FILIAL (EMPRESA) em um período específico. Use EXCLUSIVAMENTE quando o usuário pedir 'faturamento por filial', 'comparativo entre filiais', ou 'qual filial faturou/vendeu mais'.")]
    public async Task<string> GetResumoFaturadoAgrupadoPorFilial(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = @"SELECT 
                      ISNULL(EMPRESA, 'NÃO INFORMADA') as Filial, 
                      SUM(VALORPAG) as TotalFaturado, 
                      COUNT(*) as QuantidadeTitulos 
                   FROM VW_DOC_FIN_REC_PAGO 
                   WHERE DATAPAGAMENTO >= @dF 
                     AND DATAPAGAMENTO <= @dT
                   GROUP BY EMPRESA
                   ORDER BY TotalFaturado DESC";
                   
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) 
        });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Calcula o TICKET MÉDIO (valor médio) dos recebimentos em um período. Use EXCLUSIVAMENTE quando o usuário pedir 'ticket médio', 'média de valor pago pelos clientes' ou 'valor médio de recebimento'.")]
    public async Task<string> GetTicketMedioRecebimento(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = @"SELECT 
                      ISNULL(AVG(VALORPAG), 0) as TicketMedio, 
                      ISNULL(SUM(VALORPAG), 0) as TotalRecebido, 
                      COUNT(*) as QuantidadeTitulos 
                   FROM VW_DOC_FIN_REC_PAGO 
                   WHERE DATAPAGAMENTO >= @dF 
                     AND DATAPAGAMENTO <= @dT";
                     
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) 
        });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Calcula a MÉDIA de dias de atraso dos clientes que já pagaram (títulos liquidados após o vencimento) em um período. Use EXCLUSIVAMENTE quando o usuário perguntar sobre 'média de atraso', 'tempo médio que os clientes demoram para pagar atrasado' ou 'dias de atraso médio'.")]
    public async Task<string> GetMediaDiasAtrasoRecebimento(
        [Description("Data inicial Pagamento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Pagamento (ISO 8601)")] string dataFimISO)
    {
        var sq = @"SELECT 
                      ISNULL(AVG(DATEDIFF(day, DATAVENCIMENTO, DATAPAGAMENTO)), 0) as MediaDiasAtraso,
                      MAX(DATEDIFF(day, DATAVENCIMENTO, DATAPAGAMENTO)) as MaiorAtrasoNoPeriodo,
                      COUNT(*) as QuantidadeTitulosAtrasados
                   FROM VW_DOC_FIN_REC_PAGO 
                   WHERE DATAPAGAMENTO > DATAVENCIMENTO 
                     AND DATAPAGAMENTO >= @dF 
                     AND DATAPAGAMENTO <= @dT";
                     
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) 
        });
    }


    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca contas a PAGAR que já estão ATRASADAS (vencimento menor que hoje e sem data de pagamento).")]
    public async Task<string> GetAtrasados()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Calcula o Valor Total Pendente (Soma) de contas a pagar em um período de vencimento.")]
    public async Task<string> GetSomaTotalPendente(
        [Description("Data inicial Vencimento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Vencimento (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORORIG) as TotalPendente, COUNT(*) as Quantidade FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Calcula o Valor Total de contas ATRASADAS em reais.")]
    public async Task<string> GetTotalAtrasado()
    {
        var sq = "SELECT SUM(VALORORIG) as TotalAtrasado, COUNT(*) as Quantidade FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca a MAIOR conta pendente (valor mais alto) que vence em um período.")]
    public async Task<string> GetMaiorPendenteNoPeriodo(
        [Description("Data inicial Vencimento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Vencimento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT ORDER BY VALORORIG DESC";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca contas pendentes com valor ORIGINAL acima de um limite. Ex: Boletos acima de 50 mil.")]
    public async Task<string> GetPendentesAcimaDeValor([Description("Valor mínimo. Ex: 50000")] decimal valorMinimo)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE VALORORIG >= @val ORDER BY VALORORIG DESC";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@val", valorMinimo) });
    }

    public async Task<string> GetDividaPorFornecedor(
        [Description("[DOMÍNIO: PAGAR EM ABERTO] Nome do Fornecedor ou Fantasia")] string fornecedor)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE UPPER(CLIENTE) LIKE UPPER(@nf) OR UPPER(NOMEFANTASIA) LIKE UPPER(@nf)";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@nf", $"%{fornecedor}%") });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca contas que foram LANÇADAS/EMITIDAS hoje no sistema.")]
    public async Task<string> GetLancadosHoje()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE CAST(EMISSAO AS DATE) = CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca documentos que foram emitidos no ANO PASSADO mas que o vencimento é este ano.")]
    public async Task<string> GetEmitidosAnoPassadoVencendoEsteAno()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE YEAR(EMISSAO) = YEAR(GETDATE()) - 1 AND YEAR(DATAVENCIMENTO) = YEAR(GETDATE())";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca contas em aberto filtrando por Filial (EMPRESA).")]
    public async Task<string> GetPendentesPorFilial([Description("Nome da filial")] string empresa)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE EMPRESA LIKE @emp";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@emp", $"%{empresa}%") });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca fornecedores em aberto por Estado (UF).")]
    public async Task<string> GetPendentesPorEstado([Description("Sigla do Estado. Ex: MG")] string uf)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE UF = @uf";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@uf", uf) });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca detalhes de um documento específico em aberto.")]
    public async Task<string> GetDetalhesBoletoEmAberto(
        [Description("Número do documento")] string numeroDoc,
        [Description("Número da parcela (opcional)")] string parcela)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE DOCUMENTO = @doc";
        var list = new List<SqlParameter> { new SqlParameter("@doc", numeroDoc) };
        if (!string.IsNullOrEmpty(parcela)) { sq += " AND PARCELA = @par"; list.Add(new SqlParameter("@par", parcela)); }
        return await ExecuteQuery(sq, list.ToArray());
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca contas a pagar de um fornecedor que vencem num período.")]
    public async Task<string> GetPagarAbertoPorNomeFornecedor([Description("Nome do Fornecedor ou Fantasia")] string nomeFornecedor)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE UPPER(CLIENTE) LIKE UPPER(@nf) OR UPPER(NOMEFANTASIA) LIKE UPPER(@nf)";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@nf", $"%{nomeFornecedor}%") });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Busca contas a PAGAR em aberto pelo CNPJ exato do fornecedor.")]
    public async Task<string> GetPagarAbertoPorCNPJ([Description("CNPJ somente números")] string cpfCnpj)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE CPFCNPJ = @cnpj";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@cnpj", LimparCnpj(cpfCnpj)) });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Soma o valor esperado de pagamento filtrando por Tipo de Pagamento (PIX, Boleto, etc).")]
    public async Task<string> GetSomaPagarPorMetodo(
        [Description("Tipo de Pagamento. Ex: PIX, BOLETO")] string tipoPag,
        [Description("Data inicial Vencimento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Vencimento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT SUM(VALORORIG) as TotalEsperado, COUNT(*) as Quantidade FROM VW_DOC_FIN_PAG_ABERTO WHERE TIPOPAG LIKE @tp AND DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@tp", $"%{tipoPag}%"),
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Conta a Quantidade Total de boletos/contas a PAGAR que vencem em um Mês e Ano específicos.")]
    public async Task<string> GetContagemPagarAbertoPorMesVencimento(
        [Description("Ano com 4 digitos. Ex: 2026")] string ano, 
        [Description("Mês com 2 digitos. Ex: 02")] string mes)
    {
        var sq = "SELECT COUNT(*) as QuantidadeContas FROM VW_DOC_FIN_PAG_ABERTO WHERE YEAR(DATAVENCIMENTO) = @ano AND MONTH(DATAVENCIMENTO) = @mes";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano), new SqlParameter("@mes", mes) });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Conta a Quantidade e o Valor Total de contas a PAGAR que vencem em um Ano específico.")]
    public async Task<string> GetContagemPagarAbertoPorAnoVencimento(
    [Description("Ano com 4 digitos. Ex: 2026")] string ano)
    {
        // Adicionado o SUM aqui
        var sq = "SELECT COUNT(*) as QuantidadeContas, SUM(VALORORIG) as ValorTotal FROM VW_DOC_FIN_PAG_ABERTO WHERE YEAR(DATAVENCIMENTO) = @ano ";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano) });
    }

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Retorna o total a pagar e a quantidade de títulos, AGRUPADO POR ANO de vencimento. Use quando o usuário pedir 'resumo por ano', 'quanto temos por ano' ou 'total de fornecedores por ano'.")]
    public async Task<string> GetResumoPagarAbertoAgrupadoPorAno()
    {
        var sq = @"SELECT 
                  YEAR(DATAVENCIMENTO) as Ano, 
                  SUM(VALORORIG) as TotalEmAberto, 
                  COUNT(*) as QuantidadeTitulos 
               FROM VW_DOC_FIN_PAG_ABERTO 
               GROUP BY YEAR(DATAVENCIMENTO) 
               ORDER BY Ano";

        return await ExecuteQuery(sq, System.Array.Empty<SqlParameter>());
    }

    // --- VW_DOC_FIN_PAG_PAGO ---

    [Description("[DOMÍNIO: PAGAR PAGO] Busca contas que JÁ FORAM PAGAS em um período. Ex: pagamentos feitos hoje ou mês passado.")]
    public async Task<string> GetPagosNoPeriodo(
        [Description("Data inicial pagamento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final pagamento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: PAGAR PAGO] Calcula o Valor Total Gasto (Soma) em pagamentos em um período.")]
    public async Task<string> GetSomaTotalPago(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO,
        [Description("Opcional: Filtrar por filial")] string empresa)
    {
        var sq = "SELECT SUM(VALORPAG) as TotalPago FROM VW_DOC_FIN_PAG_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        var list = new List<SqlParameter> { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) 
        };
        if (!string.IsNullOrEmpty(empresa)) { sq += " AND EMPRESA LIKE @emp"; list.Add(new SqlParameter("@emp", $"%{empresa}%")); }
        return await ExecuteQuery(sq, list.ToArray());
    }

    [Description("[DOMÍNIO: PAGAR PAGO] Analisa a saúde financeira dos pagamentos: Soma de Juros (pago > original) e Descontos (pago < original) no período.")]
    public async Task<string> GetAnaliseJurosEDescontos(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = @"SELECT 
                    SUM(CASE WHEN VALORPAG > VALORORIG THEN VALORPAG - VALORORIG ELSE 0 END) as TotalJurosMulta,
                    SUM(CASE WHEN VALORPAG < VALORORIG THEN VALORORIG - VALORPAG ELSE 0 END) as TotalDescontos,
                    COUNT(CASE WHEN VALORPAG > VALORORIG THEN 1 END) as QtdComJuros,
                    COUNT(CASE WHEN VALORPAG < VALORORIG THEN 1 END) as QtdComDesconto
                   FROM VW_DOC_FIN_PAG_PAGO 
                   WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: PAGAR PAGO] Busca a conta com o MAIOR valor pago em um período.")]
    public async Task<string> GetPagamentoMaiorValor(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT ORDER BY VALORPAG DESC";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: PAGAR PAGO] Busca pagamentos por Fornecedor (Nome ou Fantasia) em um período.")]
    public async Task<string> GetPagamentosPorFornecedor(
        [Description("Nome do Fornecedor")] string fornecedor,
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_PAGO WHERE (UPPER(CLIENTE) LIKE UPPER(@nf) OR UPPER(NOMEFANTASIA) LIKE UPPER(@nf)) AND DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@nf", $"%{fornecedor}%"),
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: PAGAR PAGO] Soma o quanto foi pago filtrando pelo Meio de Pagamento (PIX, Boleto, Dinheiro, etc).")]
    public async Task<string> GetSomaPorMetodoPagamento(
        [Description("Tipo de Pagamento. Ex: PIX, BOLETO")] string tipoPag,
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORPAG) as TotalMetodo, COUNT(*) as Quantidade FROM VW_DOC_FIN_PAG_PAGO WHERE TIPOPAG LIKE @tp AND DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@tp", $"%{tipoPag}%"),
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: PAGAR PAGO] Busca status de um documento específico pelo seu número e parcela.")]
    public async Task<string> GetStatusDocumento(
        [Description("Número do documento")] string numeroDoc,
        [Description("Número da parcela (opcional)")] string parcela)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_PAGO WHERE DOCUMENTO = @doc";
        var list = new List<SqlParameter> { new SqlParameter("@doc", numeroDoc) };
        if (!string.IsNullOrEmpty(parcela)) { sq += " AND PARCELA = @par"; list.Add(new SqlParameter("@par", parcela)); }
        return await ExecuteQuery(sq, list.ToArray());
    }

    [Description("[DOMÍNIO: PAGAR PAGO] Soma pagamentos filtrando por Região (Estado ou Cidade).")]
    public async Task<string> GetSomaPorLocalidade(
        [Description("Sigla do Estado (Ex: SP)")] string uf,
        [Description("Nome da Cidade (Opcional)")] string cidade,
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORPAG) as TotalLocalidade, COUNT(*) as Quantidade FROM VW_DOC_FIN_PAG_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        var list = new List<SqlParameter> { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) 
        };
        if (!string.IsNullOrEmpty(uf)) { sq += " AND UF = @uf"; list.Add(new SqlParameter("@uf", uf)); }
        if (!string.IsNullOrEmpty(cidade)) { sq += " AND CIDADE LIKE @cid"; list.Add(new SqlParameter("@cid", $"%{cidade}%")); }
        return await ExecuteQuery(sq, list.ToArray());
    }

    [Description("[DOMÍNIO: PAGAR PAGO] Busca contas a pagar que foram pagas com ATRASO (Data de Pagamento > Data de Vencimento) em um período.")]
    public async Task<string> GetPagamentosAtrasadosRealizados(
        [Description("Data inicial Pagamento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Pagamento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_PAGO WHERE DATAPAGAMENTO > DATAVENCIMENTO AND DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    // --- VW_DOC_FIN_REC_ABERTO ---

    [Description("[DOMÍNIO: RECEBER ABERTO] SOMA SINTÉTICA (Apenas Valores): Calcula o Valor Total de documentos que VENCEM dentro de um período específico (passado ou futuro). USE APENAS quando o usuário pedir o TOTAL/SOMA de um período e NÃO quiser a lista detalhada.")]
    public async Task<string> GetSomaVencimentosNoPeriodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = @"SELECT 
                      SUM(VALORORIG - ISNULL(VALORPAG, 0)) as SaldoPendenteNoPeriodo, 
                      COUNT(*) as QuantidadeTitulos 
                   FROM VW_DOC_FIN_REC_ABERTO 
                   WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
                   
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Busca clientes com pagamentos ATRASADOS/(a receber, em aberto) (vencimento < hoje e sem pagamento).")]
    public async Task<string> GetReceberAtrasados()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Busca boletos/títulos que venceram/(a receber, em aberto) no MÊS ANTERIOR e continuam em aberto.")]
    public async Task<string> GetReceberVencidosNoMesAnterior()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND DATAVENCIMENTO >= DATEADD(month, -1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Busca a dívida (inadimplência) mais ANTIGA que temos para receber.")]
    public async Task<string> GetMaiorInadimplenciaAntiga()
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE) ORDER BY DATAVENCIMENTO ASC";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Conta quantos títulos/boletos estão vencendo/(a receber, em aberto) EXATAMENTE HOJE e não foram pagos.")]
    public async Task<string> GetContagemReceberVencidosHoje()
    {
        var sq = "SELECT COUNT(*) as QtdVencidosHoje, SUM(VALORORIG) as ValorTotal FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO = CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Conta quantos títulos/boletos estão à vencer/(a receber ou em aberto).")]
    public async Task<string> GetContagemReceberAVencer()
    {
        var sq = "SELECT COUNT(*) as QtdVencidosHoje, SUM(VALORORIG) as ValorTotal FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO > CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Conta quantos títulos/boletos estão vencidos/(a receber ou em aberto).")]
    public async Task<string> GetContagemReceberVencidos()
    {
        var sq = "SELECT COUNT(*) as QtdVencidosHoje, SUM(VALORORIG) as ValorTotal FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] PREVISÃO DE CAIXA: Busca o que está programado para receber em um período futuro (ou hoje).")]
    public async Task<string> GetPrevisaoRecebimentoNoPeriodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Soma o valor total PENDENTE de vencer/(a receber ou em aberto) em um período. USE APENAS se o usuário especificar um período explícito.")]
    public async Task<string> GetSomaReceberPendente(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORORIG) as TotalPendente, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] OPÇÃO PRINCIPAL: Soma o valor TOTAL GERAL de documentos a receber (em aberto) incluindo TODO O FUTURO. Use esta tool OBRIGATORIAMENTE quando pedirem 'valor total a receber', 'total da carteira' ou 'quantidade a receber' sem citar período.")]
    public async Task<string> GetResumoGeralReceberAberto()
    {
        var sq = "SELECT SUM(VALORORIG) as TotalGeralEmAberto, COUNT(*) as QuantidadeTotalTitulos FROM VW_DOC_FIN_REC_ABERTO";
        return await ExecuteQuery(sq, System.Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Mostra o resumo do total a receber e a quantidade de títulos, AGRUPADO (quebrado) POR ANO de vencimento (inclui anos passados, ano atual e anos futuros).")]
    public async Task<string> GetResumoReceberAbertoAgrupadoPorAno()
    {
        var sq = "SELECT YEAR(DATAVENCIMENTO) as Ano, SUM(VALORORIG) as TotalEmAberto, COUNT(*) as QuantidadeTitulos FROM VW_DOC_FIN_REC_ABERTO GROUP BY YEAR(DATAVENCIMENTO) ORDER BY Ano";
        return await ExecuteQuery(sq, System.Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Calcula o montante TOTAL da inadimplência (tudo que já venceu e não foi pago) até hoje.")]
    public async Task<string> GetSomaInadimplenciaTotal()
    {
        var sq = "SELECT SUM(VALORORIG) as TotalInadimplencia, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Busca o maior boleto que temos para receber em um mês específico.")]
    public async Task<string> GetMaiorReceberAbertoNoMes(
        [Description("Ano (4 digitos)")] int ano, 
        [Description("Mês (1 a 12)")] int mes)
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE YEAR(DATAVENCIMENTO) = @ano AND MONTH(DATAVENCIMENTO) = @mes ORDER BY VALORORIG DESC";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano), new SqlParameter("@mes", mes) });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Busca todas as faturas em aberto (pendentes) de um Cliente específico (Limitado a 50 registros).")]
    public async Task<string> GetPendenciasPorCliente([Description("Nome ou Fantasia do Cliente")] string nome)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE (UPPER(CLIENTE) LIKE UPPER(@n) OR UPPER(NOMEFANTASIA) LIKE UPPER(@n)) ORDER BY DATAVENCIMENTO ASC";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@n", $"%{nome}%") });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Busca detalhes de um boleto a receber específico que ainda está EM ABERTO.")]
    public async Task<string> GetDetalhesBoletoReceber(
        [Description("Número do documento")] string numeroDoc,
        [Description("Número da parcela (opcional)")] string parcela)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE DOCUMENTO = @doc";
        var list = new List<SqlParameter> { new SqlParameter("@doc", numeroDoc) };
        if (!string.IsNullOrEmpty(parcela)) { sq += " AND PARCELA = @par"; list.Add(new SqlParameter("@par", parcela)); }
        return await ExecuteQuery(sq, list.ToArray());
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] PREVISÃO POR MÉTODO: Soma o valor esperado de recebimento filtrando por Tipo de Pagamento (PIX, Boleto, etc).")]
    public async Task<string> GetContagemSomaEsperadaPorMetodo(
        [Description("Tipo de Pagamento")] string tipoPag,
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORORIG) as TotalEsperado, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_ABERTO WHERE TIPOPAG LIKE @tp AND DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@tp", $"%{tipoPag}%"),
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Busca boletos em aberto filtrando pela Condição de Pagamento (Ex: 90 dias).")]
    public async Task<string> GetPendentesPorCondicaoRecebimento([Description("Condição de Pagamento")] string condPag)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE CONDPAG LIKE @cp";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@cp", $"%{condPag}%") });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Busca previsões de recebimento filtrando por Filial.")]
    public async Task<string> GetReceberPendentesPorFilial(
        [Description("Nome da filial")] string empresa,
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORORIG) as TotalPendente, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_ABERTO WHERE EMPRESA LIKE @emp AND DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@emp", $"%{empresa}%"),
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Soma o total a receber de clientes de um Estado (UF) específico.")]
    public async Task<string> GetReceberPendentesPorEstado([Description("Sigla do Estado (Ex: MG)")] string uf)
    {
        var sq = "SELECT SUM(VALORORIG) as TotalEstado, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_ABERTO WHERE UF = @uf";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@uf", uf) });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Busca clientes de uma Cidade específica que estão com boletos em aberto.")]
    public async Task<string> GetReceberPendentesPorCidade([Description("Nome da Cidade")] string cidade)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE CIDADE LIKE @c";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@c", $"%{cidade}%") });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Busca faturas em aberto pelo CNPJ/CPF exato do cliente.")]
    public async Task<string> GetReceberAbertoPorCNPJ([Description("CNPJ somente números")] string cpfCnpj)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE CPFCNPJ = @cnpj";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@cnpj", LimparCnpj(cpfCnpj)) });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Conta a Quantidade Total de recebimentos que vencem/venceram em um Mês e Ano específicos.")]
    public async Task<string> GetContagemReceberAbertoPorMesVencimento(
        [Description("Ano com 4 digitos. Ex: 2026")] string ano, 
        [Description("Mês com 2 digitos. Ex: 02")] string mes)
    {
        var sq = "SELECT COUNT(*) as QuantidadeReceber FROM VW_DOC_FIN_REC_ABERTO WHERE YEAR(DATAVENCIMENTO) = @ano AND MONTH(DATAVENCIMENTO) = @mes";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano), new SqlParameter("@mes", mes) });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] FILTRO EXCLUSIVO POR ANO DE VENCIMENTO: Conta a Quantidade de títulos e o Valor Total a receber de um ANO ESPECÍFICO. USE ESTA FERRAMENTA OBRIGATORIAMENTE se o usuário digitar um ano (ex: 2026), mesmo que ele também use palavras como 'vencidos', 'a vencer' ou 'em aberto'. A prioridade é sempre o ANO.")]
    public async Task<string> GetContagemReceberAbertoPorAnoVencimento(
        [Description("Ano com 4 digitos. Ex: 2026")] string ano)
    {
        var sq = @"SELECT 
                      COUNT(*) as QuantidadeTitulos, 
                      SUM(VALORORIG - ISNULL(VALORPAG, 0)) as TotalNoAno 
                   FROM VW_DOC_FIN_REC_ABERTO 
                   WHERE YEAR(DATAVENCIMENTO) = @ano";
                   
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano)});
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] AGRUPAMENTO POR CLIENTE EM UM ANO: Retorna o Valor Total a receber e a Quantidade de títulos agrupados por Cliente, em um ANO específico. Use quando pedirem 'ranking de clientes', 'maiores clientes' ou 'listar por cliente' no ano.")]
    public async Task<string> GetResumoReceberAgrupadoPorClienteNoAno(
        [Description("Ano com 4 digitos. Ex: 2026")] string ano,
        [Description("Critério de ordenação. Valores permitidos: 'VALOR_DESC' (Maiores valores - Padrão), 'VALOR_ASC' (Menores valores), 'QTD_DESC' (Maior volume), 'QTD_ASC' (Menor volume), 'CLIENTE_ASC' (Ordem alfabética A-Z), 'CLIENTE_DESC' (Ordem alfabética Z-A).")] string ordenacao = "VALOR_DESC")
    {
        // Trava de Segurança
        string orderByClause = (ordenacao?.ToUpper()) switch
        {
            "VALOR_ASC"    => "TotalAberto ASC",
            "QTD_DESC"     => "QuantidadeTitulos DESC",
            "QTD_ASC"      => "QuantidadeTitulos ASC",
            "CLIENTE_ASC"  => "CLIENTE ASC",
            "CLIENTE_DESC" => "CLIENTE DESC",
            _              => "TotalAberto DESC" // Fallback seguro
        };

        var sq = $@"SELECT CLIENTE, 
                      SUM(VALORORIG - ISNULL(VALORPAG, 0)) as TotalAberto, 
                      COUNT(*) as QuantidadeTitulos 
                   FROM VW_DOC_FIN_REC_ABERTO 
                   WHERE YEAR(DATAVENCIMENTO) = @ano
                   GROUP BY CLIENTE 
                   ORDER BY {orderByClause}";
                   
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano) });
    }

    /*[Description("[DOMÍNIO: RECEBER ABERTO] FATURAMENTO / TÍTULOS EMITIDOS: Calcula o valor total GERAL de contas a receber que foram GERADAS/EMITIDAS em um determinado período de datas. ATENÇÃO: Esta ferramenta usa a Data de Emissão e ignora se está pago ou aberto. USE SEMPRE que o usuário usar as palavras 'faturamos', 'emitimos', 'vendemos', 'geramos' em um período.")]
    public async Task<string> GetFaturamentoEmitidoNoPeriodo(
        [Description("Data inicial da emissão (ISO 8601)")] string dataInicioISO, 
        [Description("Data final da emissão (ISO 8601)")] string dataFimISO)
    {
        // Consultando direto as tabelas base para não perder os títulos que já foram pagos
        var sq = @"SELECT 
                      SUM(b.ParcDocFinValOrig) as TotalFaturado, 
                      COUNT(*) as QuantidadeTitulosEmitidos 
                   FROM doc_fin a
                   INNER JOIN parc_doc_fin b ON a.EmpCod = b.EmpCod AND a.DocFinChv = b.DocFinChv
                   WHERE a.DocFinTipoLanc = 'REC' 
                     AND a.DocFinDataEmissao >= @dF AND a.DocFinDataEmissao <= @dT";
                   
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }*/

    [Description("[DOMÍNIO: RECEBER ABERTO] FLUXO DE CAIXA LÍQUIDO / SALDO PROJETADO: Calcula o cruzamento entre as Entradas (Receitas a Receber) e Saídas (Despesas a Pagar) de um determinado período para entregar o Saldo Líquido. USE OBRIGATORIAMENTE quando o usuário pedir 'saldo líquido', 'projeção de caixa', 'o que sobra', ou quiser comparar o receber vs pagar de um período (ex: próxima semana, próximo mês).")]
    public async Task<string> GetFluxoCaixaLiquidoNoPeriodo(
        [Description("Data inicial do período (ISO 8601)")] string dataInicioISO, 
        [Description("Data final do período (ISO 8601)")] string dataFimISO)
    {
        // O SQL já faz todo o trabalho pesado. Usamos ISNULL para evitar que um período sem despesas quebre a conta com um valor NULL.
        var sq = @"
            SELECT 
                TotalReceitas as ReceitasPrevistas, 
                TotalDespesas as DespesasPrevistas, 
                (TotalReceitas - TotalDespesas) as SaldoLiquidoProjetado
            FROM (
                SELECT 
                    (SELECT ISNULL(SUM(VALORORIG - ISNULL(VALORPAG, 0)), 0) 
                     FROM VW_DOC_FIN_REC_ABERTO 
                     WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT) as TotalReceitas,
                     
                    (SELECT ISNULL(SUM(VALORORIG - ISNULL(VALORPAG, 0)), 0) 
                     FROM VW_DOC_FIN_PAG_ABERTO 
                     WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT) as TotalDespesas
            ) FluxoBase";
                   
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    // --- VW_DOC_FIN_REC_PAGO ---

    [Description("[DOMÍNIO: RECEBER PAGO] Busca faturamento/recebimentos que JÁ FORAM RECEBIDOS em um período. Ex: O que recebemos hoje? Quem pagou ontem?")]
    public async Task<string> GetRecebidosNoPeriodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Calcula o Valor Total Recebido (Soma) em um período.")]
    public async Task<string> GetSomaRecebidoNoPeriodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORPAG) as TotalRecebido, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Analisa a saúde dos recebimentos: Soma de Juros cobrados (pago > original) e Descontos concedidos (pago < original).")]
    public async Task<string> GetAnaliseRecebimentosJurosDescontos(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = @"SELECT 
                    SUM(CASE WHEN VALORPAG > VALORORIG THEN VALORPAG - VALORORIG ELSE 0 END) as TotalJurosCobrados,
                    SUM(CASE WHEN VALORPAG < VALORORIG THEN VALORORIG - VALORPAG ELSE 0 END) as TotalDescontosConcedidos,
                    COUNT(CASE WHEN VALORPAG > VALORORIG THEN 1 END) as QtdComJuros,
                    COUNT(CASE WHEN VALORPAG < VALORORIG THEN 1 END) as QtdComDesconto
                   FROM VW_DOC_FIN_REC_PAGO 
                   WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Busca o maior pagamento individual recebido de um cliente no período.")]
    public async Task<string> GetMaiorRecebimentoNoPeriodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT ORDER BY VALORPAG DESC";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Busca clientes que pagaram faturas ATRASADAS (data pagamento > vencimento) no período.")]
    public async Task<string> GetRecebimentosAtrasadosLiquidados(
        [Description("Data inicial Pagamento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Pagamento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO > DATAVENCIMENTO AND DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Busca recebimentos que vencião no ANO PASSADO mas que o cliente só pagou ESTE ANO.")]
    public async Task<string> GetRecebidosLancadosAnoPassadoPagosAgora()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE YEAR(DATAVENCIMENTO) = YEAR(GETDATE()) - 1 AND YEAR(DATAPAGAMENTO) = YEAR(GETDATE())";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Identifica qual cliente demorou mais dias para pagar uma fatura (maior diferença entre vencimento e pagamento).")]
    public async Task<string> GetMaiorAtrasoLiquidadoNoPeriodo(
        [Description("Data inicial Pagamento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Pagamento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS}, DATEDIFF(day, DATAVENCIMENTO, DATAPAGAMENTO) as DiasAtraso FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO > DATAVENCIMENTO AND DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT ORDER BY DiasAtraso DESC";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Busca todos os pagamentos realizados por um Cliente específico em um período.")]
    public async Task<string> GetRecebimentosPorCliente(
        [Description("Nome do Cliente ou Fantasia")] string cliente,
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE (UPPER(CLIENTE) LIKE UPPER(@nc) OR UPPER(NOMEFANTASIA) LIKE UPPER(@nc)) AND DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@nc", $"%{cliente}%"),
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Traz os Top Clientes que mais geraram receita (Soma VALORPAG) no período.")]
    public async Task<string> GetTopClientesPorRecebimento(
        [Description("Quantidade de clientes (Top X). Ex: 5")] int quantidade,
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT TOP (@qty) CLIENTE, SUM(VALORPAG) as TotalPago FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT GROUP BY CLIENTE ORDER BY TotalPago DESC";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@qty", quantidade),
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Soma o quanto entrou no caixa filtrando pelo Meio de Recebimento (PIX, Cartão, etc).")]
    public async Task<string> GetSomaRecebidoPorMetodo(
        [Description("Tipo de Pagamento. Ex: PIX, CARTAO")] string tipoPag,
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORPAG) as TotalMetodo, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_PAGO WHERE TIPOPAG LIKE @tp AND DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@tp", $"%{tipoPag}%"),
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Conta a Quantidade Total de boletos/títulos liquidados/pagos pelos clientes no período.")]
    public async Task<string> GetQuantidadeRecebidosPorPeriodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT COUNT(*) as QuantidadeLiquidados FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Busca recebimentos filtrando pela Condição de Pagamento (Ex: 30/60/90).")]
    public async Task<string> GetRecebidosPorCondicaoPagamento(
        [Description("Condição de Pagamento")] string condPag,
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE CONDPAG LIKE @cp AND DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@cp", $"%{condPag}%"),
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Soma o faturamento arrecadado por uma Filial específica.")]
    public async Task<string> GetRecebidoPorFilial(
        [Description("Nome da filial")] string empresa,
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORPAG) as TotalArrecadado, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_PAGO WHERE EMPRESA LIKE @emp AND DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@emp", $"%{empresa}%"),
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Soma faturamento filtrando por Localidade (UF ou Cidade).")]
    public async Task<string> GetRecebidoPorLocalidade(
        [Description("Sigla do Estado (Ex: SP)")] string uf,
        [Description("Nome da Cidade (Opcional)")] string cidade,
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORPAG) as TotalLocalidade, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        var list = new List<SqlParameter> { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) 
        };
        if (!string.IsNullOrEmpty(uf)) { sq += " AND UF = @uf"; list.Add(new SqlParameter("@uf", uf)); }
        if (!string.IsNullOrEmpty(cidade)) { sq += " AND CIDADE LIKE @cid"; list.Add(new SqlParameter("@cid", $"%{cidade}%")); }
        return await ExecuteQuery(sq, list.ToArray());
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Busca status de um documento RECEBIDO/PAGO pelo cliente.")]
    public async Task<string> GetStatusDocumentoRecebido(
        [Description("Número do documento")] string numeroDoc,
        [Description("Número da parcela (opcional)")] string parcela)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE DOCUMENTO = @doc";
        var list = new List<SqlParameter> { new SqlParameter("@doc", numeroDoc) };
        if (!string.IsNullOrEmpty(parcela)) { sq += " AND PARCELA = @par"; list.Add(new SqlParameter("@par", parcela)); }
        return await ExecuteQuery(sq, list.ToArray());
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Busca contas RECEBIDAS em um período pelo CNPJ/CPF exato do cliente.")]
    public async Task<string> GetReceberPagoPorCNPJ([Description("CNPJ somente números")] string cpfCnpj)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE CPFCNPJ = @cnpj";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@cnpj", LimparCnpj(cpfCnpj)) });
    }

    [Description("Conta a Quantidade Total de contas RECEBIDAS em um Mês e Ano específicos.")]
    public async Task<string> GetContagemReceberPagoPorMesRecebimento(
        [Description("Ano com 4 digitos. Ex: 2026")] string ano, 
        [Description("Mês com 2 digitos. Ex: 02")] string mes)
    {
        var sq = "SELECT COUNT(*) as QuantidadeRecebida FROM VW_DOC_FIN_REC_PAGO WHERE YEAR(DATAPAGAMENTO) = @ano AND MONTH(DATAPAGAMENTO) = @mes";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano), new SqlParameter("@mes", mes) });
    }


    // --- FASE 3: NOVOS PLUGINS ANALÍTICOS ---

    [Description("[DOMÍNIO: RECEBER ABERTO] Retorna títulos a receber que estão atrasados dentro de uma faixa específica de dias. Útil ao perguntar 'quais clientes estão na faixa de 90 dias de atraso?'.")]
    public async Task<string> GetReceivablesByAgeRange(
        [Description("Mínimo de dias em atraso. Ex: 61")] int diasAtrasoInicio,
        [Description("Máximo de dias em atraso. Ex: 90")] int diasAtrasoFim)
    {
        var sq = $@"SELECT {BASE_COLUMNS}, DATEDIFF(day, DATAVENCIMENTO, CAST(GETDATE() AS DATE)) as DiasAtraso
                    FROM VW_DOC_FIN_REC_ABERTO 
                    WHERE DATAVENCIMENTO <= DATEADD(day, -@dMin, CAST(GETDATE() AS DATE)) 
                      AND DATAVENCIMENTO >= DATEADD(day, -@dMax, CAST(GETDATE() AS DATE))
                    ORDER BY DiasAtraso DESC";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@dMin", diasAtrasoInicio), new SqlParameter("@dMax", diasAtrasoFim) });
    }

    [Description("[DOMÍNIO: RECEBER PAGO E RECEBER ABERTO] Busca todo o histórico de contas a receber (em aberto e pagas) de um cliente para análise individual completa.")]
    public async Task<string> GetClientReceivablesHistory(string clientId)
    {
        var sq = $@"SELECT 'Aberto' as Status, {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE UPPER(CLIENTE) LIKE UPPER(@c) OR CPFCNPJ = @c
                    UNION ALL 
                    SELECT 'Pago' as Status, {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE UPPER(CLIENTE) LIKE UPPER(@c) OR CPFCNPJ = @c
                    ORDER BY DATAVENCIMENTO DESC";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@c", $"%{clientId}%") });
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Mede a performance e eficiência de pagamento. Compara o valor faturado com os recebimentos e atrasos de faturas.")]
    public async Task<string> GetInvoicePaymentPerformance(string dataInicioISO, string dataFimISO)
    {
        var sq = $@"SELECT 
                      COUNT(CASE WHEN DATAPAGAMENTO > DATAVENCIMENTO THEN 1 END) as FaturasPagasComAtraso,
                      COUNT(CASE WHEN DATAPAGAMENTO <= DATAVENCIMENTO THEN 1 END) as FaturasPagasEmDia,
                      AVG(CAST(DATEDIFF(day, DATAVENCIMENTO, DATAPAGAMENTO) AS FLOAT)) as MediaDiasAtrasoGeral
                    FROM VW_DOC_FIN_REC_PAGO 
                    WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: PAGAR ABERTO] Busca detalhadamente os principais credores (Fornecedores que mais devemos) limitados por uma quantidade desejada.")]
    public async Task<string> GetTopCreditorsDetails(int limit)
    {
        var sq = $@"SELECT TOP {limit} CLIENTE as NomeFornecedor, SUM(VALORORIG) as TotalPendente, COUNT(*) as QuantidadeTitulos
                    FROM VW_DOC_FIN_PAG_ABERTO 
                    GROUP BY CLIENTE ORDER BY TotalPendente DESC";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: PAGAR ABERTO] Aviso de alertas de pagamentos que vencem num período específico (próximos pagamentos a realizar). O mesmo que contas a pagar em um período.")]
    public async Task<string> GetUpcomingPayables(string dataInicioISO, string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT ORDER BY DATAVENCIMENTO ASC";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: PAGAR ABERTO E PAGAR PAGO] Busca o comportamento financeiro com um fornecedor específico: títulos que já pagamos e os que estão pra vencer.")]
    public async Task<string> GetSupplierAnalytics(string supplierIdentifier)
    {
        var sq = $@"SELECT 'Pendente' as Tipo, {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE UPPER(CLIENTE) LIKE UPPER(@f)
                    UNION ALL 
                    SELECT 'Pago' as Tipo, {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_PAGO WHERE UPPER(CLIENTE) LIKE UPPER(@f)
                    ORDER BY DATAVENCIMENTO DESC";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@f", $"%{supplierIdentifier}%") });
    }

    [Description("[DOMÍNIO: PAGAR ABERTO] Simula o fluxo de caixa dia-a-dia cruzando Receitas e Despesas agrupadas pela Data de Vencimento.")]
    public async Task<string> GetProjectedDailyCashFlow(int nextDays)
    {
        var sq = $@"
            SELECT 
                ISNULL(R.Dia, P.Dia) as DataFluxo,
                ISNULL(R.TotalReceitas, 0) as ReceitasDia,
                ISNULL(P.TotalDespesas, 0) as DespesasDia,
                (ISNULL(R.TotalReceitas, 0) - ISNULL(P.TotalDespesas, 0)) as SaldoLiquidoDia
            FROM (
                SELECT CAST(DATAVENCIMENTO AS DATE) as Dia, SUM(VALORORIG - ISNULL(VALORPAG, 0)) as TotalReceitas
                FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO >= CAST(GETDATE() AS DATE) AND DATAVENCIMENTO <= DATEADD(day, @dias, CAST(GETDATE() AS DATE))
                GROUP BY CAST(DATAVENCIMENTO AS DATE)
            ) R
            FULL OUTER JOIN (
                SELECT CAST(DATAVENCIMENTO AS DATE) as Dia, SUM(VALORORIG) as TotalDespesas
                FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO >= CAST(GETDATE() AS DATE) AND DATAVENCIMENTO <= DATEADD(day, @dias, CAST(GETDATE() AS DATE))
                GROUP BY CAST(DATAVENCIMENTO AS DATE)
            ) P ON R.Dia = P.Dia
            ORDER BY DataFluxo ASC";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@dias", nextDays) });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO E PAGAR ABERTO] Relatório global instantâneo de saúde financeira (Total Atrasado Receber vs Pagar e Inadimplência global).")]
    public async Task<string> GetFinancialHealthReportDetails()
    {
        var sq = @"SELECT 
                    (SELECT SUM(VALORORIG) FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)) as TotalInadimplenciaClientes,
                    (SELECT SUM(VALORORIG) FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)) as TotalAtrasadoFornecedores,
                    (SELECT SUM(VALORORIG) FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO >= CAST(GETDATE() AS DATE)) as TotalReceberAberto,
                    (SELECT SUM(VALORORIG) FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO >= CAST(GETDATE() AS DATE)) as TotalPagarAberto";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Calcula o Valor Total Recebido filtrando simultaneamente por Meio de Pagamento (ex: PIX) e Filial/Empresa em um período específico. Ex: Quanto recebemos em PIX na filial SP-01 mês passado?")]
    public async Task<string> GetSomaRecebidoPorMetodoEFilial(
       [Description("Tipo de Pagamento. Ex: PIX, BOLETO, CARTAO")] string tipoPag,
       [Description("Nome da filial (EMPRESA). Ex: SP-01")] string empresa,
       [Description("Data inicial (ISO 8601)")] string dataInicioISO,
       [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = @"SELECT 
                      SUM(VALORPAG) as TotalRecebido, 
                      COUNT(*) as Quantidade 
                   FROM VW_DOC_FIN_REC_PAGO 
                   WHERE TIPOPAG LIKE @tp 
                     AND EMPRESA LIKE @emp 
                     AND DATAPAGAMENTO >= @dF 
                     AND DATAPAGAMENTO <= @dT";

        return await ExecuteQuery(sq, new[] {
            new SqlParameter("@tp", $"%{tipoPag}%"),
            new SqlParameter("@emp", $"%{empresa}%"),
            new SqlParameter("@dF", ParseDate(dataInicioISO)),
            new SqlParameter("@dT", ParseDate(dataFimISO))
        });
    }

    [Description("[DOMÍNIO: RECEBER ABERTO E PAGAR ABERTO] Analisa as pendências a pagar e a receber divididas pelas diversas EMPRESAS / Filiais registradas.")]
    public async Task<string> GetLiquidityByBranch()
    {
        var sq = @"
            SELECT 
                ISNULL(R.EMPRESA, P.EMPRESA) as Filial,
                ISNULL(R.TotalReceber, 0) as TotalPendenteRecebimentos,
                ISNULL(P.TotalPagar, 0) as TotalPendenteObrigacoes,
                (ISNULL(R.TotalReceber, 0) - ISNULL(P.TotalPagar, 0)) as LiquidezFutura
            FROM (
                SELECT EMPRESA, SUM(VALORORIG) as TotalReceber FROM VW_DOC_FIN_REC_ABERTO GROUP BY EMPRESA
            ) R
            FULL OUTER JOIN (
                SELECT EMPRESA, SUM(VALORORIG) as TotalPagar FROM VW_DOC_FIN_PAG_ABERTO GROUP BY EMPRESA
            ) P ON R.EMPRESA = P.EMPRESA";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    // --- HELPERS E EXECUTOR BASE ---

    private string ParseDate(string isoDate)
    {
        // Garante o fallback pra yyyyMMdd do SQL Server
        if (DateTime.TryParse(isoDate, out var dt)) return dt.ToString("yyyyMMdd");
        return isoDate; // Devolve puro caso dê treta no parse, o SQL Server tenta se virar
    }

    private string LimparCnpj(string input)
    {
        if (string.IsNullOrEmpty(input)) return "";
        return new string(input.Where(char.IsDigit).ToArray());
    }

    private async Task<string> ExecuteQuery(string queryText, SqlParameter[] parameters)
    {
        if (string.IsNullOrEmpty(_connectionString))
            return "{\"error\": \"Connection string 'DefaultConnection' not found.\"}";

        try
        {
            Console.WriteLine($"[ErpPlugin] 🟢 EXECUTING EXACT QUERY: {queryText}");
            foreach (var p in parameters) Console.WriteLine($"   -> Param {p.ParameterName}: {p.Value}");

            // Track query for SQL transparency feature
            var paramInfo = string.Join(", ", parameters.Select(p => $"{p.ParameterName}='{p.Value}'"));
            ExecutedQueries.Add(string.IsNullOrEmpty(paramInfo) ? queryText : $"{queryText}  -- Params: {paramInfo}");

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var command = new SqlCommand(queryText, connection);
            command.Parameters.AddRange(parameters);

            using var reader = await command.ExecuteReaderAsync();
            var results = new List<Dictionary<string, object>>();

            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[reader.GetName(i)] = reader.GetValue(i) == DBNull.Value ? null! : reader.GetValue(i);
                }
                results.Add(row);
            }

            var jsonResult = JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true });
            Console.WriteLine($"[ErpPlugin] 🟢 QUERY SUCCESS. Returned {results.Count} rows.");
            return jsonResult;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ErpPlugin] 🔴 FATAL QUERY ERROR: {ex.Message}");
            return $"{{\"error\": \"Database error: {ex.Message}\"}}";
        }
    }
}
