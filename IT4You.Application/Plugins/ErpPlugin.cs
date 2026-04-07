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

    public ErpPlugin(IConfiguration configuration)
    {
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection") ?? "";
    }
    private const string BASE_COLUMNS = "EMPRESA, CLIENTE, NOMEFANTASIA, CPFCNPJ, CIDADE, UF, DOCUMENTO, EMISSAO, VALORDOC, PARCELA, VALORORIG, VALORPAG, DATAVENCIMENTO, DATAPAGAMENTO, CONDPAG, TIPOPAG, SITUACAO";

    // --- VW_DOC_FIN_PAG_ABERTO ---

    [Description("Busca contas a PAGAR em aberto que vencem em um período. Ex: O que temos para pagar hoje? O que vence amanhã? Próxima semana?")]
    public async Task<string> GetVencendoNoPeriodo(
        [Description("Data inicial Vencimento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Vencimento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Busca contas a PAGAR que já estão ATRASADAS (vencimento menor que hoje e sem data de pagamento).")]
    public async Task<string> GetAtrasados()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("Calcula o Valor Total Pendente (Soma) de contas a pagar em um período de vencimento.")]
    public async Task<string> GetSomaTotalPendente(
        [Description("Data inicial Vencimento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Vencimento (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORORIG) as TotalPendente, COUNT(*) as Quantidade FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Calcula o Valor Total de contas ATRASADAS em reais.")]
    public async Task<string> GetTotalAtrasado()
    {
        var sq = "SELECT SUM(VALORORIG) as TotalAtrasado, COUNT(*) as Quantidade FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("Busca a MAIOR conta pendente (valor mais alto) que vence em um período.")]
    public async Task<string> GetMaiorPendenteNoPeriodo(
        [Description("Data inicial Vencimento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Vencimento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT ORDER BY VALORORIG DESC";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Busca contas pendentes com valor ORIGINAL acima de um limite. Ex: Boletos acima de 50 mil.")]
    public async Task<string> GetPendentesAcimaDeValor([Description("Valor mínimo. Ex: 50000")] decimal valorMinimo)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE VALORORIG >= @val ORDER BY VALORORIG DESC";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@val", valorMinimo) });
    }

    public async Task<string> GetDividaPorFornecedor(
        [Description("Nome do Fornecedor ou Fantasia")] string fornecedor)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE UPPER(CLIENTE) LIKE UPPER(@nf) OR UPPER(NOMEFANTASIA) LIKE UPPER(@nf)";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@nf", $"%{fornecedor}%") });
    }

    [Description("Busca contas que foram LANÇADAS/EMITIDAS hoje no sistema.")]
    public async Task<string> GetLancadosHoje()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE CAST(EMISSAO AS DATE) = CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("Busca documentos que foram emitidos no ANO PASSADO mas que o vencimento é este ano.")]
    public async Task<string> GetEmitidosAnoPassadoVencendoEsteAno()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE YEAR(EMISSAO) = YEAR(GETDATE()) - 1 AND YEAR(DATAVENCIMENTO) = YEAR(GETDATE())";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("Busca contas em aberto filtrando por Filial (EMPRESA).")]
    public async Task<string> GetPendentesPorFilial([Description("Nome da filial")] string empresa)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE EMPRESA LIKE @emp";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@emp", $"%{empresa}%") });
    }

    [Description("Busca fornecedores em aberto por Estado (UF).")]
    public async Task<string> GetPendentesPorEstado([Description("Sigla do Estado. Ex: MG")] string uf)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE UF = @uf";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@uf", uf) });
    }

    [Description("Busca detalhes de um documento específico em aberto.")]
    public async Task<string> GetDetalhesBoletoEmAberto(
        [Description("Número do documento")] string numeroDoc,
        [Description("Número da parcela (opcional)")] string parcela)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE DOCUMENTO = @doc";
        var list = new List<SqlParameter> { new SqlParameter("@doc", numeroDoc) };
        if (!string.IsNullOrEmpty(parcela)) { sq += " AND PARCELA = @par"; list.Add(new SqlParameter("@par", parcela)); }
        return await ExecuteQuery(sq, list.ToArray());
    }

    [Description("Busca contas a pagar de um fornecedor que vencem num período.")]
    public async Task<string> GetPagarAbertoPorNomeFornecedor([Description("Nome do Fornecedor ou Fantasia")] string nomeFornecedor)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE UPPER(CLIENTE) LIKE UPPER(@nf) OR UPPER(NOMEFANTASIA) LIKE UPPER(@nf)";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@nf", $"%{nomeFornecedor}%") });
    }

    [Description("Busca contas a PAGAR em aberto pelo CNPJ exato do fornecedor.")]
    public async Task<string> GetPagarAbertoPorCNPJ([Description("CNPJ somente números")] string cpfCnpj)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_ABERTO WHERE CPFCNPJ = @cnpj";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@cnpj", LimparCnpj(cpfCnpj)) });
    }

    [Description("Soma o valor esperado de pagamento filtrando por Tipo de Pagamento (PIX, Boleto, etc).")]
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

    [Description("Conta a Quantidade Total de boletos/contas a PAGAR que vencem em um Mês e Ano específicos.")]
    public async Task<string> GetContagemPagarAbertoPorMesVencimento(
        [Description("Ano com 4 digitos. Ex: 2026")] string ano, 
        [Description("Mês com 2 digitos. Ex: 02")] string mes)
    {
        var sq = "SELECT COUNT(*) as QuantidadeContas FROM VW_DOC_FIN_PAG_ABERTO WHERE YEAR(DATAVENCIMENTO) = @ano AND MONTH(DATAVENCIMENTO) = @mes";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano), new SqlParameter("@mes", mes) });
    }

    // --- VW_DOC_FIN_PAG_PAGO ---

    [Description("Busca contas que JÁ FORAM PAGAS em um período. Ex: pagamentos feitos hoje ou mês passado.")]
    public async Task<string> GetPagosNoPeriodo(
        [Description("Data inicial pagamento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final pagamento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Calcula o Valor Total Gasto (Soma) em pagamentos em um período.")]
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

    [Description("Analisa a saúde financeira dos pagamentos: Soma de Juros (pago > original) e Descontos (pago < original) no período.")]
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

    [Description("Busca a conta com o MAIOR valor pago em um período.")]
    public async Task<string> GetPagamentoMaiorValor(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT ORDER BY VALORPAG DESC";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Busca pagamentos por Fornecedor (Nome ou Fantasia) em um período.")]
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

    [Description("Soma o quanto foi pago filtrando pelo Meio de Pagamento (PIX, Boleto, Dinheiro, etc).")]
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

    [Description("Busca status de um documento específico pelo seu número e parcela.")]
    public async Task<string> GetStatusDocumento(
        [Description("Número do documento")] string numeroDoc,
        [Description("Número da parcela (opcional)")] string parcela)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_PAG_PAGO WHERE DOCUMENTO = @doc";
        var list = new List<SqlParameter> { new SqlParameter("@doc", numeroDoc) };
        if (!string.IsNullOrEmpty(parcela)) { sq += " AND PARCELA = @par"; list.Add(new SqlParameter("@par", parcela)); }
        return await ExecuteQuery(sq, list.ToArray());
    }

    [Description("Soma pagamentos filtrando por Região (Estado ou Cidade).")]
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

    [Description("Busca contas a pagar que foram pagas com ATRASO (Data de Pagamento > Data de Vencimento) em um período.")]
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

    [Description("Busca clientes com pagamentos ATRASADOS/(a receber, em aberto) (vencimento < hoje e sem pagamento).")]
    public async Task<string> GetReceberAtrasados()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("Busca boletos/títulos que venceram/(a receber, em aberto) no MÊS ANTERIOR e continuam em aberto.")]
    public async Task<string> GetReceberVencidosNoMesAnterior()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND DATAVENCIMENTO >= DATEADD(month, -1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("Busca a dívida (inadimplência) mais ANTIGA que temos para receber.")]
    public async Task<string> GetMaiorInadimplenciaAntiga()
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE) ORDER BY DATAVENCIMENTO ASC";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("Conta quantos títulos/boletos estão vencendo/(a receber, em aberto) EXATAMENTE HOJE e não foram pagos.")]
    public async Task<string> GetContagemReceberVencidosHoje()
    {
        var sq = "SELECT COUNT(*) as QtdVencidosHoje, SUM(VALORORIG) as ValorTotal FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO = CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("Conta quantos títulos/boletos estão à vencer/(a receber ou em aberto).")]
    public async Task<string> GetContagemReceberAVencer()
    {
        var sq = "SELECT COUNT(*) as QtdVencidosHoje, SUM(VALORORIG) as ValorTotal FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO > CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("Conta quantos títulos/boletos estão vencidos/(a receber ou em aberto).")]
    public async Task<string> GetContagemReceberVencidos()
    {
        var sq = "SELECT COUNT(*) as QtdVencidosHoje, SUM(VALORORIG) as ValorTotal FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("PREVISÃO DE CAIXA: Busca o que está programado para receber em um período futuro (ou hoje).")]
    public async Task<string> GetPrevisaoRecebimentoNoPeriodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Soma o valor total PENDENTE de vencer/(a receber ou em aberto) em um período. USE APENAS se o usuário especificar um período explícito.")]
    public async Task<string> GetSomaReceberPendente(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORORIG) as TotalPendente, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("OPÇÃO PRINCIPAL: Soma o valor TOTAL GERAL de documentos a receber (em aberto) incluindo TODO O FUTURO. Use esta tool OBRIGATORIAMENTE quando pedirem 'valor total a receber', 'total da carteira' ou 'quantidade a receber' sem citar período.")]
    public async Task<string> GetResumoGeralReceberAberto()
    {
        var sq = "SELECT SUM(VALORORIG) as TotalGeralEmAberto, COUNT(*) as QuantidadeTotalTitulos FROM VW_DOC_FIN_REC_ABERTO";
        return await ExecuteQuery(sq, System.Array.Empty<SqlParameter>());
    }

    [Description("Mostra o resumo do total a receber e a quantidade de títulos, AGRUPADO (quebrado) POR ANO de vencimento (inclui anos passados, ano atual e anos futuros).")]
    public async Task<string> GetResumoReceberAbertoAgrupadoPorAno()
    {
        var sq = "SELECT YEAR(DATAVENCIMENTO) as Ano, SUM(VALORORIG) as TotalEmAberto, COUNT(*) as QuantidadeTitulos FROM VW_DOC_FIN_REC_ABERTO GROUP BY YEAR(DATAVENCIMENTO) ORDER BY Ano";
        return await ExecuteQuery(sq, System.Array.Empty<SqlParameter>());
    }

    [Description("Calcula o montante TOTAL da inadimplência (tudo que já venceu e não foi pago) até hoje.")]
    public async Task<string> GetSomaInadimplenciaTotal()
    {
        var sq = "SELECT SUM(VALORORIG) as TotalInadimplencia, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < CAST(GETDATE() AS DATE)";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("Busca o maior boleto que temos para receber em um mês específico.")]
    public async Task<string> GetMaiorReceberAbertoNoMes(
        [Description("Ano (4 digitos)")] int ano, 
        [Description("Mês (1 a 12)")] int mes)
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE YEAR(DATAVENCIMENTO) = @ano AND MONTH(DATAVENCIMENTO) = @mes ORDER BY VALORORIG DESC";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano), new SqlParameter("@mes", mes) });
    }

    [Description("Busca todas as faturas em aberto (pendentes) de um Cliente específico (Limitado a 50 registros).")]
    public async Task<string> GetPendenciasPorCliente([Description("Nome ou Fantasia do Cliente")] string nome)
    {
        var sq = $"SELECT TOP 50 {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE (UPPER(CLIENTE) LIKE UPPER(@n) OR UPPER(NOMEFANTASIA) LIKE UPPER(@n)) ORDER BY DATAVENCIMENTO ASC";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@n", $"%{nome}%") });
    }

    [Description("Busca detalhes de um boleto a receber específico que ainda está EM ABERTO.")]
    public async Task<string> GetDetalhesBoletoReceber(
        [Description("Número do documento")] string numeroDoc,
        [Description("Número da parcela (opcional)")] string parcela)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE DOCUMENTO = @doc";
        var list = new List<SqlParameter> { new SqlParameter("@doc", numeroDoc) };
        if (!string.IsNullOrEmpty(parcela)) { sq += " AND PARCELA = @par"; list.Add(new SqlParameter("@par", parcela)); }
        return await ExecuteQuery(sq, list.ToArray());
    }

    [Description("PREVISÃO POR MÉTODO: Soma o valor esperado de recebimento filtrando por Tipo de Pagamento (PIX, Boleto, etc).")]
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

    [Description("Busca boletos em aberto filtrando pela Condição de Pagamento (Ex: 90 dias).")]
    public async Task<string> GetPendentesPorCondicaoRecebimento([Description("Condição de Pagamento")] string condPag)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE CONDPAG LIKE @cp";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@cp", $"%{condPag}%") });
    }

    [Description("Busca previsões de recebimento filtrando por Filial.")]
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

    [Description("Soma o total a receber de clientes de um Estado (UF) específico.")]
    public async Task<string> GetReceberPendentesPorEstado([Description("Sigla do Estado (Ex: MG)")] string uf)
    {
        var sq = "SELECT SUM(VALORORIG) as TotalEstado, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_ABERTO WHERE UF = @uf";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@uf", uf) });
    }

    [Description("Busca clientes de uma Cidade específica que estão com boletos em aberto.")]
    public async Task<string> GetReceberPendentesPorCidade([Description("Nome da Cidade")] string cidade)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE CIDADE LIKE @c";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@c", $"%{cidade}%") });
    }

    [Description("Busca faturas em aberto pelo CNPJ/CPF exato do cliente.")]
    public async Task<string> GetReceberAbertoPorCNPJ([Description("CNPJ somente números")] string cpfCnpj)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_ABERTO WHERE CPFCNPJ = @cnpj";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@cnpj", LimparCnpj(cpfCnpj)) });
    }

    [Description("Conta a Quantidade Total de recebimentos que vencem/venceram em um Mês e Ano específicos.")]
    public async Task<string> GetContagemReceberAbertoPorMesVencimento(
        [Description("Ano com 4 digitos. Ex: 2026")] string ano, 
        [Description("Mês com 2 digitos. Ex: 02")] string mes)
    {
        var sq = "SELECT COUNT(*) as QuantidadeReceber FROM VW_DOC_FIN_REC_ABERTO WHERE YEAR(DATAVENCIMENTO) = @ano AND MONTH(DATAVENCIMENTO) = @mes";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano), new SqlParameter("@mes", mes) });
    }

    [Description("Conta a Quantidade Total de recebimentos que vencem/venceram em um Ano específicos.")]
    public async Task<string> GetContagemReceberAbertoPorAnoVencimento(
        [Description("Ano com 4 digitos. Ex: 2026")] string ano)
    {
        var sq = "SELECT COUNT(*) as QuantidadeReceber FROM VW_DOC_FIN_REC_ABERTO WHERE YEAR(DATAVENCIMENTO) = @ano ";
        return await ExecuteQuery(sq, new[] { new SqlParameter("@ano", ano)});
    }

    // --- VW_DOC_FIN_REC_PAGO ---

    [Description("Busca faturamento/recebimentos que JÁ FORAM RECEBIDOS em um período. Ex: O que recebemos hoje? Quem pagou ontem?")]
    public async Task<string> GetRecebidosNoPeriodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Calcula o Valor Total Recebido (Soma) em um período.")]
    public async Task<string> GetSomaRecebidoNoPeriodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT SUM(VALORPAG) as TotalRecebido, COUNT(*) as Quantidade FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Analisa a saúde dos recebimentos: Soma de Juros cobrados (pago > original) e Descontos concedidos (pago < original).")]
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

    [Description("Busca o maior pagamento individual recebido de um cliente no período.")]
    public async Task<string> GetMaiorRecebimentoNoPeriodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT ORDER BY VALORPAG DESC";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Busca clientes que pagaram faturas ATRASADAS (data pagamento > vencimento) no período.")]
    public async Task<string> GetRecebimentosAtrasadosLiquidados(
        [Description("Data inicial Pagamento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Pagamento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO > DATAVENCIMENTO AND DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Busca recebimentos que vencião no ANO PASSADO mas que o cliente só pagou ESTE ANO.")]
    public async Task<string> GetRecebidosLancadosAnoPassadoPagosAgora()
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE YEAR(DATAVENCIMENTO) = YEAR(GETDATE()) - 1 AND YEAR(DATAPAGAMENTO) = YEAR(GETDATE())";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    [Description("Identifica qual cliente demorou mais dias para pagar uma fatura (maior diferença entre vencimento e pagamento).")]
    public async Task<string> GetMaiorAtrasoLiquidadoNoPeriodo(
        [Description("Data inicial Pagamento (ISO 8601)")] string dataInicioISO, 
        [Description("Data final Pagamento (ISO 8601)")] string dataFimISO)
    {
        var sq = $"SELECT TOP 1 {BASE_COLUMNS}, DATEDIFF(day, DATAVENCIMENTO, DATAPAGAMENTO) as DiasAtraso FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO > DATAVENCIMENTO AND DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT ORDER BY DiasAtraso DESC";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Busca todos os pagamentos realizados por um Cliente específico em um período.")]
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

    [Description("Traz os Top Clientes que mais geraram receita (Soma VALORPAG) no período.")]
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

    [Description("Soma o quanto entrou no caixa filtrando pelo Meio de Recebimento (PIX, Cartão, etc).")]
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

    [Description("Conta a Quantidade Total de boletos/títulos liquidados/pagos pelos clientes no período.")]
    public async Task<string> GetQuantidadeRecebidosPorPeriodo(
        [Description("Data inicial (ISO 8601)")] string dataInicioISO, 
        [Description("Data final (ISO 8601)")] string dataFimISO)
    {
        var sq = "SELECT COUNT(*) as QuantidadeLiquidados FROM VW_DOC_FIN_REC_PAGO WHERE DATAPAGAMENTO >= @dF AND DATAPAGAMENTO <= @dT";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("Busca recebimentos filtrando pela Condição de Pagamento (Ex: 30/60/90).")]
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

    [Description("Soma o faturamento arrecadado por uma Filial específica.")]
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

    [Description("Soma faturamento filtrando por Localidade (UF ou Cidade).")]
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

    [Description("Busca status de um documento RECEBIDO/PAGO pelo cliente.")]
    public async Task<string> GetStatusDocumentoRecebido(
        [Description("Número do documento")] string numeroDoc,
        [Description("Número da parcela (opcional)")] string parcela)
    {
        var sq = $"SELECT {BASE_COLUMNS} FROM VW_DOC_FIN_REC_PAGO WHERE DOCUMENTO = @doc";
        var list = new List<SqlParameter> { new SqlParameter("@doc", numeroDoc) };
        if (!string.IsNullOrEmpty(parcela)) { sq += " AND PARCELA = @par"; list.Add(new SqlParameter("@par", parcela)); }
        return await ExecuteQuery(sq, list.ToArray());
    }

    [Description("Busca contas RECEBIDAS em um período pelo CNPJ/CPF exato do cliente.")]
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
