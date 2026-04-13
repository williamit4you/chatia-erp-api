using System.ComponentModel;
using Microsoft.SemanticKernel;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;
using System.Text.Json;
using System.Data;
using System.Text;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

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

    [Description("[DOMÍNIO: PAGAR EM ABERTO] Consulta flexível de contas a pagar em aberto. Preencha apenas os parâmetros necessários.")]
    public async Task<string> ConsultarContasPagarAberto(
        [Description("Data inicial Vencimento (ISO 8601). Vazio para ignorar.")] string dataInicioISO = null,
        [Description("Data final Vencimento (ISO 8601). Vazio para ignorar.")] string dataFimISO = null,
        [Description("Nome ou Fantasia do Fornecedor. Vazio para ignorar.")] string fornecedor = null,
        [Description("Sigla do Estado (Ex: SP). Vazio para ignorar.")] string uf = null,
        [Description("Nome da Filial. Vazio para ignorar.")] string filial = null,
        [Description("CNPJ do Fornecedor (somente números). Vazio para ignorar.")] string cnpj = null,
        [Description("Apenas contas com atraso (verdadeiro/falso).")] bool apenasAtrasados = false,
        [Description("Agrupar resultados por: 'NENHUM', 'FORNECEDOR', 'ANO', 'MES' ou 'FILIAL'")] string agrupamento = "NENHUM"
    )
    {
        return await ExecuteDynamicQuery(
            "VW_DOC_FIN_PAG_ABERTO", 
            "DATAVENCIMENTO", 
            dataInicioISO, dataFimISO, fornecedor, uf, filial, cnpj, agrupamento, apenasAtrasados, null, null);
    }

    [Description("[DOMÍNIO: PAGAR PAGO] Consulta flexível de contas já pagas/liquidadas. Preencha apenas os parâmetros necessários.")]
    public async Task<string> ConsultarContasPagarPago(
        [Description("Data inicial do Pagamento (ISO 8601). Vazio para ignorar.")] string dataPagamentoInicioISO = null,
        [Description("Data final do Pagamento (ISO 8601). Vazio para ignorar.")] string dataPagamentoFimISO = null,
        [Description("Nome ou Fantasia do Fornecedor. Vazio para ignorar.")] string fornecedor = null,
        [Description("Sigla do Estado (Ex: SP). Vazio para ignorar.")] string uf = null,
        [Description("Nome da Filial. Vazio para ignorar.")] string filial = null,
        [Description("CNPJ do Fornecedor (somente números). Vazio para ignorar.")] string cnpj = null,
        [Description("Tipo de Pagamento (Ex: PIX, BOLETO). Vazio para ignorar.")] string tipoPagamento = null,
        [Description("Agrupar resultados por: 'NENHUM', 'FORNECEDOR', 'ANO', 'MES', 'FILIAL' ou 'METODO_PAGAMENTO'")] string agrupamento = "NENHUM"
    )
    {
        return await ExecuteDynamicQuery(
            "VW_DOC_FIN_PAG_PAGO", 
            "DATAPAGAMENTO", 
            dataPagamentoInicioISO, dataPagamentoFimISO, fornecedor, uf, filial, cnpj, agrupamento, false, tipoPagamento, null);
    }

    [Description("[DOMÍNIO: RECEBER ABERTO] Consulta flexível de contas a receber em aberto. Preencha apenas os parâmetros necessários.")]
    public async Task<string> ConsultarContasReceberAberto(
        [Description("Data inicial Vencimento (ISO 8601). Vazio para ignorar.")] string dataInicioISO = null,
        [Description("Data final Vencimento (ISO 8601). Vazio para ignorar.")] string dataFimISO = null,
        [Description("Nome ou Fantasia do Cliente. Vazio para ignorar.")] string cliente = null,
        [Description("Sigla do Estado (Ex: SP). Vazio para ignorar.")] string uf = null,
        [Description("Nome da Filial. Vazio para ignorar.")] string filial = null,
        [Description("CNPJ/CPF do Cliente (somente números). Vazio para ignorar.")] string cnpj = null,
        [Description("Apenas contas com atraso (verdadeiro/falso).")] bool apenasAtrasados = false,
        [Description("Agrupar resultados por: 'NENHUM', 'CLIENTE', 'ANO', 'MES' ou 'FILIAL'")] string agrupamento = "NENHUM"
    )
    {
        return await ExecuteDynamicQuery(
            "VW_DOC_FIN_REC_ABERTO", 
            "DATAVENCIMENTO", 
            dataInicioISO, dataFimISO, cliente, uf, filial, cnpj, agrupamento, apenasAtrasados, null, null);
    }

    [Description("[DOMÍNIO: RECEBER PAGO] Consulta flexível de contas já recebidas/liquidadas. Preencha apenas os parâmetros necessários.")]
    public async Task<string> ConsultarContasReceberPago(
        [Description("Data inicial do Pagamento recebido (ISO 8601). Vazio para ignorar.")] string dataPagamentoInicioISO = null,
        [Description("Data final do Pagamento recebido (ISO 8601). Vazio para ignorar.")] string dataPagamentoFimISO = null,
        [Description("Nome ou Fantasia do Cliente. Vazio para ignorar.")] string cliente = null,
        [Description("Sigla do Estado (Ex: SP). Vazio para ignorar.")] string uf = null,
        [Description("Nome da Filial. Vazio para ignorar.")] string filial = null,
        [Description("CNPJ/CPF do Cliente (somente números). Vazio para ignorar.")] string cnpj = null,
        [Description("Tipo de Pagamento/Meio do Recebimento (Ex: PIX, BOLETO). Vazio para ignorar.")] string tipoPagamento = null,
        [Description("Agrupar resultados por: 'NENHUM', 'CLIENTE', 'ANO', 'MES', 'FILIAL' ou 'METODO_PAGAMENTO'")] string agrupamento = "NENHUM"
    )
    {
        return await ExecuteDynamicQuery(
            "VW_DOC_FIN_REC_PAGO", 
            "DATAPAGAMENTO", 
            dataPagamentoInicioISO, dataPagamentoFimISO, cliente, uf, filial, cnpj, agrupamento, false, tipoPagamento, null);
    }

    [Description("[DOMÍNIO: AMBOS] Simula o fluxo de caixa cruzando Receitas e Despesas agrupadas pela Data.")]
    public async Task<string> GetFluxoCaixaLiquidoNoPeriodo(
        [Description("Data inicial (ISO 8601). Feriados/FDS não são filtrados automaticamente.")] string dataInicioISO,
        [Description("Data final (ISO 8601).")] string dataFimISO)
    {
        var sq = $@"
            SELECT 
                ISNULL(R.Dia, P.Dia) as DataFluxo,
                ISNULL(R.TotalReceitas, 0) as ReceitasDia,
                ISNULL(P.TotalDespesas, 0) as DespesasDia,
                (ISNULL(R.TotalReceitas, 0) - ISNULL(P.TotalDespesas, 0)) as SaldoLiquidoDia
            FROM (
                SELECT CAST(DATAVENCIMENTO AS DATE) as Dia, SUM(VALORORIG - ISNULL(VALORPAG, 0)) as TotalReceitas
                FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT
                GROUP BY CAST(DATAVENCIMENTO AS DATE)
            ) R
            FULL OUTER JOIN (
                SELECT CAST(DATAVENCIMENTO AS DATE) as Dia, SUM(VALORORIG) as TotalDespesas
                FROM VW_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT
                GROUP BY CAST(DATAVENCIMENTO AS DATE)
            ) P ON R.Dia = P.Dia
            ORDER BY DataFluxo ASC";
        return await ExecuteQuery(sq, new[] { 
            new SqlParameter("@dF", ParseDate(dataInicioISO)), 
            new SqlParameter("@dT", ParseDate(dataFimISO)) });
    }

    [Description("[DOMÍNIO: AMBOS] Relatório global instantâneo de saúde financeira e liquidez dividida por Filial/Empresa.")]
    public async Task<string> GetLiquidezEInadimplenciaGeral()
    {
        var sq = @"
            SELECT 
                ISNULL(R.EMPRESA, P.EMPRESA) as Filial,
                ISNULL(R.TotalReceber, 0) as TotalPendenteRecebimentos,
                ISNULL(R.TotalAtrasadoCliente, 0) as TotalAtrasadoCliente,
                ISNULL(P.TotalPagar, 0) as TotalPendenteObrigacoes,
                ISNULL(P.TotalAtrasadoFornecedor, 0) as TotalAtrasadoFornecedor,
                (ISNULL(R.TotalReceber, 0) - ISNULL(P.TotalPagar, 0)) as LiquidezFutura
            FROM (
                SELECT EMPRESA, 
                       SUM(VALORORIG) as TotalReceber,
                       SUM(CASE WHEN DATAVENCIMENTO < CAST(GETDATE() AS DATE) THEN VALORORIG ELSE 0 END) as TotalAtrasadoCliente
                FROM VW_DOC_FIN_REC_ABERTO GROUP BY EMPRESA
            ) R
            FULL OUTER JOIN (
                SELECT EMPRESA, 
                       SUM(VALORORIG) as TotalPagar,
                       SUM(CASE WHEN DATAVENCIMENTO < CAST(GETDATE() AS DATE) THEN VALORORIG ELSE 0 END) as TotalAtrasadoFornecedor
                FROM VW_DOC_FIN_PAG_ABERTO GROUP BY EMPRESA
            ) P ON R.EMPRESA = P.EMPRESA";
        return await ExecuteQuery(sq, Array.Empty<SqlParameter>());
    }

    // --- MÉTODOS BASE ---

    private async Task<string> ExecuteDynamicQuery(
        string viewName, 
        string dateColumn, 
        string dataInicioISO, 
        string dataFimISO, 
        string entidade, 
        string uf, 
        string filial, 
        string cnpj, 
        string agrupamento, 
        bool apenasAtrasados,
        string tipoPagamento,
        string situacao)
    {
        var sql = new StringBuilder();
        var where = new StringBuilder(" WHERE 1=1");
        var parameters = new List<SqlParameter>();

        if (!string.IsNullOrEmpty(dataInicioISO)) { where.Append($" AND {dateColumn} >= @dI"); parameters.Add(new SqlParameter("@dI", ParseDate(dataInicioISO))); }
        if (!string.IsNullOrEmpty(dataFimISO)) { where.Append($" AND {dateColumn} <= @dF"); parameters.Add(new SqlParameter("@dF", ParseDate(dataFimISO))); }
        if (!string.IsNullOrEmpty(entidade)) { where.Append(" AND (UPPER(CLIENTE) LIKE UPPER(@ent) OR UPPER(NOMEFANTASIA) LIKE UPPER(@ent))"); parameters.Add(new SqlParameter("@ent", $"%{entidade}%")); }
        if (!string.IsNullOrEmpty(uf)) { where.Append(" AND UF = @uf"); parameters.Add(new SqlParameter("@uf", uf.ToUpper())); }
        if (!string.IsNullOrEmpty(filial)) { where.Append(" AND UPPER(EMPRESA) LIKE UPPER(@fil)"); parameters.Add(new SqlParameter("@fil", $"%{filial}%")); }
        if (!string.IsNullOrEmpty(cnpj)) { where.Append(" AND CPFCNPJ = @cnpj"); parameters.Add(new SqlParameter("@cnpj", LimparCnpj(cnpj))); }
        if (apenasAtrasados && dateColumn == "DATAVENCIMENTO") { where.Append(" AND DATAVENCIMENTO < CAST(GETDATE() AS DATE)"); }
        if (!string.IsNullOrEmpty(tipoPagamento)) { where.Append(" AND UPPER(TIPOPAG) LIKE UPPER(@tpag)"); parameters.Add(new SqlParameter("@tpag", $"%{tipoPagamento}%")); }

        string sumColumn = viewName.Contains("PAGO") ? "VALORPAG" : "VALORORIG";

        if (agrupamento.Equals("FORNECEDOR", StringComparison.OrdinalIgnoreCase) || agrupamento.Equals("CLIENTE", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT CLIENTE as Entidade, SUM({sumColumn}) as Total, COUNT(*) as Quantidade FROM {viewName} {where} GROUP BY CLIENTE ORDER BY Total DESC");
        else if (agrupamento.Equals("ANO", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT YEAR({dateColumn}) as Ano, SUM({sumColumn}) as Total, COUNT(*) as Quantidade FROM {viewName} {where} GROUP BY YEAR({dateColumn}) ORDER BY Ano DESC");
        else if (agrupamento.Equals("MES", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT YEAR({dateColumn}) as Ano, MONTH({dateColumn}) as Mes, SUM({sumColumn}) as Total, COUNT(*) as Quantidade FROM {viewName} {where} GROUP BY YEAR({dateColumn}), MONTH({dateColumn}) ORDER BY Ano DESC, Mes DESC");
        else if (agrupamento.Equals("FILIAL", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT EMPRESA as Filial, SUM({sumColumn}) as Total, COUNT(*) as Quantidade FROM {viewName} {where} GROUP BY EMPRESA ORDER BY Total DESC");
        else if (agrupamento.Equals("METODO_PAGAMENTO", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT TIPOPAG as MetodoPagamento, SUM({sumColumn}) as Total, COUNT(*) as Quantidade FROM {viewName} {where} GROUP BY TIPOPAG ORDER BY Total DESC");
        else
            sql.Append($"SELECT {BASE_COLUMNS} FROM {viewName} {where} ORDER BY {dateColumn} ASC");

        return await ExecuteQuery(sql.ToString(), parameters.ToArray());
    }

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
