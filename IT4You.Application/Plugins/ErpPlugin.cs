using System.ComponentModel;
using Microsoft.SemanticKernel;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Data.SqlClient;
using System.Text.Json;
using System.Data;
using System.Text;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClosedXML.Excel;
using QuestPDF.Fluent;
using QuestPDF.Helpers;
using QuestPDF.Infrastructure;

namespace IT4You.Application.Plugins;

public class ErpPlugin
{
    private readonly IConfiguration _configuration;
    private readonly string _connectionString;
    private readonly IMemoryCache _cache;

    // A partir deste número o sistema gera Excel direto ao usuário (bypass da IA)
    private const int EXPORT_THRESHOLD = 10;

    // SQL query tracking
    public List<string> ExecutedQueries { get; } = new();
    public void ClearExecutedQueries() => ExecutedQueries.Clear();

    // Export metadata — preenchido por ExecuteExportToCache, lido pelo ChatService
    public string? LastExportId { get; private set; }
    public int LastExportTotalLinhas { get; private set; }
    public decimal LastExportValorTotal { get; private set; }
    public void ClearExportMetadata()
    {
        LastExportId = null;
        LastExportTotalLinhas = 0;
        LastExportValorTotal = 0;
    }

    public string? GetExecutedQueriesJson()
    {
        if (ExecutedQueries.Count == 0) return null;
        var options = new JsonSerializerOptions 
        { 
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            WriteIndented = true
        };
        return JsonSerializer.Serialize(ExecutedQueries);
    }

    public ErpPlugin(IConfiguration configuration, IMemoryCache cache)
    {
        _configuration = configuration;
        _cache = cache;
        _connectionString = _configuration.GetConnectionString("DefaultConnection") ?? "";
    }

    // Omitimos a coluna CLIENTE/FORNECEDOR do BASE_COLUMNS fixo pois ela varia por View
    private const string BASE_COLUMNS_PARTIAL = "EMPRESA, NOMEFANTASIA, CPFCNPJ, CIDADE, UF, DOCUMENTO, EMISSAO, VALORDOC, PARCELA, VALORORIG, VALORPAG, DATAVENCIMENTO, DATAPAGAMENTO, CONDPAG, TIPOPAG, SITUACAO";

    [Description("[DOMÍNIO: ABERTO] Consulta flexível de contas EM ABERTO (vencidas ou a vencer).")]
    public async Task<string> ConsultarContasEmAberto(
        [Description("OBRIGATÓRIO: 'PAGAR', 'RECEBER' ou 'INDEFINIDO'. REGRA: NUNCA presuma se um nome (ex: Minerva) é Cliente ou Fornecedor. No nosso sistema, qualquer pessoa pode ser ambos. Se o usuário não disser explicitamente o lado, PASSE O VALOR 'INDEFINIDO'.")] string tipoDominio = "INDEFINIDO",
        [Description("Data inicial Vencimento (ISO 8601). Vazio para ignorar.")] string dataInicioISO = "",
        [Description("Data final Vencimento (ISO 8601). Vazio para ignorar.")] string dataFimISO = "",
        [Description("Nome ou Fantasia do Fornecedor ou Cliente. Vazio para ignorar.")] string nomePessoa = "",
        [Description("Sigla do Estado (Ex: SP). Vazio para ignorar.")] string uf = "",
        [Description("Nome da Filial. Vazio para ignorar.")] string filial = "",
        [Description("CNPJ ou CPF (somente números). Vazio para ignorar.")] string cnpj = "",
        [Description("Apenas contas com atraso (verdadeiro/falso).")] bool apenasAtrasados = false,
        [Description("Número do documento específico. Use para localizar UM documento exato.")] string numeroDocumento = "",
        [Description("Agrupar resultados por: 'NENHUM', 'FORNECEDOR', 'CLIENTE', 'ANO', 'MES', 'FILIAL', 'METODO_PAGAMENTO', 'TOTAL' ou 'SITUACAO_VENCIMENTO'. USE 'SITUACAO_VENCIMENTO' quando o usuário perguntar sobre vencidos e a vencer ao mesmo tempo (ex: 'quantos vencidos e a vencer?'). REGRA DE OURO: Se o usuário pedir para agrupar/dividir/quebrar por 'empresa', NÃO EXECUTE A FERRAMENTA. Pergunte primeiro se ele quer por Filial (nossa empresa) ou por Cliente/Fornecedor.")] string agrupamento = "NENHUM",
        [Description("Limite máximo de registros para retornar. Use apenas se o usuário pedir 'Top X' ou 'X documentos'.")] int limite = 0,
        [Description("Se verdadeiro, ordena os resultados pelos maiores valores financeiros.")] bool ordenarPorMaiorValor = false
        )
    {
        // 🚨 TRAVA DE SEGURANÇA: Se a IA não souber, ela cai aqui e devolve a pergunta pro chat
        if (tipoDominio.Equals("INDEFINIDO", StringComparison.OrdinalIgnoreCase))
        {
            return "SISTEMA: Pare a execução. Pergunte ao usuário se ele deseja consultar o Contas a Pagar (fornecedor) ou o Contas a Receber (cliente).";
        }

        string viewName = tipoDominio.Equals("PAGAR", StringComparison.OrdinalIgnoreCase) 
            ? "VW_SWIA_DOC_FIN_PAG_ABERTO"
            : "VW_SWIA_DOC_FIN_REC_ABERTO";

        return await ExecuteDynamicQuery(
            viewName, 
            "DATAVENCIMENTO", 
            dataInicioISO, dataFimISO, nomePessoa, uf, filial, cnpj, agrupamento, apenasAtrasados, null, null, numeroDocumento, limite, ordenarPorMaiorValor);
    }

    [Description("[DOMÍNIO: PAGO] Consulta flexível de contas JÁ PAGAS/LIQUIDADAS.")]
    public async Task<string> ConsultarContasPagas(
        [Description("OBRIGATÓRIO: 'PAGAR', 'RECEBER' ou 'INDEFINIDO'. REGRA: NUNCA presuma se um nome (ex: Minerva) é Cliente ou Fornecedor. No nosso sistema, qualquer pessoa pode ser ambos. Se o usuário não disser explicitamente o lado, PASSE O VALOR 'INDEFINIDO'.")] string tipoDominio = "INDEFINIDO",
        [Description("Data inicial do Pagamento (ISO 8601). Vazio para ignorar.")] string dataPagamentoInicioISO = "",
        [Description("Data final do Pagamento (ISO 8601). Vazio para ignorar.")] string dataPagamentoFimISO = "",
        [Description("Nome ou Fantasia do Fornecedor ou Cliente. Vazio para ignorar.")] string nomePessoa = "",
        [Description("Sigla do Estado (Ex: SP). Vazio para ignorar.")] string uf = "",
        [Description("Nome da Filial. Vazio para ignorar.")] string filial = "",
        [Description("CNPJ ou CPF (somente números). Vazio para ignorar.")] string cnpj = "",
        [Description("Número do documento específico. Use para localizar UM documento exato.")] string numeroDocumento = "",
        [Description("Tipo de Pagamento/Meio (Ex: PIX, BOLETO). Vazio para ignorar.")] string tipoPagamento = "",
        [Description("Agrupar resultados por: 'NENHUM', 'FORNECEDOR', 'CLIENTE', 'ANO', 'MES', 'FILIAL', 'METODO_PAGAMENTO', 'TOTAL' ou 'SITUACAO_VENCIMENTO'. USE 'SITUACAO_VENCIMENTO' quando o usuário perguntar sobre vencidos e a vencer ao mesmo tempo. REGRA DE OURO: Se o usuário pedir para agrupar/dividir/quebrar por 'empresa', NÃO EXECUTE A FERRAMENTA. Pergunte primeiro se ele quer por Filial (nossa empresa) ou por Cliente/Fornecedor.")] string agrupamento = "NENHUM",
        [Description("Limite máximo de registros para retornar. Use apenas se o usuário pedir 'Top X' ou 'X documentos'.")] int limite = 0,
        [Description("Se verdadeiro, ordena os resultados pelos maiores valores financeiros.")] bool ordenarPorMaiorValor = false
        )
    {
        // 🚨 TRAVA DE SEGURANÇA: Se a IA não souber, ela cai aqui e devolve a pergunta pro chat
        if (tipoDominio.Equals("INDEFINIDO", StringComparison.OrdinalIgnoreCase))
        {
            return "SISTEMA: Pare a execução. Pergunte ao usuário se ele deseja consultar o Contas a Pagar (fornecedor) ou o Contas a Receber (cliente).";
        }

        string viewName = tipoDominio.Equals("PAGAR", StringComparison.OrdinalIgnoreCase) 
            ? "VW_SWIA_DOC_FIN_PAG_PAGO"
            : "VW_SWIA_DOC_FIN_REC_PAGO";

        return await ExecuteDynamicQuery(
            viewName, 
            "DATAPAGAMENTO", 
            dataPagamentoInicioISO, dataPagamentoFimISO, nomePessoa, uf, filial, cnpj, agrupamento, false, tipoPagamento, null, numeroDocumento, limite, ordenarPorMaiorValor);
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
                    FROM VW_SWIA_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT
                    GROUP BY CAST(DATAVENCIMENTO AS DATE)
                ) R
                FULL OUTER JOIN (
                    SELECT CAST(DATAVENCIMENTO AS DATE) as Dia, SUM(VALORORIG) as TotalDespesas
                    FROM VW_SWIA_DOC_FIN_PAG_ABERTO WHERE DATAVENCIMENTO >= @dF AND DATAVENCIMENTO <= @dT
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
                    FROM VW_SWIA_DOC_FIN_REC_ABERTO GROUP BY EMPRESA
                ) R
                FULL OUTER JOIN (
                    SELECT EMPRESA, 
                        SUM(VALORORIG) as TotalPagar,
                        SUM(CASE WHEN DATAVENCIMENTO < CAST(GETDATE() AS DATE) THEN VALORORIG ELSE 0 END) as TotalAtrasadoFornecedor
                    FROM VW_SWIA_DOC_FIN_PAG_ABERTO GROUP BY EMPRESA
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
        string situacao,
        string numeroDocumento = "",
        int limite = 0,
        bool ordenarPorMaiorValor = false)
    {
        var sql = new StringBuilder();
        var conditions = new List<string>(); // Usar lista remove a necessidade do "WHERE 1=1"
        var parameters = new List<SqlParameter>();

        if (!string.IsNullOrEmpty(dataInicioISO))
        {
            conditions.Add($"{dateColumn} >= @dI");
            parameters.Add(new SqlParameter("@dI", ParseDate(dataInicioISO)));
        }
        if (!string.IsNullOrEmpty(dataFimISO))
        {
            conditions.Add($"{dateColumn} <= @dF");
            parameters.Add(new SqlParameter("@dF", ParseDate(dataFimISO)));
        }
        // Identifica o nome da coluna de entidade baseado na View (FORNECEDOR para PAGAR, CLIENTE para RECEBER)
        string personCol = viewName.Contains("_PAG_") ? "FORNECEDOR" : "CLIENTE";

        if (!string.IsNullOrEmpty(entidade))
        {
            conditions.Add($"(UPPER({personCol}) LIKE UPPER(@ent) OR UPPER(NOMEFANTASIA) LIKE UPPER(@ent))");
            parameters.Add(new SqlParameter("@ent", $"%{entidade}%"));
        }
        if (!string.IsNullOrEmpty(uf))
        {
            conditions.Add("UF = @uf");
            parameters.Add(new SqlParameter("@uf", uf.ToUpper()));
        }
        if (!string.IsNullOrEmpty(filial))
        {
            conditions.Add("UPPER(EMPRESA) LIKE UPPER(@fil)");
            parameters.Add(new SqlParameter("@fil", $"%{filial}%"));
        }
        if (!string.IsNullOrEmpty(cnpj))
        {
            conditions.Add("CPFCNPJ = @cnpj");
            parameters.Add(new SqlParameter("@cnpj", LimparCnpj(cnpj)));
        }
        if (apenasAtrasados && dateColumn == "DATAVENCIMENTO")
        {
            conditions.Add("DATAVENCIMENTO < CAST(GETDATE() AS DATE)");
        }
        if (!string.IsNullOrEmpty(tipoPagamento))
        {
            conditions.Add("UPPER(TIPOPAG) LIKE UPPER(@tpag)");
            parameters.Add(new SqlParameter("@tpag", $"%{tipoPagamento}%"));
        }
        if (!string.IsNullOrEmpty(numeroDocumento))
        {
            conditions.Add("DOCUMENTO LIKE @doc");
            parameters.Add(new SqlParameter("@doc", $"%{numeroDocumento}%"));
        }

        // Monta o WHERE apenas se tiver condições, limpíssimo
        string whereClause = conditions.Count > 0 ? " WHERE " + string.Join(" AND ", conditions) : "";
        string sumColumn = viewName.Contains("PAGO") ? "VALORPAG" : "VALORORIG";
        string fullBaseColumns = $"{personCol}, {BASE_COLUMNS_PARTIAL}";

        // 🚨 TRAVA DE SEGURANÇA: Previne erro fatal se a IA omitir a propriedade e passar NULL
        string agrupar = string.IsNullOrEmpty(agrupamento) ? "NENHUM" : agrupamento;

        if (agrupar.Equals("FORNECEDOR", StringComparison.OrdinalIgnoreCase) || agrupar.Equals("CLIENTE", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT {personCol} as Entidade, SUM({sumColumn}) as Total, COUNT(*) as Quantidade FROM {viewName}{whereClause} GROUP BY {personCol} ORDER BY Total DESC");
        else if (agrupar.Equals("ANO", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT YEAR({dateColumn}) as Ano, SUM({sumColumn}) as Total, COUNT(*) as Quantidade FROM {viewName}{whereClause} GROUP BY YEAR({dateColumn}) ORDER BY Ano DESC");
        else if (agrupar.Equals("MES", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT YEAR({dateColumn}) as Ano, MONTH({dateColumn}) as Mes, SUM({sumColumn}) as Total, COUNT(*) as Quantidade FROM {viewName}{whereClause} GROUP BY YEAR({dateColumn}), MONTH({dateColumn}) ORDER BY Ano DESC, Mes DESC");
        else if (agrupar.Equals("FILIAL", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT EMPRESA as Filial, SUM({sumColumn}) as Total, COUNT(*) as Quantidade FROM {viewName}{whereClause} GROUP BY EMPRESA ORDER BY Total DESC");
        else if (agrupar.Equals("METODO_PAGAMENTO", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT TIPOPAG as MetodoPagamento, SUM({sumColumn}) as Total, COUNT(*) as Quantidade FROM {viewName}{whereClause} GROUP BY TIPOPAG ORDER BY Total DESC");
        else if (agrupar.Equals("TOTAL", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT SUM({sumColumn}) as ValorTotalGeral, COUNT(*) as QuantidadeTotal FROM {viewName}{whereClause}");
        else if (agrupar.Equals("SITUACAO_VENCIMENTO", StringComparison.OrdinalIgnoreCase))
            // Uma única query que divide vencidos vs a vencer — evita que a IA faça aritmética errada
            sql.Append($@"SELECT
                CASE WHEN DATAVENCIMENTO < CAST(GETDATE() AS DATE) THEN 'Vencido' ELSE 'A Vencer' END as SituacaoVencimento,
                COUNT(*) as Quantidade,
                SUM({sumColumn}) as ValorTotal
            FROM {viewName}{whereClause}
            GROUP BY CASE WHEN DATAVENCIMENTO < CAST(GETDATE() AS DATE) THEN 'Vencido' ELSE 'A Vencer' END
            ORDER BY SituacaoVencimento");
        else
        {
            // Passo 1: conta e soma primeiro (barato para o SQL Server)
            var countSql = $"SELECT COUNT(*) AS Quantidade, SUM({sumColumn}) AS ValorTotal FROM {viewName}{whereClause}";
            var (totalReal, valorTotal) = await ExecuteCountAndSum(countSql, parameters.ToArray());

            // Define o critério de ordenação
            string sortCol = ordenarPorMaiorValor ? sumColumn : dateColumn;
            string sortOrder = ordenarPorMaiorValor ? "DESC" : "ASC";
            string orderByClause = $" ORDER BY {sortCol} {sortOrder}";

            // Se a IA pediu um limite específico (ex: Top 10), usamos esse limite para decidir a exibição
            int thresholdParaUso = (limite > 0 && limite <= EXPORT_THRESHOLD) ? limite : totalReal;

            if (thresholdParaUso <= EXPORT_THRESHOLD)
            {
                // Pequeno (ou pedido limitadamente): envia inline para a IA (tabela no chat)
                int topN = limite > 0 ? limite : EXPORT_THRESHOLD;
                var listSql = $"SELECT TOP {topN} {fullBaseColumns} FROM {viewName}{whereClause}{orderByClause}";
                return await ExecuteListQueryInline(listSql, countSql, parameters.ToArray());
            }
            else
            {
                // Grande: gera Excel, salva em cache, retorna só metadados para a IA
                var fullSql = $"SELECT {fullBaseColumns} FROM {viewName}{whereClause}{orderByClause}";
                return await ExecuteExportToCache(fullSql, totalReal, valorTotal, parameters.ToArray(), viewName, dateColumn);
            }
        }

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

    private string BuildRunnableQuery(string queryText, SqlParameter[] parameters)
    {
        var finalQuery = queryText;
        foreach (var p in parameters.OrderByDescending(p => p.ParameterName.Length))
        {
            // Garante que o C# sempre procure pelo parâmetro com "@", mesmo que a classe se perca internamente
            string pName = p.ParameterName.StartsWith("@") ? p.ParameterName : "@" + p.ParameterName;
            
            var val = p.Value == null || p.Value == DBNull.Value ? "NULL" : $"'{p.Value.ToString().Replace("'", "''")}'";
            finalQuery = finalQuery.Replace(pName, val);
        }
        return finalQuery;
    }

    private async Task<string> ExecuteQuery(string queryText, SqlParameter[] parameters)
    {
        if (string.IsNullOrEmpty(_connectionString))
            return "{\"error\": \"Connection string 'DefaultConnection' not found.\"}";

        try
        {
            string runnableQuery = BuildRunnableQuery(queryText, parameters);
            Console.WriteLine($"[ErpPlugin] 🟢 EXECUTING EXACT QUERY: {runnableQuery}");

            // Track query for SQL transparency feature
            ExecutedQueries.Add(runnableQuery);

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

            var payload = new {
                TotalEncontradoNestaBusca = results.Count,
                AlertaQuantidade = "Ok",
                Data = results
            };

            var jsonResult = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
            Console.WriteLine($"[ErpPlugin] 🟢 QUERY SUCCESS. Returned {results.Count} rows.");
            Console.WriteLine($"[ErpPlugin] 🟢 QUERY RESULTS:");
            Console.WriteLine(jsonResult);
            return jsonResult;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ErpPlugin] 🔴 FATAL QUERY ERROR: {ex.Message}");
            return $"{{\"error\": \"Database error: {ex.Message}\"}}";
        }
    }

    // ---------- HELPERS DE LISTAGEM ----------

    /// <summary>Executa COUNT + SUM em uma única query leve.</summary>
    private async Task<(int total, decimal valorTotal)> ExecuteCountAndSum(string countSql, SqlParameter[] parameters)
    {
        if (string.IsNullOrEmpty(_connectionString)) return (0, 0);
        try
        {
            string runnable = BuildRunnableQuery(countSql, parameters);
            ExecutedQueries.Add(runnable);
            Console.WriteLine($"[ErpPlugin] 🟢 COUNT+SUM: {runnable}");

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var cmd = new SqlCommand(countSql, connection);
            foreach (var p in parameters)
                cmd.Parameters.Add(new SqlParameter(p.ParameterName, p.Value));

            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                int total = reader["Quantidade"] == DBNull.Value ? 0 : Convert.ToInt32(reader["Quantidade"]);
                decimal valor = reader["ValorTotal"] == DBNull.Value ? 0 : Convert.ToDecimal(reader["ValorTotal"]);
                return (total, valor);
            }
            return (0, 0);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ErpPlugin] 🔴 COUNT ERRO: {ex.Message}");
            return (0, 0);
        }
    }

    /// <summary>Listagem inline para conjuntos pequenos (≤ EXPORT_THRESHOLD). A IA recebe as linhas + totais.</summary>
    private async Task<string> ExecuteListQueryInline(string listQuery, string countQuery, SqlParameter[] parameters)
    {
        if (string.IsNullOrEmpty(_connectionString))
            return "{\"error\": \"Connection string not found.\"}";
        try
        {
            string runnableList = BuildRunnableQuery(listQuery, parameters);
            Console.WriteLine($"[ErpPlugin] 🟢 LIST INLINE: {runnableList}");
            ExecutedQueries.Add(runnableList);

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            // Re-executa COUNT+SUM (já foi feito antes, mas precisamos dos números aqui)
            int totalReal = 0; decimal valorTotal = 0;
            using (var countCmd = new SqlCommand(countQuery, connection))
            {
                foreach (var p in parameters)
                    countCmd.Parameters.Add(new SqlParameter(p.ParameterName, p.Value));
                using var cr = await countCmd.ExecuteReaderAsync();
                if (await cr.ReadAsync())
                {
                    totalReal = cr["Quantidade"] == DBNull.Value ? 0 : Convert.ToInt32(cr["Quantidade"]);
                    valorTotal = cr["ValorTotal"] == DBNull.Value ? 0 : Convert.ToDecimal(cr["ValorTotal"]);
                }
            }

            var results = new List<Dictionary<string, object>>();
            using (var listCmd = new SqlCommand(listQuery, connection))
            {
                foreach (var p in parameters)
                    listCmd.Parameters.Add(new SqlParameter(p.ParameterName, p.Value));
                using var reader = await listCmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        row[reader.GetName(i)] = reader.GetValue(i) == DBNull.Value ? null! : reader.GetValue(i);
                    results.Add(row);
                }
            }

            var payload = new
            {
                TotalDeDocumentosNoBanco = totalReal,
                ValorTotalConfirmado = valorTotal,
                ExibindoPrimeirosDocumentos = results.Count,
                AlertaParaIA = $"LISTAGEM COMPLETA: Todos os {totalReal} documentos estão exibidos. " +
                               $"Total confirmado: R$ {valorTotal:F2}. " +
                               $"EXIBA este valor como rodapé da tabela: **Total: R$ {valorTotal:N2} ({totalReal} documentos)**",
                Data = results
            };

            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
            Console.WriteLine($"[ErpPlugin] 🟢 INLINE OK. {results.Count} rows. Total R$ {valorTotal:F2}");
            return json;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ErpPlugin] 🔴 INLINE ERRO: {ex.Message}");
            return $"{{\"error\": \"{ex.Message}\"}}";
        }
    }

    /// <summary>Gera Excel com TODOS os registros, salva no IMemoryCache (TTL 30 min) e retorna apenas metadados para a IA.</summary>
    private async Task<string> ExecuteExportToCache(string fullQuery, int totalReal, decimal valorTotal,
        SqlParameter[] parameters, string viewName, string dateColumn)
    {
        if (string.IsNullOrEmpty(_connectionString))
            return "{\"error\": \"Connection string not found.\"}";
        try
        {
            string runnable = BuildRunnableQuery(fullQuery, parameters);
            Console.WriteLine($"[ErpPlugin] 🟢 EXPORT FULL QUERY ({totalReal} rows): {runnable}");
            ExecutedQueries.Add(runnable);

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            var results = new List<Dictionary<string, object>>();
            using (var cmd = new SqlCommand(fullQuery, connection))
            {
                foreach (var p in parameters)
                    cmd.Parameters.Add(new SqlParameter(p.ParameterName, p.Value));
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        row[reader.GetName(i)] = reader.GetValue(i) == DBNull.Value ? null! : reader.GetValue(i);
                    results.Add(row);
                }
            }

            // Gera Excel em memória com ClosedXML
            var excelBytes = GerarExcel(results, viewName, totalReal, valorTotal);

            // Salva no cache com TTL de 30 minutos
            var exportId = Guid.NewGuid().ToString();
            var cacheKey = $"export:{exportId}";
            _cache.Set(cacheKey, excelBytes, TimeSpan.FromMinutes(30));

            // Expõe metadados para o ChatService ler diretamente (não depende do texto da IA)
            LastExportId = exportId;
            LastExportTotalLinhas = totalReal;
            LastExportValorTotal = valorTotal;

            // Salva dados brutos (JSON) para geração de PDF on-demand — mesma TTL de 30 min
            var rawDataJson = JsonSerializer.Serialize(results);
            _cache.Set($"export-data:{exportId}", rawDataJson, TimeSpan.FromMinutes(30));

            Console.WriteLine($"[ErpPlugin] 🟢 EXPORT CACHED. Id={exportId}, Rows={totalReal}, Size={excelBytes.Length} bytes");

            // Retorna só os metadados para a IA — ela não deve mencionar links nem URLs
            return JsonSerializer.Serialize(new
            {
                tipo = "EXPORT_PRONTO",
                exportId = exportId,
                totalLinhas = totalReal,
                valorTotalConfirmado = valorTotal,
                instrucaoParaIA =
                    $"Relatório gerado com sucesso: {totalReal} documentos, valor total R$ {valorTotal:N2}. " +
                    $"Informe ao usuário de forma objetiva quantos documentos foram encontrados e o valor total. " +
                    $"NÃO mencione links, URLs, exportId nem instruções de download — os botões de download (Excel e PDF) são exibidos automaticamente pela interface abaixo desta mensagem."
            }, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ErpPlugin] 🔴 EXPORT ERRO: {ex.Message}");
            return $"{{\"error\": \"{ex.Message}\"}}";
        }
    }

    /// <summary>Gera arquivo PDF em memória usando QuestPDF.</summary>
    public static byte[] GerarPdf(List<Dictionary<string, object>> rows, int total, decimal valorTotal)
    {
        QuestPDF.Settings.License = LicenseType.Community;

        return Document.Create(container =>
        {
            container.Page(page =>
            {
                page.Margin(30);
                page.Size(PageSizes.A4.Landscape());
                page.DefaultTextStyle(x => x.FontSize(8).FontFamily("Arial"));

                page.Header().Padding(5).Row(row =>
                {
                    row.RelativeItem().Text(text =>
                    {
                        text.Span("Relatório Financeiro").Bold().FontSize(12);
                        text.Span($"  —  Gerado em {DateTime.Now:dd/MM/yyyy HH:mm}").FontSize(9).FontColor(Colors.Grey.Medium);
                    });
                    row.ConstantItem(180).AlignRight().Text(text =>
                    {
                        text.Span($"{total} registros  |  ").FontSize(9).FontColor(Colors.Grey.Medium);
                        text.Span($"R$ {valorTotal:N2}").Bold().FontSize(9).FontColor(Colors.Green.Darken2);
                    });
                });

                page.Content().PaddingVertical(8).Table(table =>
                {
                    if (rows.Count == 0) return;

                    var columns = rows[0].Keys.ToList();

                    // Columas dinâmicas com tamanho relativo igual
                    table.ColumnsDefinition(cols =>
                    {
                        foreach (var _ in columns)
                            cols.RelativeColumn();
                    });

                    // Cabeçalho
                    table.Header(header =>
                    {
                        foreach (var col in columns)
                        {
                            header.Cell().Background(Colors.BlueGrey.Darken3).Padding(4)
                                .Text(col).Bold().FontColor(Colors.White).FontSize(7.5f);
                        }
                    });

                    // Linhas de dados com zebra stripe
                    for (int r = 0; r < rows.Count; r++)
                    {
                        var bgColor = r % 2 == 0 ? Colors.White : Colors.Grey.Lighten4;
                        foreach (var col in columns)
                        {
                            var val = rows[r].GetValueOrDefault(col);
                            string text = val switch
                            {
                                DateTime dt => dt.ToString("dd/MM/yyyy"),
                                decimal dec => dec.ToString("N2"),
                                double dbl  => dbl.ToString("N2"),
                                null        => "",
                                _           => val.ToString() ?? ""
                            };
                            table.Cell().Background(bgColor).Padding(3).Text(text).FontSize(7.5f);
                        }
                    }
                });

                page.Footer().AlignCenter().Text(text =>
                {
                    text.Span("Página ").FontSize(8).FontColor(Colors.Grey.Medium);
                    text.CurrentPageNumber().FontSize(8).FontColor(Colors.Grey.Medium);
                    text.Span(" de ").FontSize(8).FontColor(Colors.Grey.Medium);
                    text.TotalPages().FontSize(8).FontColor(Colors.Grey.Medium);
                });
            });
        }).GeneratePdf();
    }

    /// <summary>Gera arquivo Excel (.xlsx) em memória usando ClosedXML.</summary>
    private static byte[] GerarExcel(List<Dictionary<string, object>> rows, string viewName, int total, decimal valorTotal)
    {
        using var workbook = new XLWorkbook();
        var ws = workbook.Worksheets.Add("Relatório");

        if (rows.Count == 0)
        {
            ws.Cell(1, 1).Value = "Nenhum registro encontrado.";
        }
        else
        {
            // Cabeçalho
            var columns = rows[0].Keys.ToList();
            for (int col = 0; col < columns.Count; col++)
            {
                var headerCell = ws.Cell(1, col + 1);
                headerCell.Value = columns[col];
                headerCell.Style.Font.Bold = true;
                headerCell.Style.Fill.BackgroundColor = XLColor.FromHtml("#1E293B");
                headerCell.Style.Font.FontColor = XLColor.White;
                headerCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            }

            // Dados
            for (int row = 0; row < rows.Count; row++)
            {
                for (int col = 0; col < columns.Count; col++)
                {
                    var val = rows[row].GetValueOrDefault(columns[col]);
                    var cell = ws.Cell(row + 2, col + 1);

                    if (val is DateTime dt)       cell.Value = dt;
                    else if (val is decimal dec)  cell.Value = dec;
                    else if (val is double dbl)   cell.Value = dbl;
                    else if (val is int integer)  cell.Value = integer;
                    else if (val is long lng)      cell.Value = lng;
                    else                           cell.Value = val?.ToString() ?? "";

                    // Zebra stripes
                    if (row % 2 == 1)
                        cell.Style.Fill.BackgroundColor = XLColor.FromHtml("#F8FAFC");
                }
            }

            // Rodapé com totais
            int footerRow = rows.Count + 3;
            ws.Cell(footerRow, 1).Value = $"Total de Documentos: {total}";
            ws.Cell(footerRow, 1).Style.Font.Bold = true;
            ws.Cell(footerRow + 1, 1).Value = $"Valor Total: R$ {valorTotal:N2}";
            ws.Cell(footerRow + 1, 1).Style.Font.Bold = true;

            ws.Columns().AdjustToContents();
        }

        // Informações da planilha
        ws.Cell("A1").WorksheetColumn().Width = Math.Max(ws.Cell("A1").WorksheetColumn().Width, 15);

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        return stream.ToArray();
    }
}
