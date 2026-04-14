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

        var options = new JsonSerializerOptions 
        { 
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            WriteIndented = true
        };

        return JsonSerializer.Serialize(ExecutedQueries);
    }

    public ErpPlugin(IConfiguration configuration)
    {
        _configuration = configuration;
        _connectionString = _configuration.GetConnectionString("DefaultConnection") ?? "";
    }
    private const string BASE_COLUMNS = "EMPRESA, CLIENTE, NOMEFANTASIA, CPFCNPJ, CIDADE, UF, DOCUMENTO, EMISSAO, VALORDOC, PARCELA, VALORORIG, VALORPAG, DATAVENCIMENTO, DATAPAGAMENTO, CONDPAG, TIPOPAG, SITUACAO";

        [Description("[DOMÍNIO: ABERTO] Consulta flexível de contas EM ABERTO (vencidas ou a vencer).")]
        public async Task<string> ConsultarContasEmAberto(
            [Description("OBRIGATÓRIO: 'PAGAR', 'RECEBER' ou 'INDEFINIDO'. REGRA: NUNCA presuma se um nome (ex: Minerva) é Cliente ou Fornecedor. No nosso sistema, qualquer pessoa pode ser ambos. Se o usuário não disser explicitamente o lado, PASSE O VALOR 'INDEFINIDO'.")] string tipoDominio = "INDEFINIDO",
            [Description("Data inicial Vencimento (ISO 8601). Vazio para ignorar.")] string dataInicioISO = null,
            [Description("Data final Vencimento (ISO 8601). Vazio para ignorar.")] string dataFimISO = null,
            [Description("Nome ou Fantasia do Fornecedor ou Cliente. Vazio para ignorar.")] string nomePessoa = null,
            [Description("Sigla do Estado (Ex: SP). Vazio para ignorar.")] string uf = null,
            [Description("Nome da Filial. Vazio para ignorar.")] string filial = null,
            [Description("CNPJ ou CPF (somente números). Vazio para ignorar.")] string cnpj = null,
            [Description("Apenas contas com atraso (verdadeiro/falso).")] bool apenasAtrasados = false,
            [Description("Agrupar resultados por: 'NENHUM', 'FORNECEDOR', 'CLIENTE', 'ANO', 'MES', 'FILIAL', 'METODO_PAGAMENTO', 'TOTAL' ou 'SITUACAO_VENCIMENTO'. USE 'SITUACAO_VENCIMENTO' quando o usuário perguntar sobre vencidos e a vencer ao mesmo tempo (ex: 'quantos vencidos e a vencer?'). REGRA DE OURO: Se o usuário pedir para agrupar/dividir/quebrar por 'empresa', NÃO EXECUTE A FERRAMENTA. Pergunte primeiro se ele quer por Filial (nossa empresa) ou por Cliente/Fornecedor.")] string agrupamento = "NENHUM"
        )
        {
            // 🚨 TRAVA DE SEGURANÇA: Se a IA não souber, ela cai aqui e devolve a pergunta pro chat
            if (tipoDominio.Equals("INDEFINIDO", StringComparison.OrdinalIgnoreCase))
            {
                return "SISTEMA: Pare a execução. Pergunte ao usuário se ele deseja consultar o Contas a Pagar (fornecedor) ou o Contas a Receber (cliente).";
            }

            string viewName = tipoDominio.Equals("PAGAR", StringComparison.OrdinalIgnoreCase) 
                ? "VW_DOC_FIN_PAG_ABERTO" 
                : "VW_DOC_FIN_REC_ABERTO";

            return await ExecuteDynamicQuery(
                viewName, 
                "DATAVENCIMENTO", 
                dataInicioISO, dataFimISO, nomePessoa, uf, filial, cnpj, agrupamento, apenasAtrasados, null, null);
        }

        [Description("[DOMÍNIO: PAGO] Consulta flexível de contas JÁ PAGAS/LIQUIDADAS.")]
        public async Task<string> ConsultarContasPagas(
            [Description("OBRIGATÓRIO: 'PAGAR', 'RECEBER' ou 'INDEFINIDO'. REGRA: NUNCA presuma se um nome (ex: Minerva) é Cliente ou Fornecedor. No nosso sistema, qualquer pessoa pode ser ambos. Se o usuário não disser explicitamente o lado, PASSE O VALOR 'INDEFINIDO'.")] string tipoDominio = "INDEFINIDO",
            [Description("Data inicial do Pagamento (ISO 8601). Vazio para ignorar.")] string dataPagamentoInicioISO = null,
            [Description("Data final do Pagamento (ISO 8601). Vazio para ignorar.")] string dataPagamentoFimISO = null,
            [Description("Nome ou Fantasia do Fornecedor ou Cliente. Vazio para ignorar.")] string nomePessoa = null,
            [Description("Sigla do Estado (Ex: SP). Vazio para ignorar.")] string uf = null,
            [Description("Nome da Filial. Vazio para ignorar.")] string filial = null,
            [Description("CNPJ ou CPF (somente números). Vazio para ignorar.")] string cnpj = null,
            [Description("Tipo de Pagamento/Meio (Ex: PIX, BOLETO). Vazio para ignorar.")] string tipoPagamento = null,
            [Description("Agrupar resultados por: 'NENHUM', 'FORNECEDOR', 'CLIENTE', 'ANO', 'MES', 'FILIAL', 'METODO_PAGAMENTO', 'TOTAL' ou 'SITUACAO_VENCIMENTO'. USE 'SITUACAO_VENCIMENTO' quando o usuário perguntar sobre vencidos e a vencer ao mesmo tempo. REGRA DE OURO: Se o usuário pedir para agrupar/dividir/quebrar por 'empresa', NÃO EXECUTE A FERRAMENTA. Pergunte primeiro se ele quer por Filial (nossa empresa) ou por Cliente/Fornecedor.")] string agrupamento = "NENHUM"
        )
        {
            // 🚨 TRAVA DE SEGURANÇA: Se a IA não souber, ela cai aqui e devolve a pergunta pro chat
            if (tipoDominio.Equals("INDEFINIDO", StringComparison.OrdinalIgnoreCase))
            {
                return "SISTEMA: Pare a execução. Pergunte ao usuário se ele deseja consultar o Contas a Pagar (fornecedor) ou o Contas a Receber (cliente).";
            }

            string viewName = tipoDominio.Equals("PAGAR", StringComparison.OrdinalIgnoreCase) 
                ? "VW_DOC_FIN_PAG_PAGO" 
                : "VW_DOC_FIN_REC_PAGO";

            return await ExecuteDynamicQuery(
                viewName, 
                "DATAPAGAMENTO", 
                dataPagamentoInicioISO, dataPagamentoFimISO, nomePessoa, uf, filial, cnpj, agrupamento, false, tipoPagamento, null);
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
        if (!string.IsNullOrEmpty(entidade))
        {
            conditions.Add("(UPPER(CLIENTE) LIKE UPPER(@ent) OR UPPER(NOMEFANTASIA) LIKE UPPER(@ent))");
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

        // Monta o WHERE apenas se tiver condições, limpíssimo
        string whereClause = conditions.Count > 0 ? " WHERE " + string.Join(" AND ", conditions) : "";
        string sumColumn = viewName.Contains("PAGO") ? "VALORPAG" : "VALORORIG";

        // 🚨 TRAVA DE SEGURANÇA: Previne erro fatal se a IA omitir a propriedade e passar NULL
        string agrupar = string.IsNullOrEmpty(agrupamento) ? "NENHUM" : agrupamento;

        if (agrupar.Equals("FORNECEDOR", StringComparison.OrdinalIgnoreCase) || agrupar.Equals("CLIENTE", StringComparison.OrdinalIgnoreCase))
            sql.Append($"SELECT CLIENTE as Entidade, SUM({sumColumn}) as Total, COUNT(*) as Quantidade FROM {viewName}{whereClause} GROUP BY CLIENTE ORDER BY Total DESC");
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
            // Listagem: retorna TOP 50 + COUNT(*) real para a IA saber o total exato
            var countSql = $"SELECT COUNT(*) FROM {viewName}{whereClause}";
            sql.Append($"SELECT TOP 50 {BASE_COLUMNS} FROM {viewName}{whereClause} ORDER BY {dateColumn} ASC");
            return await ExecuteListQueryWithCount(sql.ToString(), countSql, parameters.ToArray());
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

    private async Task<string> ExecuteListQueryWithCount(string listQuery, string countQuery, SqlParameter[] parameters)
    {
        if (string.IsNullOrEmpty(_connectionString))
            return "{\"error\": \"Connection string 'DefaultConnection' not found.\"}";

        try
        {
            string runnableListQuery = BuildRunnableQuery(listQuery, parameters);
            string runnableCountQuery = BuildRunnableQuery(countQuery, parameters);
            Console.WriteLine($"[ErpPlugin] 🟢 EXECUTING COUNT: {runnableCountQuery}");
            Console.WriteLine($"[ErpPlugin] 🟢 EXECUTING LIST:  {runnableListQuery}");

            ExecutedQueries.Add(runnableCountQuery);
            ExecutedQueries.Add(runnableListQuery);

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            // 1. COUNT(*) real — custo quase zero pro SQL Server
            int totalReal = 0;
            using (var countCmd = new SqlCommand(countQuery, connection))
            {
                foreach (var p in parameters)
                    countCmd.Parameters.Add(new SqlParameter(p.ParameterName, p.Value));
                var scalar = await countCmd.ExecuteScalarAsync();
                totalReal = Convert.ToInt32(scalar);
            }

            // 2. TOP 50 para exibição
            var results = new List<Dictionary<string, object>>();
            using (var listCmd = new SqlCommand(listQuery, connection))
            {
                // Parâmetros precisam ser recriados (SqlParameter não pode ser reusado entre commands)
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
                // ATENÇÃO IA: Este número é uma QUANTIDADE DE DOCUMENTOS (contagem), NÃO é um valor monetário.
                // Para obter o valor financeiro total (R$), use agrupamento='TOTAL'.
                TotalDeDocumentosNoBanco = totalReal,
                ExibindoPrimeirosDocumentos = results.Count,
                AlertaParaIA = totalReal > results.Count
                    ? $"LISTAGEM PARCIAL: Existem {totalReal} DOCUMENTOS (não R$) no banco, mas apenas {results.Count} estão listados. " +
                      $"NÃO some os valores desta lista — ela está incompleta. " +
                      $"Para o VALOR FINANCEIRO TOTAL ou QUANTIDADE EXATA, chame a ferramenta novamente com agrupamento='TOTAL'."
                    : $"LISTAGEM COMPLETA: Todos os {totalReal} documentos estão exibidos.",
                Data = results
            };

            var jsonResult = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
            Console.WriteLine($"[ErpPlugin] 🟢 LIST SUCCESS. Showing {results.Count} of {totalReal} total rows.");
            return jsonResult;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ErpPlugin] 🔴 FATAL QUERY ERROR: {ex.Message}");
            return $"{{\"error\": \"Database error: {ex.Message}\"}}";
        }
    }
}
