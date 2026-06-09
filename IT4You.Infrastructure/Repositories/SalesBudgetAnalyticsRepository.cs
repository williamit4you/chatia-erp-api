using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Data.Common;
using Dapper;
using IT4You.Application.FinanceAnalytics.Interfaces;
using IT4You.Application.SalesBudgetAnalytics.DTOs;
using IT4You.Application.SalesBudgetAnalytics.Interfaces;
using Microsoft.Extensions.Logging;

namespace IT4You.Infrastructure.Repositories
{
    public class SalesBudgetAnalyticsRepository : ISalesBudgetAnalyticsRepository
    {
        private readonly IErpConnectionFactory _connectionFactory;
        private readonly ILogger<SalesBudgetAnalyticsRepository> _logger;

        public SalesBudgetAnalyticsRepository(
            IErpConnectionFactory connectionFactory,
            ILogger<SalesBudgetAnalyticsRepository> logger)
        {
            _connectionFactory = connectionFactory;
            _logger = logger;
        }

        private async Task<IDbConnection> CreateConnectionAsync()
            => await _connectionFactory.CreateConnectionAsync();

        private static void AddDateFilters(DynamicParameters parameters, SalesBudgetFilterDto? filters, List<string> conditions, string columnName = "EMISSAO")
        {
            if (filters?.StartDate.HasValue == true)
            {
                conditions.Add($"{columnName} >= @StartDate");
                parameters.Add("StartDate", filters.StartDate.Value.Date);
            }

            if (filters?.EndDate.HasValue == true)
            {
                conditions.Add($"{columnName} <= @EndDate");
                parameters.Add("EndDate", filters.EndDate.Value.Date);
            }
        }

        private static string BuildWhere(IEnumerable<string> conditions)
        {
            var list = conditions.Where(x => !string.IsNullOrWhiteSpace(x)).ToList();
            return list.Count == 0 ? string.Empty : $" WHERE {string.Join(" AND ", list)}";
        }

        private static string DistinctBudgetCountSql(string alias = "")
        {
            var prefix = string.IsNullOrWhiteSpace(alias) ? string.Empty : $"{alias}.";
            return $"COUNT(DISTINCT CONCAT(CAST({prefix}CODEMPRESA AS NVARCHAR(50)), ':', CAST({prefix}ORCAMENTO AS NVARCHAR(50))))";
        }

        private static string BudgetKeySql(string alias = "")
        {
            var prefix = string.IsNullOrWhiteSpace(alias) ? string.Empty : $"{alias}.";
            return $"CONCAT(CAST({prefix}CODEMPRESA AS NVARCHAR(50)), ':', CAST({prefix}ORCAMENTO AS NVARCHAR(50)))";
        }

        private static string DistinctCustomerCountSql(string alias = "", string customerColumn = "CLIENTE")
        {
            var prefix = string.IsNullOrWhiteSpace(alias) ? string.Empty : $"{alias}.";
            return $"COUNT(DISTINCT CAST({prefix}{customerColumn} AS NVARCHAR(200)))";
        }

        private static string StatusValueExpression(string alias = "")
        {
            var prefix = string.IsNullOrWhiteSpace(alias) ? string.Empty : $"{alias}.";
            return $"UPPER(LTRIM(RTRIM(ISNULL(CAST({prefix}STATUS AS NVARCHAR(200)), ''))))";
        }

        private static string CanonicalStatusLabelExpression(string alias = "")
        {
            var prefix = string.IsNullOrWhiteSpace(alias) ? string.Empty : $"{alias}.";
            var status = StatusValueExpression(alias);
            return $@"CASE
                WHEN {status} = 'ABERTO' THEN 'Aberto'
                WHEN {status} = 'FECHADO' THEN 'Fechado'
                WHEN {status} = 'PARCIAL' THEN 'Parcial'
                WHEN {status} = 'PEDIDO' THEN 'Pedido'
                WHEN {status} = 'PERDEU' THEN 'Perdeu'
                WHEN {status} = 'PROJETO' THEN 'Projeto'
                WHEN {status} = '' THEN 'Sem status'
                ELSE LTRIM(RTRIM(ISNULL(CAST({prefix}STATUS AS NVARCHAR(200)), 'Sem status')))
            END";
        }

        private static string StatusOrderExpression(string alias = "")
        {
            var status = StatusValueExpression(alias);
            return $@"CASE
                WHEN {status} = 'PROJETO' THEN 1
                WHEN {status} = 'ABERTO' THEN 2
                WHEN {status} = 'PARCIAL' THEN 3
                WHEN {status} = 'FECHADO' THEN 4
                WHEN {status} = 'PEDIDO' THEN 5
                WHEN {status} = 'PERDEU' THEN 6
                WHEN {status} = '' THEN 98
                ELSE 99
            END";
        }

        private static string ApprovedStatusCondition(string alias = "")
        {
            var status = StatusValueExpression(alias);
            return $"({status} IN ('FECHADO', 'PEDIDO'))";
        }

        private static string OpenStatusCondition(string alias = "")
        {
            var status = StatusValueExpression(alias);
            return $"({status} IN ('PROJETO', 'ABERTO', 'PARCIAL'))";
        }

        private static string NegotiationStatusCondition(string alias = "")
        {
            return OpenStatusCondition(alias);
        }

        private static string LostStatusCondition(string alias = "")
        {
            var status = StatusValueExpression(alias);
            return $"({status} = 'PERDEU')";
        }

        private static string AppendCondition(string where, string condition)
            => string.IsNullOrWhiteSpace(where) ? $" WHERE {condition}" : $"{where} AND {condition}";

        private static decimal SafeToDecimal(object row, string key)
        {
            try
            {
                var dict = row as IDictionary<string, object>;
                if (dict != null && dict.TryGetValue(key, out var value))
                {
                    return value == null ? 0m : Convert.ToDecimal(value);
                }

                return Convert.ToDecimal(((object)row.GetType().GetProperty(key)?.GetValue(row)!) ?? 0m);
            }
            catch
            {
                return 0m;
            }
        }

        private static string? SafeToString(object row, string key)
        {
            try
            {
                var dict = row as IDictionary<string, object>;
                if (dict != null && dict.TryGetValue(key, out var value))
                {
                    return value?.ToString();
                }

                return row.GetType().GetProperty(key)?.GetValue(row)?.ToString();
            }
            catch
            {
                return null;
            }
        }

        public async Task<SalesBudgetKpiResponseDto> GetKpisAsync(SalesBudgetKpiRequestDto request)
        {
            using var connection = await CreateConnectionAsync();
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, request?.Filters, conditions);
            var where = BuildWhere(conditions);

            var totalAmountSql = $"SELECT ISNULL(SUM(VALORTOTAL), 0) FROM VW_SWIA_ORCAMENTO {where}";
            var totalCountSql = $"SELECT {DistinctBudgetCountSql()} FROM VW_SWIA_ORCAMENTO {where}";
            var openAmountSql = $"SELECT ISNULL(SUM(VALORTOTAL), 0) FROM VW_SWIA_ORCAMENTO {AppendCondition(where, OpenStatusCondition())}";
            var approvedAmountSql = $"SELECT ISNULL(SUM(VALORTOTAL), 0) FROM VW_SWIA_ORCAMENTO {AppendCondition(where, ApprovedStatusCondition())}";
            var lostAmountSql = $"SELECT ISNULL(SUM(VALORTOTAL), 0) FROM VW_SWIA_ORCAMENTO {AppendCondition(where, LostStatusCondition())}";
            var bestSellerSql = $@"SELECT TOP 1 ISNULL(VENDEDOR, 'Sem vendedor') 
                                   FROM VW_SWIA_ORCAMENTO {where}
                                   GROUP BY VENDEDOR
                                   ORDER BY SUM(VALORTOTAL) DESC";
            var bestCustomerSql = $@"SELECT TOP 1 ISNULL(CLIENTE, 'Sem cliente')
                                     FROM VW_SWIA_ORCAMENTO {where}
                                     GROUP BY CLIENTE
                                     ORDER BY SUM(VALORTOTAL) DESC";
            var bestProductSql = $@"SELECT TOP 1 ISNULL(I.ITEM, 'Sem produto')
                                    FROM VW_SWIA_ORCAMENTO_ITEM I
                                    INNER JOIN VW_SWIA_ORCAMENTO O
                                      ON O.CODEMPRESA = I.CODEMPRESA
                                     AND O.ORCAMENTO = I.ORCAMENTO
                                    {where.Replace("EMISSAO", "O.EMISSAO")}
                                    GROUP BY I.ITEM
                                    ORDER BY SUM(I.VALORTOTAL) DESC";

            var totalAmount = await connection.ExecuteScalarAsync<decimal>(totalAmountSql, parameters);
            var totalCount = await connection.ExecuteScalarAsync<int>(totalCountSql, parameters);
            var openAmount = await connection.ExecuteScalarAsync<decimal>(openAmountSql, parameters);
            var approvedAmount = await connection.ExecuteScalarAsync<decimal>(approvedAmountSql, parameters);
            var lostAmount = await connection.ExecuteScalarAsync<decimal>(lostAmountSql, parameters);
            var bestSeller = await connection.ExecuteScalarAsync<string?>(bestSellerSql, parameters);
            var bestCustomer = await connection.ExecuteScalarAsync<string?>(bestCustomerSql, parameters);
            var bestProduct = await connection.ExecuteScalarAsync<string?>(bestProductSql, parameters);

            var approvedCountSql = $"SELECT {DistinctBudgetCountSql()} FROM VW_SWIA_ORCAMENTO {AppendCondition(where, ApprovedStatusCondition())}";
            var approvedCount = await connection.ExecuteScalarAsync<int>(approvedCountSql, parameters);

            var conversionRate = totalCount > 0 ? (decimal)approvedCount / totalCount : 0m;
            var avgTicket = totalCount > 0 ? totalAmount / totalCount : 0m;

	            var items = new List<SalesBudgetKpiItemDto>
	            {
	                new() { KpiId = "kpi_total_budget_amount", Label = "Valor total orÃ§ado", Value = totalAmount, Format = "currency" },
	                new() { KpiId = "kpi_budget_count", Label = "Quantidade de orÃ§amentos", Value = totalCount, Format = "number" },
	                new() { KpiId = "kpi_avg_ticket", Label = "Ticket mÃ©dio", Value = avgTicket, Format = "currency" },
	                new() { KpiId = "kpi_open_amount", Label = "Valor em aberto", Value = openAmount, Format = "currency" },
	                new() { KpiId = "kpi_approved_amount", Label = "Valor aprovado", Value = approvedAmount, Format = "currency" },
	                new() { KpiId = "kpi_lost_amount", Label = "Valor perdido", Value = lostAmount, Format = "currency" },
	                new() { KpiId = "kpi_conversion_rate", Label = "Taxa de conversÃ£o", Value = conversionRate, Format = "percentage", Warning = "Depende do mapeamento atual de STATUS." },
	                new() { KpiId = "kpi_best_seller", Label = "Melhor vendedor", TextValue = bestSeller ?? "Sem dados", Format = "text" },
	                new() { KpiId = "kpi_best_customer", Label = "Melhor cliente", TextValue = bestCustomer ?? "Sem dados", Format = "text" },
	                new() { KpiId = "kpi_best_product", Label = "Melhor produto", TextValue = bestProduct ?? "Sem dados", Format = "text" },
	            };

            if (request?.KpiIds?.Count > 0)
            {
                var requested = request.KpiIds
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                items = items.Where(item => requested.Contains(item.KpiId)).ToList();
            }

            return new SalesBudgetKpiResponseDto { Items = items };
        }

        public async Task<SalesBudgetChartBatchResponseDto> GetChartsAsync(SalesBudgetChartBatchRequestDto request)
        {
            var connection = await CreateConnectionAsync();
            var response = new SalesBudgetChartBatchResponseDto();
            var chartIds = request.ChartIds
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Select(id => id.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            try
            {
                if (connection.State != ConnectionState.Open)
                    connection.Open();

                foreach (var chartId in chartIds)
                {
                    try
                    {
                        response.Items.Add(await BuildChartAsync(connection, request.Filters, chartId));
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Falha ao construir gráfico '{ChartId}': {Message}", chartId, ex.Message);
                        response.Items.Add(new SalesBudgetChartDatasetDto
                        {
                            ChartId = chartId,
                            Title = chartId,
                            Visualization = "error",
                            Data = new List<SalesBudgetChartPointDto>(),
                            Totals = new Dictionary<string, decimal>(),
                            Meta = new SalesBudgetChartMetaDto
                            {
                                Source = "error",
                                Warnings = new List<string> { $"Erro ao gerar este gráfico: {ex.Message}" }
                            }
                        });

                        // Reconnect if connection was broken by the error
                        if (connection.State != ConnectionState.Open)
                        {
                            try
                            {
                                connection.Dispose();
                                connection = await CreateConnectionAsync();
                                if (connection.State != ConnectionState.Open)
                                    connection.Open();
                            }
                            catch (Exception reconnectEx)
                            {
                                _logger.LogError(reconnectEx, "Falha ao reconectar apos erro no gráfico '{ChartId}'.", chartId);
                            }
                        }
                    }
                }
            }
            finally
            {
                connection.Dispose();
            }

            return response;
        }

        public async Task<List<SalesBudgetChartQueryDetailsItemDto>> GetChartQueryDetailsAsync(
            SalesBudgetChartQueryDetailsRequestDto request
        )
        {
            var connection = await CreateConnectionAsync();
            var chartIds = request?.ChartIds?
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Select(id => id.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList() ?? new();

            var filters = new SalesBudgetFilterDto
            {
                StartDate = request?.StartDate,
                EndDate = request?.EndDate,
            };

            try
            {
                if (connection.State != ConnectionState.Open)
                    connection.Open();

                var items = new List<SalesBudgetChartQueryDetailsItemDto>();

                foreach (var chartId in chartIds)
                {
                    var capturedSql = new List<string>();
                    var traced = connection as DbConnection is DbConnection dbConnection
                        ? new SqlTraceConnection(dbConnection, capturedSql)
                        : null;

                    var rules = new List<string>();
                    if (filters.StartDate.HasValue || filters.EndDate.HasValue)
                    {
                        var startLabel = filters.StartDate?.ToString("yyyy-MM-dd") ?? "-";
                        var endLabel = filters.EndDate?.ToString("yyyy-MM-dd") ?? "-";
                        rules.Add($"Período: {startLabel} ate {endLabel} (coluna EMISSAO).");
                    }

                    if (traced == null)
                    {
                        rules.Add("Captura automatica de SQL indisponivel: o provider atual não expoe DbConnection.");
                    }

                    try
                    {
                        // Reuse the same builder used by the chart endpoint so the SELECTs match reality.
                        await BuildChartAsync(traced ?? connection, filters, chartId);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Falha ao capturar SQL do gráfico '{ChartId}': {Message}", chartId, ex.Message);
                        rules.Add($"Falha ao capturar SQL automaticamente: {ex.Message}");
                    }

                    items.Add(new SalesBudgetChartQueryDetailsItemDto
                    {
                        ChartId = chartId,
                        SqlQueries = capturedSql
                            .Where(sql => !string.IsNullOrWhiteSpace(sql))
                            .Select(sql => sql.Trim())
                            .Distinct()
                            .ToList(),
                        Rules = rules,
                    });
                }

                return items;
            }
            finally
            {
                connection.Dispose();
            }
        }

        private sealed class SqlTraceConnection : DbConnection
        {
            private readonly DbConnection _inner;
            private readonly List<string> _captured;

            public SqlTraceConnection(DbConnection inner, List<string> captured)
            {
                _inner = inner;
                _captured = captured;
            }

            public DbConnection InnerConnection => _inner;

            public override string ConnectionString { get => _inner.ConnectionString; set => _inner.ConnectionString = value ?? string.Empty; }
            public override int ConnectionTimeout => _inner.ConnectionTimeout;
            public override string Database => _inner.Database;
            public override string DataSource => _inner.DataSource;
            public override string ServerVersion => _inner.ServerVersion;
            public override ConnectionState State => _inner.State;
            public override void ChangeDatabase(string databaseName) => _inner.ChangeDatabase(databaseName);
            public override void Close() => _inner.Close();
            public override void Open() => _inner.Open();
            public override Task OpenAsync(CancellationToken cancellationToken) => _inner.OpenAsync(cancellationToken);

            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
                => _inner.BeginTransaction(isolationLevel);

            protected override DbCommand CreateDbCommand()
                => new SqlTraceCommand(_inner.CreateCommand(), _captured, this);

            protected override void Dispose(bool disposing)
            {
                // Do not dispose the shared inner connection here.
            }
        }

        private sealed class SqlTraceCommand : DbCommand
        {
            private readonly DbCommand _inner;
            private readonly List<string> _captured;
            private readonly SqlTraceConnection _owner;

            public SqlTraceCommand(DbCommand inner, List<string> captured, SqlTraceConnection owner)
            {
                _inner = inner;
                _captured = captured;
                _owner = owner;
            }

            private void Capture()
            {
                var sql = _inner.CommandText;
                if (!string.IsNullOrWhiteSpace(sql))
                    _captured.Add(sql);
            }

            public override string CommandText { get => _inner.CommandText; set => _inner.CommandText = value ?? string.Empty; }
            public override int CommandTimeout { get => _inner.CommandTimeout; set => _inner.CommandTimeout = value; }
            public override CommandType CommandType { get => _inner.CommandType; set => _inner.CommandType = value; }
            public override bool DesignTimeVisible { get => _inner.DesignTimeVisible; set => _inner.DesignTimeVisible = value; }
            public override UpdateRowSource UpdatedRowSource { get => _inner.UpdatedRowSource; set => _inner.UpdatedRowSource = value; }
            protected override DbConnection? DbConnection
            {
                get => _owner;
                set => _inner.Connection = value is SqlTraceConnection traced ? traced.InnerConnection : value;
            }
            protected override DbParameterCollection DbParameterCollection => _inner.Parameters;
            protected override DbTransaction? DbTransaction
            {
                get => _inner.Transaction as DbTransaction;
                set => _inner.Transaction = value;
            }

            public override void Cancel() => _inner.Cancel();
            public override int ExecuteNonQuery() { Capture(); return _inner.ExecuteNonQuery(); }
            public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
            {
                Capture();
                return await _inner.ExecuteNonQueryAsync(cancellationToken);
            }

            public override object? ExecuteScalar() { Capture(); return _inner.ExecuteScalar(); }
            public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
            {
                Capture();
                return await _inner.ExecuteScalarAsync(cancellationToken);
            }

            public override void Prepare() => _inner.Prepare();
            protected override DbParameter CreateDbParameter() => _inner.CreateParameter();
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
            {
                Capture();
                return _inner.ExecuteReader(behavior);
            }

            protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
            {
                Capture();
                return await _inner.ExecuteReaderAsync(behavior, cancellationToken);
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _inner.Dispose();
                }

                base.Dispose(disposing);
            }
        }

        private async Task<SalesBudgetChartDatasetDto> BuildChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            return chartId switch
            {
                "overview_total_amount_period" => await BuildSingleValueChartAsync(connection, filters, chartId, "Valor total de orÃ§amentos por perÃ­odo", "kpi", "SELECT ISNULL(SUM(VALORTOTAL), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Valor total", "currency"),
                "overview_total_count_period" => await BuildSingleValueChartAsync(connection, filters, chartId, "Quantidade de orÃ§amentos por perÃ­odo", "kpi", $"SELECT {DistinctBudgetCountSql()} AS Valor FROM VW_SWIA_ORCAMENTO {{0}}", "Quantidade", "number"),
                "overview_avg_ticket" => await BuildAverageTicketChartAsync(connection, filters),
                "overview_monthly_evolution" => await BuildMonthlyEvolutionChartAsync(connection, filters),
                "overview_weekly_evolution" => await BuildWeeklyEvolutionChartAsync(connection, filters, chartId),
                "overview_daily_evolution" => await BuildDailyEvolutionChartAsync(connection, filters, chartId),
                "overview_current_vs_previous_month" => await BuildCurrentVsPreviousMonthChartAsync(connection, filters, chartId),
                "overview_current_year_vs_previous_year" => await BuildCurrentYearVsPreviousYearChartAsync(connection, filters, chartId),
                "overview_top_days_by_volume" => await BuildTopDaysByVolumeChartAsync(connection, filters, chartId),
                "overview_top_months_by_amount" => await BuildTopMonthsByAmountChartAsync(connection, filters, chartId),
                "overview_month_seasonality" => await BuildMonthSeasonalityChartAsync(connection, filters, chartId),
                "overview_amount_by_company" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total por empresa/filial", "bar", "EMPRESA", "Empresa", "SUM(VALORTOTAL)", "currency"),
                "overview_count_by_company" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Quantidade de orÃ§amentos por empresa/filial", "bar", "EMPRESA", "Empresa", DistinctBudgetCountSql(), "number"),
                "overview_weekday_heatmap" => await BuildWeekdayHeatmapChartAsync(connection, filters),
                "overview_month_year_heatmap" => await BuildMonthYearHeatmapChartAsync(connection, filters, chartId),

                "funnel_by_status" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Funil por status do orÃ§amento", "bar", "STATUS", "Status", "SUM(VALORTOTAL)", "currency"),
                "funnel_amount_by_status" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total por status", "bar", "STATUS", "Status", "SUM(VALORTOTAL)", "currency"),
                "funnel_count_by_status" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Quantidade de orÃ§amentos por status", "bar", "STATUS", "Status", DistinctBudgetCountSql(), "number", null, 15),
                "funnel_conversion_percent_by_status" => await BuildStatusParticipationChartAsync(connection, filters, chartId),
                "funnel_open_approved_lost" => await BuildOpenApprovedLostChartAsync(connection, filters, chartId),
                "funnel_pending_amount" => await BuildOpenPipelineLikeChartAsync(connection, filters, chartId, "Valor parado em orÃ§amentos pendentes"),
                "funnel_approval_rate" => await BuildConversionKpiChartAsync(connection, filters, chartId, "Taxa de aprovaÃ§Ã£o de orÃ§amentos"),
                "funnel_loss_cancel_rate" => await BuildLostCancelRateChartAsync(connection, filters, chartId),
                "funnel_conversion_evolution" => await BuildConversionEvolutionChartAsync(connection, filters, chartId),
                "funnel_conversion_by_seller" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "ConversÃ£o por vendedor", "VENDEDOR", "Vendedor"),
                "funnel_conversion_by_customer" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "ConversÃ£o por cliente", "CLIENTE", "Cliente"),
                "funnel_conversion_by_origin" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "ConversÃ£o por origem", "ORIGEM", "Origem"),
                "funnel_conversion_by_geo" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "ConversÃ£o por UF", "UF", "UF"),
                "funnel_conversion_by_payment" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "ConversÃ£o por condiÃ§Ã£o de pagamento", "CONDPAG", "Condicao"),
                "funnel_blocking_status_ranking" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Ranking de status que mais travam vendas", "bar", "STATUS", "Status", "SUM(VALORTOTAL)", "currency", OpenStatusCondition()),

                "seller_total_amount" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total orÃ§ado por vendedor", "bar", "VENDEDOR", "Vendedor", "SUM(VALORTOTAL)", "currency"),
                "seller_total_count" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Quantidade de orÃ§amentos por vendedor", "bar", "VENDEDOR", "Vendedor", DistinctBudgetCountSql(), "number"),
                "seller_avg_ticket" => await BuildAverageTicketByGroupChartAsync(connection, filters, chartId, "Ticket mÃ©dio por vendedor", "VENDEDOR", "Vendedor"),
                "seller_conversion" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "ConversÃ£o por vendedor", "VENDEDOR", "Vendedor"),
                "seller_avg_discount" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "Desconto mÃ©dio concedido por vendedor", "VENDEDOR", "Vendedor", "PERCENTUALDESCONTO", "VALORDESCONTO"),
                "seller_avg_markup" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Markup mÃ©dio por vendedor", "bar", "VENDEDOR", "Vendedor", "AVG(MARKUP)", "number"),
                "seller_avg_surcharge" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "AcrÃ©scimo mÃ©dio por vendedor", "VENDEDOR", "Vendedor", "PERCENTUALACRESCIMO", "VALORACRESCIMO"),
                "seller_avg_freight" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor de frete mÃ©dio por vendedor", "bar", "VENDEDOR", "Vendedor", "AVG(VALORFRETE)", "currency"),
                "seller_ranking_amount" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Ranking de vendedores por valor total", "bar", "VENDEDOR", "Vendedor", "SUM(VALORTOTAL)", "currency", null, 12),
                "seller_ranking_count" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Ranking de vendedores por quantidade", "bar", "VENDEDOR", "Vendedor", DistinctBudgetCountSql(), "number", null, 12),
                "seller_ranking_ticket" => await BuildRankingAvgTicketBySellerChartAsync(connection, filters, chartId),
                "seller_ranking_markup" => await BuildRankingAvgMarkupBySellerChartAsync(connection, filters, chartId),
                "seller_most_lost" => await BuildStatusCountAmountByGroupChartAsync(connection, filters, chartId, "Vendedores com mais orÃ§amentos perdidos", "VENDEDOR", "Vendedor", LostStatusCondition(), 12),
                "seller_most_approved" => await BuildStatusCountAmountByGroupChartAsync(connection, filters, chartId, "Vendedores com mais orÃ§amentos aprovados", "VENDEDOR", "Vendedor", ApprovedStatusCondition(), 12),
                "seller_monthly_evolution" => await BuildMonthlyEvolutionByGroupChartAsync(connection, filters, chartId, "EvoluÃ§Ã£o mensal por vendedor", "VENDEDOR", 5),
                "seller_comparison" => await BuildSellerComparisonChartAsync(connection, filters, chartId, 8),
                "seller_share_total" => await BuildShareByGroupChartAsync(connection, filters, chartId, "ParticipaÃ§Ã£o de cada vendedor no faturamento", "VENDEDOR", "Vendedor", 12),
                "seller_abc_curve" => await BuildAbcChartAsync(connection, filters, chartId, "Curva ABC de vendedores", "VENDEDOR", "Vendedor"),
                "seller_top_product" => await BuildTopCrossChartAsync(connection, filters, chartId, "Vendedor x produto mais orÃ§ado", "O.VENDEDOR", "Vendedor", "I.ITEM", "Produto"),
                "seller_top_customer" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Vendedor x cliente mais atendido", "VENDEDOR", "Vendedor", "CLIENTE", "Cliente"),

                "customer_top_amount" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Top clientes por valor orÃ§ado", "bar", "CLIENTE", "Cliente", "SUM(VALORTOTAL)", "currency"),
                "customer_top_count" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Top clientes por quantidade de orÃ§amentos", "bar", "CLIENTE", "Cliente", DistinctBudgetCountSql(), "number", null, 12),
                "customer_avg_ticket" => await BuildAverageTicketByGroupChartAsync(connection, filters, chartId, "Ticket mÃ©dio por cliente", "CLIENTE", "Cliente", 12),
                "customer_recurring" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Clientes recorrentes", "bar", "CLIENTE", "Cliente", DistinctBudgetCountSql(), "number"),
                "customer_new_period" => await BuildNewCustomersChartAsync(connection, filters, chartId),
                "customer_inactive_recent" => await BuildInactiveCustomersChartAsync(connection, filters, chartId),
                "customer_highest_discount" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "Clientes com maior desconto recebido", "CLIENTE", "Cliente", "PERCENTUALDESCONTO", "VALORDESCONTO", 12),
                "customer_highest_markup" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Clientes com maior markup", "bar", "CLIENTE", "Cliente", "AVG(MARKUP)", "number", null, 12),
                "customer_highest_open_amount" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Clientes com maior valor em aberto", "bar", "CLIENTE", "Cliente", "SUM(VALORTOTAL)", "currency", OpenStatusCondition(), 12),
                "customer_highest_conversion" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "Clientes com maior taxa de conversÃ£o", "CLIENTE", "Cliente"),
                "customer_low_conversion" => await BuildLowConversionCustomersChartAsync(connection, filters, chartId),
                "customer_abc_curve" => await BuildAbcChartAsync(connection, filters, chartId, "Curva ABC de clientes", "CLIENTE", "Cliente"),
                "customer_top_share" => await BuildShareByGroupChartAsync(connection, filters, chartId, "ParticipaÃ§Ã£o dos principais clientes no total", "CLIENTE", "Cliente", 12),
                "customer_evolution" => await BuildMonthlyEvolutionByGroupChartAsync(connection, filters, chartId, "EvoluÃ§Ã£o de orÃ§amentos por cliente", "CLIENTE", 5),
                "customer_top_products" => await BuildTopCrossChartAsync(connection, filters, chartId, "Cliente x produtos mais orÃ§ados", "O.CLIENTE", "Cliente", "I.ITEM", "Produto"),
                "customer_responsible_seller" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Cliente x vendedor responsÃ¡vel", "CLIENTE", "Cliente", "VENDEDOR", "Vendedor", 12),
                "customer_origin" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Cliente x origem", "CLIENTE", "Cliente", "ORIGEM", "Origem", 12),
                "customer_payment_condition" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Cliente x condiÃ§Ã£o de pagamento", "CLIENTE", "Cliente", "CONDPAG", "Condicao", 12),
                "customer_by_city" => await BuildDistinctCustomerCountByGroupChartAsync(connection, filters, chartId, "Clientes por cidade", "CIDADE", "Cidade", 12),
                "customer_by_uf" => await BuildDistinctCustomerCountByGroupChartAsync(connection, filters, chartId, "Clientes por UF", "UF", "UF", 27),

                "product_top_amount" => await BuildGroupedItemChartAsync(connection, filters, chartId, "Top produtos por valor total", "bar", "I.ITEM", "Produto", "SUM(I.VALORTOTAL)", "currency"),
                "product_top_quantity" => await BuildGroupedItemChartAsync(connection, filters, chartId, "Top produtos por quantidade vendida/orÃ§ada", "bar", "I.ITEM", "Produto", "SUM(ISNULL(I.QUANTIDADE, 0))", "number", 12),
                "product_highest_avg_ticket" => await BuildAverageTicketByItemChartAsync(connection, filters, chartId),
                "product_highest_discount" => await BuildAveragePercentByItemChartAsync(connection, filters, chartId, "Produtos com maior desconto aplicado", "PERCENTUALDESCONTO", "VALORDESCONTO"),
                "product_highest_markup" => await BuildAverageNumberByItemChartAsync(connection, filters, chartId, "Produtos com maior markup", "AVG(ISNULL(I.MARKUP, 0))", "number"),
                "product_highest_surcharge" => await BuildAveragePercentByItemChartAsync(connection, filters, chartId, "Produtos com maior acrÃ©scimo", "PERCENTUALACRESCIMO", "VALORACRESCIMO"),
                "product_most_quoted_period" => await BuildGroupedItemChartAsync(connection, filters, chartId, "Produtos mais orÃ§ados por perÃ­odo", "bar", "I.ITEM", "Produto", DistinctBudgetCountSql("I"), "number", 12),
                "product_least_quoted" => await BuildLeastQuotedProductsChartAsync(connection, filters, chartId),
                "product_share_total" => await BuildShareByItemChartAsync(connection, filters, chartId, 12),
                "product_abc_curve" => await BuildAbcItemChartAsync(connection, filters, chartId, 12),
                "product_monthly_evolution" => await BuildMonthlyEvolutionByItemChartAsync(connection, filters, chartId, 5),
                "product_by_seller" => await BuildTopCrossChartAsync(connection, filters, chartId, "Produtos por vendedor", "O.VENDEDOR", "Vendedor", "I.ITEM", "Produto"),
                "product_by_customer" => await BuildTopCrossChartAsync(connection, filters, chartId, "Produtos por cliente", "O.CLIENTE", "Cliente", "I.ITEM", "Produto"),
                "product_by_geo" => await BuildTopCrossChartAsync(connection, filters, chartId, "Produtos por UF", "O.UF", "UF", "I.ITEM", "Produto"),
                "product_by_company" => await BuildTopCrossChartAsync(connection, filters, chartId, "Produtos por empresa/filial", "O.EMPRESA", "Empresa", "I.ITEM", "Produto"),
                "product_by_origin" => await BuildTopCrossChartAsync(connection, filters, chartId, "Produtos por origem do orÃ§amento", "O.ORIGEM", "Origem", "I.ITEM", "Produto"),
                "product_avg_quantity_per_item" => await BuildAverageNumberByItemChartAsync(connection, filters, chartId, "Quantidade mÃ©dia por item", "AVG(ISNULL(I.QUANTIDADE, 0))", "number"),
                "product_avg_value_per_item" => await BuildAverageNumberByItemChartAsync(connection, filters, chartId, "Valor mÃ©dio por item", "AVG(ISNULL(I.VALORTOTAL, 0))", "currency"),
                "product_highest_gross_unit" => await BuildAverageNumberByItemChartAsync(connection, filters, chartId, "Produtos com maior valor unitÃ¡rio bruto", "AVG(ISNULL(I.VALORUNITARIOBRUTO, 0))", "currency"),
                "product_highest_net_unit" => await BuildAverageNumberByItemChartAsync(connection, filters, chartId, "Produtos com maior valor unitÃ¡rio lÃ­quido", "AVG(ISNULL(I.VALORUNITARIOLIQUIDO, 0))", "currency"),
                "product_gross_net_gap" => await BuildGrossNetGapByItemChartAsync(connection, filters, chartId),
                "product_demand_drop" => await BuildProductDemandDropChartAsync(connection, filters, chartId),
                "product_demand_growth" => await BuildProductDemandGrowthChartAsync(connection, filters, chartId),
                "product_mix_per_budget" => await BuildProductMixPerBudgetChartAsync(connection, filters, chartId),
                "product_cooccurrence" => await BuildProductCooccurrenceChartAsync(connection, filters, chartId),

                "margin_total_discount" => await BuildSingleValueChartAsync(connection, filters, chartId, "Valor total de descontos concedidos", "kpi", "SELECT ISNULL(SUM(VALORDESCONTO), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Desconto total", "currency"),
                "margin_discount_vs_conversion" => await BuildDiscountVsConversionChartAsync(connection, filters, chartId),
                "margin_avg_markup_general" => await BuildSingleValueChartAsync(connection, filters, chartId, "Markup mÃ©dio geral", "kpi", "SELECT ISNULL(AVG(MARKUP), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Markup mÃ©dio", "number"),
                "margin_avg_discount_percent" => await BuildSinglePercentKpiChartAsync(connection, filters, chartId, "Percentual mÃ©dio de desconto", "PERCENTUALDESCONTO"),
                "margin_discount_by_seller" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "Desconto por vendedor", "VENDEDOR", "Vendedor", "PERCENTUALDESCONTO", "VALORDESCONTO", 12),
                "margin_discount_by_customer" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "Desconto por cliente", "CLIENTE", "Cliente", "PERCENTUALDESCONTO", "VALORDESCONTO", 12),
                "margin_discount_by_origin" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "Desconto por origem", "ORIGEM", "Origem", "PERCENTUALDESCONTO", "VALORDESCONTO", 12),
                "margin_discount_by_payment" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "Desconto por condiÃ§Ã£o de pagamento", "CONDPAG", "Condicao", "PERCENTUALDESCONTO", "VALORDESCONTO", 12),
                "margin_discount_by_product" => await BuildAveragePercentByItemChartAsync(connection, filters, chartId, "Desconto por produto", "PERCENTUALDESCONTO", "VALORDESCONTO", 12),
                "margin_highest_discount_ranking" => await BuildHighestDiscountRankingChartAsync(connection, filters, chartId),
                "margin_above_avg_discount_budgets" => await BuildAboveAvgDiscountBudgetsChartAsync(connection, filters, chartId),
                "margin_discount_impact_total" => await BuildDiscountImpactTotalChartAsync(connection, filters, chartId),
                "margin_discount_vs_seller" => await BuildDiscountVsSellerChartAsync(connection, filters, chartId),
                "margin_total_surcharge" => await BuildSingleValueChartAsync(connection, filters, chartId, "Valor total de acrÃ©scimos", "kpi", "SELECT ISNULL(SUM(VALORACRESCIMO), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "AcrÃ©scimo total", "currency"),
                "margin_avg_surcharge_percent" => await BuildSinglePercentKpiChartAsync(connection, filters, chartId, "Percentual mÃ©dio de acrÃ©scimo", "PERCENTUALACRESCIMO"),
                "margin_surcharge_by_seller" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "AcrÃ©scimo por vendedor", "VENDEDOR", "Vendedor", "PERCENTUALACRESCIMO", "VALORACRESCIMO", 12),
                "margin_surcharge_by_customer" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "AcrÃ©scimo por cliente", "CLIENTE", "Cliente", "PERCENTUALACRESCIMO", "VALORACRESCIMO", 12),
                "margin_surcharge_by_product" => await BuildAveragePercentByItemChartAsync(connection, filters, chartId, "AcrÃ©scimo por produto", "PERCENTUALACRESCIMO", "VALORACRESCIMO", 12),
                "margin_markup_by_seller" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Markup por vendedor", "bar", "VENDEDOR", "Vendedor", "AVG(MARKUP)", "number", null, 12),
                "margin_markup_by_customer" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Markup por cliente", "bar", "CLIENTE", "Cliente", "AVG(MARKUP)", "number", null, 12),
                "margin_markup_by_origin" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Markup por origem", "bar", "ORIGEM", "Origem", "AVG(MARKUP)", "number", null, 12),
                "margin_markup_by_product" => await BuildAverageNumberByItemChartAsync(connection, filters, chartId, "Markup por produto", "AVG(ISNULL(I.MARKUP, 0))", "number", 12),
                "margin_low_markup_budgets" => await BuildLowMarkupBudgetsChartAsync(connection, filters, chartId, 10, "OrÃ§amentos com markup baixo"),
                "margin_possible_bad_margin_budgets" => await BuildLowMarkupBudgetsChartAsync(connection, filters, chartId),
                "margin_gross_vs_net" => await BuildGrossVsNetChartAsync(connection, filters, chartId),

                "source_total_amount" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total por origem", "pie", "ORIGEM", "Origem", "SUM(VALORTOTAL)", "currency"),
                "source_total_count" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Quantidade de orÃ§amentos por origem", "bar", "ORIGEM", "Origem", DistinctBudgetCountSql(), "number", null, 12),
                "source_avg_ticket" => await BuildAverageTicketByGroupChartAsync(connection, filters, chartId, "Ticket mÃ©dio por origem", "ORIGEM", "Origem", 12),
                "source_conversion" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "ConversÃ£o por origem", "ORIGEM", "Origem"),
                "source_highest_avg_discount" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "Origem com maior desconto mÃ©dio", "ORIGEM", "Origem", "PERCENTUALDESCONTO", "VALORDESCONTO", 12),
                "source_highest_markup" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Origem com maior markup", "bar", "ORIGEM", "Origem", "AVG(MARKUP)", "number", null, 12),
                "source_evolution" => await BuildMonthlyEvolutionByGroupChartAsync(connection, filters, chartId, "EvoluÃ§Ã£o de origens por perÃ­odo", "ORIGEM", 5),
                "source_share_total" => await BuildShareByGroupChartAsync(connection, filters, chartId, "ParticipaÃ§Ã£o de cada origem no total", "ORIGEM", "Origem", 12),
                "source_by_seller" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Origem x vendedor", "ORIGEM", "Origem", "VENDEDOR", "Vendedor", 12),
                "source_by_product" => await BuildTopCrossChartAsync(connection, filters, chartId, "Origem x produto", "O.ORIGEM", "Origem", "I.ITEM", "Produto", 12),
                "source_by_customer" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Origem x cliente", "ORIGEM", "Origem", "CLIENTE", "Cliente", 12),
                "source_by_geo" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Origem x UF", "ORIGEM", "Origem", "UF", "UF", 12),
                "source_best_channels" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Ranking de melhores canais de venda", "bar", "ORIGEM", "Origem", "SUM(VALORTOTAL)", "currency"),
                "source_high_volume_low_conversion" => await BuildVolumeLowConversionChartAsync(connection, filters, chartId, "Canais com muito orÃ§amento e pouca conversÃ£o", "ORIGEM", "Origem"),
                "source_low_volume_high_ticket" => await BuildLowVolumeHighTicketByOriginChartAsync(connection, filters, chartId),

                "geo_amount_by_uf" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total por UF", "bar", "UF", "UF", "SUM(VALORTOTAL)", "currency"),
                "geo_count_by_uf" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Quantidade de orÃ§amentos por UF", "bar", "UF", "UF", DistinctBudgetCountSql(), "number", null, 27),
                "geo_avg_ticket_by_uf" => await BuildAverageTicketByGroupChartAsync(connection, filters, chartId, "Ticket mÃ©dio por UF", "UF", "UF", 27),
                "geo_conversion_by_uf" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "ConversÃ£o por UF", "UF", "UF", 27),
                "geo_count_by_city" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Quantidade por cidade", "bar", "CIDADE", "Cidade", DistinctBudgetCountSql(), "number"),
                "geo_amount_by_city" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total por cidade", "bar", "CIDADE", "Cidade", "SUM(VALORTOTAL)", "currency", null, 12),
                "geo_top_cities_count" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Ranking de cidades com mais vendas/orÃ§amentos", "bar", "CIDADE", "Cidade", DistinctBudgetCountSql(), "number", null, 12),
                "geo_top_cities_ticket" => await BuildTopCitiesTicketChartAsync(connection, filters, chartId, 12, 10),
                "geo_state_heatmap" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Mapa de calor por estado", "heatmap", "UF", "UF", "SUM(VALORTOTAL)", "currency"),
                "geo_city_heatmap" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Mapa de calor por cidade", "heatmap", "CIDADE", "Cidade", "SUM(VALORTOTAL)", "currency", null, 28),
                "geo_seller_by_region" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Vendedor por regiÃ£o", "UF", "UF", "VENDEDOR", "Vendedor", 12),
                "geo_top_product_by_uf" => await BuildTopCrossChartAsync(connection, filters, chartId, "Produto mais orÃ§ado por UF", "O.UF", "UF", "I.ITEM", "Produto", 12),
                "geo_customer_by_region" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Cliente por regiÃ£o", "UF", "UF", "CLIENTE", "Cliente", 12),
                "geo_origin_by_region" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Origem por regiÃ£o", "UF", "UF", "ORIGEM", "Origem", 12),
                "geo_highest_avg_discount_regions" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "RegiÃµes com maior desconto mÃ©dio", "UF", "UF", "PERCENTUALDESCONTO", "VALORDESCONTO", 27),
                "geo_highest_markup_regions" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "RegiÃµes com maior markup", "bar", "UF", "UF", "AVG(MARKUP)", "number", null, 27),
                "geo_growth_opportunity_regions" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "RegiÃµes com maior oportunidade de crescimento", "bar", "UF", "UF", "SUM(VALORDESCONTO)", "currency"),

                "payment_total_amount" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total por condiÃ§Ã£o de pagamento", "bar", "CONDPAG", "Condicao", "SUM(VALORTOTAL)", "currency"),
                "payment_total_count" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Quantidade de orÃ§amentos por condiÃ§Ã£o de pagamento", "bar", "CONDPAG", "Condicao", DistinctBudgetCountSql(), "number", null, 12),
                "payment_avg_ticket" => await BuildAverageTicketByGroupChartAsync(connection, filters, chartId, "Ticket mÃ©dio por condiÃ§Ã£o de pagamento", "CONDPAG", "Condicao", 12),
                "payment_avg_discount" => await BuildAveragePercentByGroupChartAsync(connection, filters, chartId, "Desconto mÃ©dio por condiÃ§Ã£o de pagamento", "CONDPAG", "Condicao", "PERCENTUALDESCONTO", "VALORDESCONTO", 12),
                "payment_avg_markup" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Markup mÃ©dio por condiÃ§Ã£o de pagamento", "bar", "CONDPAG", "Condicao", "AVG(MARKUP)", "number", null, 12),
                "payment_conversion" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "ConversÃ£o por condiÃ§Ã£o de pagamento", "CONDPAG", "Condicao"),
                "payment_most_used" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "CondiÃ§Ãµes de pagamento mais usadas", "pie", "CONDPAG", "Condicao", DistinctBudgetCountSql(), "number"),
                "payment_vs_approval" => await BuildApprovedAmountByGroupChartAsync(connection, filters, chartId, "CondiÃ§Ã£o de pagamento x aprovaÃ§Ã£o", "CONDPAG", "Condicao"),
                "payment_by_seller" => await BuildTopByGroupChartAsync(connection, filters, chartId, "CondiÃ§Ã£o de pagamento x vendedor", "CONDPAG", "Condicao", "VENDEDOR", "Vendedor", 12),
                "payment_by_customer" => await BuildTopByGroupChartAsync(connection, filters, chartId, "CondiÃ§Ã£o de pagamento x cliente", "CONDPAG", "Condicao", "CLIENTE", "Cliente", 12),
                "payment_by_origin" => await BuildTopByGroupChartAsync(connection, filters, chartId, "CondiÃ§Ã£o de pagamento x origem", "CONDPAG", "Condicao", "ORIGEM", "Origem", 12),
                "payment_by_product" => await BuildTopCrossChartAsync(connection, filters, chartId, "CondiÃ§Ã£o de pagamento x produto", "O.CONDPAG", "Condicao", "I.ITEM", "Produto", 12),

                "freight_total_amount" => await BuildSingleValueChartAsync(connection, filters, chartId, "Valor total de frete", "kpi", "SELECT ISNULL(SUM(VALORFRETE), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Frete total", "currency"),
                "freight_avg_per_budget" => await BuildAverageFreightPerBudgetChartAsync(connection, filters, chartId),
                "freight_by_type" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Frete por tipo de frete", "pie", "TIPOFRETE", "Tipo de frete", "SUM(VALORFRETE)", "currency"),
                "freight_most_used_type" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Tipos de frete mais usados", "pie", "TIPOFRETE", "Tipo de frete", DistinctBudgetCountSql(), "number", null, 10),
                "freight_avg_ticket_by_type" => await BuildAverageTicketByGroupChartAsync(connection, filters, chartId, "Ticket mÃ©dio por tipo de frete", "TIPOFRETE", "Tipo de frete", 10),
                "freight_ratio_total" => await BuildFreightRatioChartAsync(connection, filters, chartId),
                "freight_vs_conversion" => await BuildConversionByFreightTypeChartAsync(connection, filters, chartId),
                "freight_by_seller" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Frete x vendedor", "TIPOFRETE", "Tipo de frete", "VENDEDOR", "Vendedor", 12),
                "freight_by_customer" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Frete x cliente", "TIPOFRETE", "Tipo de frete", "CLIENTE", "Cliente", 12),
                "freight_by_geo" => await BuildTopByGroupChartAsync(connection, filters, chartId, "Frete x UF", "TIPOFRETE", "Tipo de frete", "UF", "UF", 12),
                "freight_high_budgets" => await BuildHighestFreightBudgetsChartAsync(connection, filters, chartId, 10),

                "exec_dashboard" => await BuildExecutiveDashboardChartAsync(connection, filters, chartId),
                "exec_open_pipeline" => await BuildOpenPipelineChartAsync(connection, filters, chartId),
                "exec_total_revenue_budget" => await BuildMonthlyAmountSeriesChartAsync(connection, filters, chartId, "Receita/orÃ§amento total por perÃ­odo"),
                "exec_opportunity_ranking" => await BuildBudgetRankingChartAsync(connection, filters, chartId, "Ranking de oportunidades", OpenStatusCondition(), 10),
                "exec_monthly_growth" => await BuildMonthlyGrowthChartAsync(connection, filters, chartId),
                "exec_seller_share" => await BuildShareByGroupChartAsync(connection, filters, chartId, "ParticipaÃ§Ã£o por vendedor", "VENDEDOR", "vendedor", 12),
                "exec_customer_share" => await BuildShareByGroupChartAsync(connection, filters, chartId, "ParticipaÃ§Ã£o por cliente", "CLIENTE", "cliente", 12),
                "exec_product_share" => await BuildShareByItemChartAsync(connection, filters, chartId, 12, "ParticipaÃ§Ã£o por produto"),
                "exec_region_share" => await BuildShareByGroupChartAsync(connection, filters, chartId, "ParticipaÃ§Ã£o por UF", "UF", "uf", 27),
                "exec_origin_share" => await BuildShareByGroupChartAsync(connection, filters, chartId, "ParticipaÃ§Ã£o por origem", "ORIGEM", "origem", 12),
                "exec_avg_markup" => await BuildSingleValueChartAsync(connection, filters, chartId, "Margem/markup mÃ©dio consolidado", "kpi", "SELECT ISNULL(AVG(MARKUP), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Markup mÃ©dio", "number"),
                "exec_total_discount" => await BuildSingleValueChartAsync(connection, filters, chartId, "Desconto total concedido", "kpi", "SELECT ISNULL(SUM(VALORDESCONTO), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Desconto total", "currency"),
                "exec_lost_opportunities" => await BuildBudgetRankingChartAsync(connection, filters, chartId, "Oportunidades perdidas", LostStatusCondition(), 10),
                "exec_negotiation_opportunities" => await BuildBudgetRankingChartAsync(connection, filters, chartId, "Oportunidades em negociaÃ§Ã£o", NegotiationStatusCondition(), 10),
                "exec_strategic_customers" => await BuildStrategicRankingByGroupChartAsync(connection, filters, chartId, "Clientes estratÃ©gicos", "CLIENTE", 10),
                "exec_strategic_products" => await BuildStrategicRankingByItemChartAsync(connection, filters, chartId, "Produtos estratÃ©gicos", 10),
                "exec_strategic_channels" => await BuildStrategicRankingByGroupChartAsync(connection, filters, chartId, "Canais estratÃ©gicos", "ORIGEM", 10),
                "exec_sales_drop_alerts" => await BuildSalesDropAlertsChartAsync(connection, filters, chartId, 10),
                "exec_goal_vs_actual" => BuildPlannedChart(chartId, "Meta x realizado"),
                "exec_sales_forecast" => BuildPlannedChart(chartId, "Forecast de vendas"),

                "future_budget_vs_sold" => await BuildBudgetVsSoldChartAsync(connection, filters, chartId),
                "future_budget_converted_to_order" => await BuildBudgetConvertedToOrderChartAsync(connection, filters, chartId),
                "future_avg_conversion_time" => await BuildAverageJourneyDaysKpiChartAsync(connection, filters, chartId, "Tempo médio de conversão", "DIAS_ATE_PRIMEIRO_PEDIDO"),
                "future_issue_to_approval_time" => await BuildAverageJourneyDaysKpiChartAsync(connection, filters, chartId, "Tempo médio entre emissão e aprovação (pedido gerado)", "DIAS_ATE_PRIMEIRO_PEDIDO"),

                "velocity_avg_cycle_time" => await BuildAverageJourneyDaysKpiChartAsync(connection, filters, chartId, "Tempo médio do ciclo de vendas", "DIAS_ATE_PRIMEIRO_PEDIDO"),
                "velocity_by_seller" => await BuildVelocityBySellerChartAsync(connection, filters, chartId),
                "velocity_by_product" => await BuildVelocityByProductChartAsync(connection, filters, chartId),
                "velocity_conversion_acceleration" => await BuildVelocityConversionAccelerationChartAsync(connection, filters, chartId),
                "velocity_status_bottleneck_time" => BuildPlannedChart(chartId, "Tempo médio de permanência em status"),

                "insight_pending_followup_budgets" => await BuildPendingFollowupChartAsync(connection, filters, chartId),
                "insight_high_value_no_return" => await BuildHighValueNoReturnChartAsync(connection, filters, chartId),
                "insight_seller_vs_team_avg" => await BuildSellerVsTeamChartAsync(connection, filters, chartId),
                "insight_repurchase_potential_customers" => await BuildRepurchasePotentialChartAsync(connection, filters, chartId),
                "insight_customers_high_conversion_chance" => await BuildCustomersHighConversionChanceChartAsync(connection, filters, chartId, 12),
                "insight_frequent_customers" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Clientes que compram/orÃ§am com frequÃªncia", "bar", "CLIENTE", "Cliente", DistinctBudgetCountSql(), "number", null, 12),
                "insight_customers_stopped_quoting" => await BuildCustomersStoppedQuotingChartAsync(connection, filters, chartId, 12),
                "insight_recommended_products_by_customer" => await BuildRecommendedProductsByCustomerChartAsync(connection, filters, chartId, 12),
                "insight_top_products_by_region" => await BuildTopProductsByRegionChartAsync(connection, filters, chartId, 12),
                "insight_high_acceptance_products" => await BuildHighAcceptanceProductsChartAsync(connection, filters, chartId, 12),
                "insight_old_open_budgets" => await BuildOldOpenBudgetsChartAsync(connection, filters, chartId, 10),
                "insight_high_ticket_customers" => await BuildAverageTicketByGroupChartAsync(connection, filters, chartId, "Clientes com alto ticket mÃ©dio", "CLIENTE", "Cliente", 12),
                "insight_discount_sensitive_customers" => await BuildDiscountSensitiveCustomersChartAsync(connection, filters, chartId, 12),
                "insight_low_discount_customers" => await BuildLowDiscountCustomersChartAsync(connection, filters, chartId, 12),
                "insight_best_origin_by_seller" => await BuildBestOriginBySellerChartAsync(connection, filters, chartId, 12),
                "insight_best_product_by_seller" => await BuildBestProductBySellerChartAsync(connection, filters, chartId, 12),
                "insight_best_region_by_seller" => await BuildBestRegionBySellerChartAsync(connection, filters, chartId, 12),
                "insight_personal_seller_ranking" => await BuildPersonalSellerRankingChartAsync(connection, filters, chartId, 12),
                "insight_individual_monthly_evolution" => await BuildIndividualMonthlyEvolutionChartAsync(connection, filters, chartId),
                "insight_underused_products_by_seller" => await BuildUnderusedProductsBySellerChartAsync(connection, filters, chartId, 12),

                "kpi_total_budget_amount" => await BuildSingleValueChartAsync(connection, filters, chartId, "Valor total orÃ§ado", "kpi", "SELECT ISNULL(SUM(VALORTOTAL), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Valor total", "currency"),
                "kpi_budget_count" => await BuildSingleValueChartAsync(connection, filters, chartId, "Quantidade de orÃ§amentos", "kpi", $"SELECT {DistinctBudgetCountSql()} AS Valor FROM VW_SWIA_ORCAMENTO {{0}}", "Quantidade", "number"),
                "kpi_avg_ticket" => await BuildAverageTicketKpiChartAsync(connection, filters, chartId, "Ticket mÃ©dio"),
                "kpi_conversion_rate" => await BuildConversionKpiChartAsync(connection, filters, chartId, "Taxa de conversÃ£o"),
                "kpi_open_amount" => await BuildStatusAmountKpiChartAsync(connection, filters, chartId, "Valor em aberto", OpenStatusCondition()),
                "kpi_approved_amount" => await BuildStatusAmountKpiChartAsync(connection, filters, chartId, "Valor aprovado", ApprovedStatusCondition()),
                "kpi_lost_amount" => await BuildStatusAmountKpiChartAsync(connection, filters, chartId, "Valor perdido", LostStatusCondition()),
                "kpi_best_seller" => await BuildTextKpiChartAsync(connection, filters, chartId, "Melhor vendedor", "VENDEDOR"),
                "kpi_best_customer" => await BuildTextKpiChartAsync(connection, filters, chartId, "Melhor cliente", "CLIENTE"),
                "kpi_best_product" => await BuildBestItemKpiChartAsync(connection, filters, chartId, "Melhor produto"),
                "kpi_best_city" => await BuildTextKpiChartAsync(connection, filters, chartId, "Melhor cidade", "CIDADE"),
                "kpi_best_uf" => await BuildTextKpiChartAsync(connection, filters, chartId, "Melhor UF", "UF"),
                "kpi_best_origin" => await BuildTextKpiChartAsync(connection, filters, chartId, "Melhor origem", "ORIGEM"),
                "kpi_highest_discount" => await BuildHighestDiscountKpiChartAsync(connection, filters, chartId),
                "kpi_avg_discount" => await BuildSinglePercentKpiChartAsync(connection, filters, chartId, "Desconto mÃ©dio", "PERCENTUALDESCONTO"),
                "kpi_avg_markup" => await BuildSingleValueChartAsync(connection, filters, chartId, "Markup mÃ©dio", "kpi", "SELECT ISNULL(AVG(MARKUP), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Markup mÃ©dio", "number"),
                "kpi_avg_freight" => await BuildSingleValueChartAsync(connection, filters, chartId, "Frete mÃ©dio", "kpi", "SELECT ISNULL(AVG(VALORFRETE), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Frete mÃ©dio", "currency"),
                "kpi_most_quoted_product" => await BuildMostQuotedProductKpiChartAsync(connection, filters, chartId),
                "kpi_highest_potential_customer" => await BuildHighestPotentialCustomerKpiChartAsync(connection, filters, chartId),
                "kpi_seller_highest_growth" => await BuildHighestDeltaByGroupKpiChartAsync(connection, filters, chartId, "Vendedor com maior crescimento", "VENDEDOR", "seller", "growth"),
                "kpi_seller_highest_drop" => await BuildHighestDeltaByGroupKpiChartAsync(connection, filters, chartId, "Vendedor com maior queda", "VENDEDOR", "seller", "drop"),
                "kpi_product_highest_growth" => await BuildHighestDeltaByItemKpiChartAsync(connection, filters, chartId, "Produto com maior crescimento", "growth"),
                "kpi_product_highest_drop" => await BuildHighestDeltaByItemKpiChartAsync(connection, filters, chartId, "Produto com maior queda", "drop"),
                "kpi_channel_highest_conversion" => await BuildBestConversionOriginKpiTextChartAsync(connection, filters, chartId, "Canal com maior conversÃ£o", "highest"),
                "kpi_channel_lowest_conversion" => await BuildBestConversionOriginKpiTextChartAsync(connection, filters, chartId, "Canal com menor conversÃ£o", "lowest"),

                // Risk
                "risk_customer_concentration" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "ConcentraÃ§Ã£o nos Top Clientes", "pie", "CLIENTE", "Cliente", "SUM(VALORTOTAL)", "currency", null, 10),
                "risk_product_dependence" => await BuildGroupedItemChartAsync(connection, filters, chartId, "DependÃªncia de produtos", "bar", "I.ITEM", "Produto", "SUM(I.VALORTOTAL)", "currency", 10),
                "risk_seller_concentration" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "ConcentraÃ§Ã£o em vendedores", "pie", "VENDEDOR", "Vendedor", "SUM(VALORTOTAL)", "currency", null, 10),
                "risk_geo_concentration" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "ConcentraÃ§Ã£o de risco por regiÃ£o", "bar", "UF", "UF", "SUM(VALORTOTAL)", "currency", null, 10),
                "risk_high_discount_volume" => await BuildHighDiscountVolumeChartAsync(connection, filters, chartId),

                // Efficiency
                "efficiency_win_rate_vs_time" => await BuildWinRateVsTimeChartAsync(connection, filters, chartId),
                "efficiency_quote_to_close_ratio" => await BuildQuoteToCloseRatioChartAsync(connection, filters, chartId),
                "efficiency_abandonment_rate" => await BuildAbandonmentRateChartAsync(connection, filters, chartId),
                "efficiency_avg_items_per_ticket" => await BuildAvgItemsPerTicketChartAsync(connection, filters, chartId),

                // Predictive
                "predictive_sales_forecast" => await BuildSalesForecastChartAsync(connection, filters, chartId),
                "predictive_churn_risk" => await BuildChurnRiskChartAsync(connection, filters, chartId),
                "predictive_seasonal_trend" => await BuildMonthSeasonalityChartAsync(connection, filters, chartId),
                "predictive_high_probability_deals" => await BuildHighProbabilityDealsChartAsync(connection, filters, chartId),

                _ => BuildPlannedChart(chartId, chartId)
            };
        }

        private static SalesBudgetChartDatasetDto BuildPlannedChart(string chartId, string title)
        {
            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "planned",
                Meta = new SalesBudgetChartMetaDto
                {
                    Source = "future",
                    Warnings = new List<string> { "Grafico ainda não implementado neste batch inicial." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildSingleValueChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string visualization,
            string sqlTemplate,
            string label,
            string metricKind)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = string.Format(sqlTemplate, where);
            var value = await connection.ExecuteScalarAsync<decimal>(sql, parameters);

            SalesBudgetChartPointDto point = metricKind switch
            {
                "number" => new SalesBudgetChartPointDto { Label = label, Value = value, Count = value },
                "percentage" => new SalesBudgetChartPointDto { Label = label, Value = value, Percentage = value },
                _ => new SalesBudgetChartPointDto { Label = label, Value = value, Amount = value }
            };

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = visualization,
                Data = new List<SalesBudgetChartPointDto>
                {
                    point
                },
                Totals = new Dictionary<string, decimal> { ["total"] = value },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAverageTicketChartAsync(IDbConnection connection, SalesBudgetFilterDto filters)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"SELECT 
                            ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                            {DistinctBudgetCountSql()} AS TotalOrçamentos
                         FROM VW_SWIA_ORCAMENTO {where}";

            var row = await connection.QuerySingleAsync(sql, parameters);
            var totalValor = SafeToDecimal(row, "TotalValor");
            var totalOrçamentos = Convert.ToInt32(SafeToDecimal(row, "TotalOrçamentos"));
            var avgTicket = totalOrçamentos > 0 ? totalValor / totalOrçamentos : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = "overview_avg_ticket",
                Title = "Ticket mÃ©dio dos orÃ§amentos",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Ticket médio", Value = avgTicket, Amount = avgTicket }
                },
                Totals = new Dictionary<string, decimal> { ["avgTicket"] = avgTicket },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildBudgetVsSoldChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "EMISSAO_ORCAMENTO");
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    ISNULL(SUM(VALOR_ORCADO), 0) AS TotalOrçado,
                    ISNULL(SUM(VALOR_PEDIDO), 0) AS TotalPedido,
                    ISNULL(SUM(VALOR_FATURADO), 0) AS TotalFaturado
                FROM VW_SWIA_ORCAMENTO_JORNADA
                {where}";

            var row = await connection.QuerySingleAsync(sql, parameters);
            var totalOrçado = SafeToDecimal(row, "TotalOrçado");
            var totalPedido = SafeToDecimal(row, "TotalPedido");
            var totalFaturado = SafeToDecimal(row, "TotalFaturado");

            var points = new List<SalesBudgetChartPointDto>
            {
                new() { Label = "Orçado", Value = totalOrçado, Amount = totalOrçado },
                new() { Label = "Pedido", Value = totalPedido, Amount = totalPedido },
                new() { Label = "Faturado", Value = totalFaturado, Amount = totalFaturado },
            };

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Orçado x vendido/faturado",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["budgetAmount"] = totalOrçado,
                    ["orderAmount"] = totalPedido,
                    ["invoicedAmount"] = totalFaturado,
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Source = "journey",
                    DateField = "EMISSAO_ORCAMENTO",
                    Warnings = new List<string>
                    {
                        "Usa a view VW_SWIA_ORCAMENTO_JORNADA para comparar valores orçados, pedidos e faturados no período."
                    }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildBudgetConvertedToOrderChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "EMISSAO_ORCAMENTO");
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    COUNT(*) AS TotalOrçamentos,
                    ISNULL(SUM(CASE WHEN CONVERTEU_EM_PEDIDO = 1 THEN 1 ELSE 0 END), 0) AS OrçamentosConvertidos
                FROM VW_SWIA_ORCAMENTO_JORNADA
                {where}";

            var row = await connection.QuerySingleAsync(sql, parameters);
            var totalOrçamentos = SafeToDecimal(row, "TotalOrçamentos");
            var convertidos = SafeToDecimal(row, "OrçamentosConvertidos");
            var taxa = totalOrçamentos > 0 ? convertidos / totalOrçamentos : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Orçamento convertido em pedido",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Conversão em pedido", Value = taxa, Percentage = taxa, Count = convertidos }
                },
                Totals = new Dictionary<string, decimal>
                {
                    ["convertedCount"] = convertidos,
                    ["totalCount"] = totalOrçamentos,
                    ["conversionRate"] = taxa,
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Source = "journey",
                    DateField = "EMISSAO_ORCAMENTO",
                    Warnings = new List<string>
                    {
                        "Considera convertido quando CONVERTEU_EM_PEDIDO = 1 na view de jornada."
                    }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAverageJourneyDaysKpiChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string daysColumn)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "EMISSAO_ORCAMENTO");
            conditions.Add($"{daysColumn} IS NOT NULL");
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    ISNULL(AVG(CAST({daysColumn} AS decimal(18, 4))), 0) AS Valor,
                    COUNT(*) AS TotalConvertidos
                FROM VW_SWIA_ORCAMENTO_JORNADA
                {where}";

            var row = await connection.QuerySingleAsync(sql, parameters);
            var value = SafeToDecimal(row, "Valor");
            var totalConvertidos = SafeToDecimal(row, "TotalConvertidos");

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Dias médios", Value = value, Count = totalConvertidos }
                },
                Totals = new Dictionary<string, decimal>
                {
                    ["avgDays"] = value,
                    ["convertedCount"] = totalConvertidos,
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Source = "journey",
                    DateField = "EMISSAO_ORCAMENTO",
                    Warnings = new List<string>
                    {
                        $"Calculado com base em {daysColumn} da view VW_SWIA_ORCAMENTO_JORNADA."
                    }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildVelocityBySellerChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12,
            int minConverted = 2)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "EMISSAO_ORCAMENTO");
            conditions.Add("DIAS_ATE_PRIMEIRO_PEDIDO IS NOT NULL");
            var where = BuildWhere(conditions);
            parameters.Add("Top", top);
            parameters.Add("MinConverted", minConverted);

            var sql = $@"
                SELECT TOP (@Top)
                    ISNULL(NULLIF(VENDEDOR, ''), 'Sem vendedor') AS Label,
                    CAST(AVG(CAST(DIAS_ATE_PRIMEIRO_PEDIDO AS decimal(18, 4))) AS decimal(18, 2)) AS AvgDays,
                    COUNT(*) AS ConvertedCount
                FROM VW_SWIA_ORCAMENTO_JORNADA
                {where}
                GROUP BY ISNULL(NULLIF(VENDEDOR, ''), 'Sem vendedor')
                HAVING COUNT(*) >= @MinConverted
                ORDER BY AvgDays ASC, ConvertedCount DESC, Label ASC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Label") ?? "Sem vendedor",
                Value = SafeToDecimal(row, "AvgDays"),
                Count = SafeToDecimal(row, "ConvertedCount"),
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Velocidade de conversão por vendedor",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["sellers"] = points.Count,
                    ["avgDays"] = points.Count > 0 ? points.Average(p => p.Value ?? 0m) : 0m,
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Source = "journey",
                    DateField = "EMISSAO_ORCAMENTO",
                    Warnings = new List<string>
                    {
                        "Menor valor = conversão mais rápida.",
                        $"Considera apenas vendedores com pelo menos {minConverted} orçamentos convertidos."
                    }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildVelocityByProductChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12,
            int minConverted = 2)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "EMISSAO_ORCAMENTO");
            conditions.Add("DIAS_ATE_PRIMEIRO_PEDIDO IS NOT NULL");
            var where = BuildWhere(conditions);
            parameters.Add("Top", top);
            parameters.Add("MinConverted", minConverted);

            var sql = $@"
                SELECT TOP (@Top)
                    ISNULL(NULLIF(ITEM, ''), ISNULL(NULLIF(CODIGOITEM, ''), 'Sem produto')) AS Label,
                    CAST(AVG(CAST(DIAS_ATE_PRIMEIRO_PEDIDO AS decimal(18, 4))) AS decimal(18, 2)) AS AvgDays,
                    COUNT(*) AS ConvertedCount
                FROM VW_SWIA_ORCAMENTO_JORNADA_PRODUTO
                {where}
                GROUP BY ISNULL(NULLIF(ITEM, ''), ISNULL(NULLIF(CODIGOITEM, ''), 'Sem produto'))
                HAVING COUNT(*) >= @MinConverted
                ORDER BY AvgDays ASC, ConvertedCount DESC, Label ASC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Label") ?? "Sem produto",
                Value = SafeToDecimal(row, "AvgDays"),
                Count = SafeToDecimal(row, "ConvertedCount"),
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Velocidade por produto",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["products"] = points.Count,
                    ["avgDays"] = points.Count > 0 ? points.Average(p => p.Value ?? 0m) : 0m,
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Source = "journey_product",
                    DateField = "EMISSAO_ORCAMENTO",
                    Warnings = new List<string>
                    {
                        "Menor valor = conversão mais rápida.",
                        $"Considera apenas produtos com pelo menos {minConverted} orçamentos convertidos.",
                        "A view atual mede velocidade por produto, não por categoria agregada."
                    }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildVelocityConversionAccelerationChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId)
        {
            var endDate = filters?.EndDate?.Date ?? DateTime.UtcNow.Date;
            var startDate = filters?.StartDate?.Date ?? endDate.AddDays(-29);
            if (startDate > endDate)
            {
                (startDate, endDate) = (endDate, startDate);
            }

            var rangeDays = Math.Max(1, (endDate - startDate).Days + 1);
            var previousStart = startDate.AddDays(-rangeDays);
            var previousEnd = startDate.AddDays(-1);

            var parameters = new DynamicParameters();
            parameters.Add("StartDate", startDate);
            parameters.Add("EndDate", endDate);
            parameters.Add("PreviousStart", previousStart);
            parameters.Add("PreviousEnd", previousEnd);

            var sql = @"
                SELECT
                    CAST(ISNULL(AVG(CASE WHEN EMISSAO_ORCAMENTO >= @StartDate AND EMISSAO_ORCAMENTO <= @EndDate THEN CAST(DIAS_ATE_PRIMEIRO_PEDIDO AS decimal(18, 4)) END), 0) AS decimal(18, 2)) AS AvgAtual,
                    CAST(ISNULL(AVG(CASE WHEN EMISSAO_ORCAMENTO >= @PreviousStart AND EMISSAO_ORCAMENTO <= @PreviousEnd THEN CAST(DIAS_ATE_PRIMEIRO_PEDIDO AS decimal(18, 4)) END), 0) AS decimal(18, 2)) AS AvgAnterior
                FROM VW_SWIA_ORCAMENTO_JORNADA
                WHERE DIAS_ATE_PRIMEIRO_PEDIDO IS NOT NULL
                  AND EMISSAO_ORCAMENTO >= @PreviousStart
                  AND EMISSAO_ORCAMENTO <= @EndDate";

            var row = await connection.QuerySingleAsync(sql, parameters);
            var avgAtual = SafeToDecimal(row, "AvgAtual");
            var avgAnterior = SafeToDecimal(row, "AvgAnterior");
            var deltaDias = avgAnterior - avgAtual;
            var improvementRatio = avgAnterior > 0 ? deltaDias / avgAnterior : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Aceleração de conversão",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Aceleracao", Value = improvementRatio, Percentage = improvementRatio, Amount = deltaDias }
                },
                Totals = new Dictionary<string, decimal>
                {
                    ["currentAvgDays"] = avgAtual,
                    ["previousAvgDays"] = avgAnterior,
                    ["deltaDays"] = deltaDias,
                    ["improvementRatio"] = improvementRatio,
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Source = "journey",
                    DateField = "EMISSAO_ORCAMENTO",
                    Warnings = new List<string>
                    {
                        "Positivo = ciclo atual mais rápido que o período anterior equivalente.",
                        "A comparação usa DIAS_ATE_PRIMEIRO_PEDIDO."
                    }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildMonthlyEvolutionChartAsync(IDbConnection connection, SalesBudgetFilterDto filters)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    YEAR(EMISSAO) AS Ano,
                    MONTH(EMISSAO) AS Mes,
                    CONCAT(RIGHT('00' + CAST(MONTH(EMISSAO) AS VARCHAR(2)), 2), '/', CAST(YEAR(EMISSAO) AS VARCHAR(4))) AS MesAno,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY YEAR(EMISSAO), MONTH(EMISSAO)
                ORDER BY YEAR(EMISSAO), MONTH(EMISSAO)";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "MesAno") ?? "-",
                Date = SafeToString(row, "MesAno"),
                Amount = SafeToDecimal(row, "TotalValor"),
                Count = SafeToDecimal(row, "TotalOrçamentos"),
                Value = SafeToDecimal(row, "TotalValor")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = "overview_monthly_evolution",
                Title = "EvoluÃ§Ã£o mensal de orÃ§amentos",
                Visualization = "line",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["totalAmount"] = points.Sum(x => x.Amount ?? 0m),
                    ["totalCount"] = points.Sum(x => x.Count ?? 0m)
                },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildWeeklyEvolutionChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    YEAR(EMISSAO) AS Ano,
                    DATEPART(ISO_WEEK, EMISSAO) AS Semana,
                    CONCAT(CAST(YEAR(EMISSAO) AS VARCHAR(4)), '-W', RIGHT('00' + CAST(DATEPART(ISO_WEEK, EMISSAO) AS VARCHAR(2)), 2)) AS SemanaAno,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY YEAR(EMISSAO), DATEPART(ISO_WEEK, EMISSAO)
                ORDER BY YEAR(EMISSAO), DATEPART(ISO_WEEK, EMISSAO)";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "SemanaAno") ?? "-",
                Date = SafeToString(row, "SemanaAno"),
                Amount = SafeToDecimal(row, "TotalValor"),
                Count = SafeToDecimal(row, "TotalOrçamentos"),
                Value = SafeToDecimal(row, "TotalValor")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "EvoluÃ§Ã£o semanal de orÃ§amentos",
                Visualization = "line",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["totalAmount"] = points.Sum(x => x.Amount ?? 0m),
                    ["totalCount"] = points.Sum(x => x.Count ?? 0m)
                },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildDailyEvolutionChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    CONVERT(date, EMISSAO) AS Dia,
                    CONVERT(VARCHAR(10), CONVERT(date, EMISSAO), 103) AS DiaLabel,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY CONVERT(date, EMISSAO)
                ORDER BY CONVERT(date, EMISSAO)";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "DiaLabel") ?? "-",
                Date = SafeToString(row, "DiaLabel"),
                Amount = SafeToDecimal(row, "TotalValor"),
                Count = SafeToDecimal(row, "TotalOrçamentos"),
                Value = SafeToDecimal(row, "TotalValor")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "EvoluÃ§Ã£o diÃ¡ria de orÃ§amentos",
                Visualization = "line",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["totalAmount"] = points.Sum(x => x.Amount ?? 0m),
                    ["totalCount"] = points.Sum(x => x.Count ?? 0m)
                },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private sealed record PeriodTotals(decimal TotalAmount, int TotalCount)
        {
            public decimal AvgTicket => TotalCount > 0 ? TotalAmount / TotalCount : 0m;
        }

        private async Task<PeriodTotals> QueryPeriodTotalsAsync(IDbConnection connection, DateTime startDate, DateTime endDate)
        {
            var sql = $@"
                SELECT
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                WHERE EMISSAO >= @StartDate AND EMISSAO <= @EndDate";

            var row = await connection.QuerySingleAsync(sql, new
            {
                StartDate = startDate.Date,
                EndDate = endDate.Date
            });

            var totalAmount = SafeToDecimal(row, "TotalValor");
            var totalCount = Convert.ToInt32(SafeToDecimal(row, "TotalOrçamentos"));
            return new PeriodTotals(totalAmount, totalCount);
        }

        private static DateTime ClampDate(DateTime value, DateTime min, DateTime max)
        {
            if (value < min) return min;
            if (value > max) return max;
            return value;
        }

        private async Task<SalesBudgetChartDatasetDto> BuildCurrentVsPreviousMonthChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var referenceEnd = (filters.EndDate?.Date ?? DateTime.Today.Date);
            var currentStart = new DateTime(referenceEnd.Year, referenceEnd.Month, 1);
            var offsetDays = (referenceEnd - currentStart).Days;

            var previousStart = currentStart.AddMonths(-1);
            var previousMonthEnd = previousStart.AddMonths(1).AddDays(-1);
            var previousEndCandidate = previousStart.AddDays(offsetDays);
            var previousEnd = ClampDate(previousEndCandidate, previousStart, previousMonthEnd);

            var current = await QueryPeriodTotalsAsync(connection, currentStart, referenceEnd);
            var previous = await QueryPeriodTotalsAsync(connection, previousStart, previousEnd);

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
	                Title = "Comparativo mÃªs atual x mÃªs anterior",
	                Visualization = "kpi_grid",
	                Data = new List<SalesBudgetChartPointDto>
	                {
	                    new() { Label = "Valor (mÃªs atual)", Amount = current.TotalAmount },
	                    new() { Label = "Qtd. orÃ§amentos (mÃªs atual)", Count = current.TotalCount },
	                    new() { Label = "Ticket mÃ©dio (mÃªs atual)", Amount = current.AvgTicket },
	                    new() { Label = "Valor (mÃªs anterior)", Amount = previous.TotalAmount },
	                    new() { Label = "Qtd. orÃ§amentos (mÃªs anterior)", Count = previous.TotalCount },
	                    new() { Label = "Ticket mÃ©dio (mÃªs anterior)", Amount = previous.AvgTicket },
	                },
                Totals = new Dictionary<string, decimal>
                {
                    ["currentAmount"] = current.TotalAmount,
                    ["currentCount"] = current.TotalCount,
                    ["previousAmount"] = previous.TotalAmount,
                    ["previousCount"] = previous.TotalCount
                },
	                Meta = new SalesBudgetChartMetaDto
	                {
	                    Warnings = new List<string>
	                    {
	                        "Comparativo MTD (mÃªs atual atÃ© a data final do filtro) vs perÃ­odo equivalente no mÃªs anterior."
	                    }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildCurrentYearVsPreviousYearChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var referenceEnd = (filters.EndDate?.Date ?? DateTime.Today.Date);
            var currentStart = new DateTime(referenceEnd.Year, 1, 1);
            var offsetDays = (referenceEnd - currentStart).Days;

            var previousStart = currentStart.AddYears(-1);
            var previousYearEnd = new DateTime(previousStart.Year, 12, 31);
            var previousEndCandidate = previousStart.AddDays(offsetDays);
            var previousEnd = ClampDate(previousEndCandidate, previousStart, previousYearEnd);

            var current = await QueryPeriodTotalsAsync(connection, currentStart, referenceEnd);
            var previous = await QueryPeriodTotalsAsync(connection, previousStart, previousEnd);

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
	                Title = "Comparativo ano atual x ano anterior",
	                Visualization = "kpi_grid",
	                Data = new List<SalesBudgetChartPointDto>
	                {
	                    new() { Label = "Valor (ano atual)", Amount = current.TotalAmount },
	                    new() { Label = "Qtd. orÃ§amentos (ano atual)", Count = current.TotalCount },
	                    new() { Label = "Ticket mÃ©dio (ano atual)", Amount = current.AvgTicket },
	                    new() { Label = "Valor (ano anterior)", Amount = previous.TotalAmount },
	                    new() { Label = "Qtd. orÃ§amentos (ano anterior)", Count = previous.TotalCount },
	                    new() { Label = "Ticket mÃ©dio (ano anterior)", Amount = previous.AvgTicket },
	                },
                Totals = new Dictionary<string, decimal>
                {
                    ["currentAmount"] = current.TotalAmount,
                    ["currentCount"] = current.TotalCount,
                    ["previousAmount"] = previous.TotalAmount,
                    ["previousCount"] = previous.TotalCount
                },
	                Meta = new SalesBudgetChartMetaDto
	                {
	                    Warnings = new List<string>
	                    {
	                        "Comparativo YTD (ano atual atÃ© a data final do filtro) vs perÃ­odo equivalente no ano anterior."
	                    }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildTopDaysByVolumeChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    CONVERT(VARCHAR(10), CONVERT(date, EMISSAO), 103) AS DiaLabel,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY CONVERT(date, EMISSAO)
                ORDER BY TotalOrçamentos DESC, TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "DiaLabel") ?? "-",
                Amount = SafeToDecimal(row, "TotalValor"),
                Value = SafeToDecimal(row, "TotalValor"),
                Count = SafeToDecimal(row, "TotalOrçamentos")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Top dias com maior volume de orÃ§amentos",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["totalAmount"] = points.Sum(x => x.Amount ?? 0m),
                    ["totalCount"] = points.Sum(x => x.Count ?? 0m)
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Ordenado por quantidade (desempate por valor)." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildTopMonthsByAmountChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    YEAR(EMISSAO) AS Ano,
                    MONTH(EMISSAO) AS Mes,
                    CONCAT(RIGHT('00' + CAST(MONTH(EMISSAO) AS VARCHAR(2)), 2), '/', CAST(YEAR(EMISSAO) AS VARCHAR(4))) AS MesAno,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY YEAR(EMISSAO), MONTH(EMISSAO)
                ORDER BY TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "MesAno") ?? "-",
                Amount = SafeToDecimal(row, "TotalValor"),
                Value = SafeToDecimal(row, "TotalValor")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Top meses com maior valor orçado",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["totalAmount"] = points.Sum(x => x.Amount ?? 0m) },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildMonthSeasonalityChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    MONTH(EMISSAO) AS MesNumero,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY MONTH(EMISSAO)
                ORDER BY MONTH(EMISSAO)";

            var monthMap = new Dictionary<int, string>
            {
                [1] = "Jan",
                [2] = "Fev",
                [3] = "Mar",
                [4] = "Abr",
                [5] = "Mai",
                [6] = "Jun",
                [7] = "Jul",
                [8] = "Ago",
                [9] = "Set",
                [10] = "Out",
                [11] = "Nov",
                [12] = "Dez"
            };

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var monthNumber = Convert.ToInt32(SafeToDecimal(row, "MesNumero"));
                var label = monthMap.TryGetValue(monthNumber, out string shortLabel) ? shortLabel : monthNumber.ToString();
                var totalAmount = SafeToDecimal(row, "TotalValor");
                var totalCount = SafeToDecimal(row, "TotalOrçamentos");
                return new SalesBudgetChartPointDto
                {
                    Label = label,
                    Value = totalAmount,
                    Amount = totalAmount,
                    Count = totalCount
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Sazonalidade de vendas/orÃ§amentos por mÃªs",
                Visualization = "heatmap",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["totalAmount"] = points.Sum(x => x.Amount.GetValueOrDefault()),
                    ["totalCount"] = points.Sum(x => x.Count.GetValueOrDefault())
                },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildMonthYearHeatmapChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    YEAR(EMISSAO) AS Ano,
                    MONTH(EMISSAO) AS Mes,
                    CONCAT(RIGHT('00' + CAST(MONTH(EMISSAO) AS VARCHAR(2)), 2), '/', CAST(YEAR(EMISSAO) AS VARCHAR(4))) AS MesAno,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY YEAR(EMISSAO), MONTH(EMISSAO)
                ORDER BY YEAR(EMISSAO), MONTH(EMISSAO)";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var total = SafeToDecimal(row, "TotalValor");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "MesAno") ?? "-",
                    Value = total,
                    Amount = total
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Mapa de calor de orÃ§amentos por mÃªs e ano",
                Visualization = "heatmap",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["totalAmount"] = points.Sum(x => x.Amount ?? 0m) },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildWeekdayHeatmapChartAsync(IDbConnection connection, SalesBudgetFilterDto filters)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT
                    DATEPART(WEEKDAY, EMISSAO) AS DiaSemanaNumero,
                    DATENAME(WEEKDAY, EMISSAO) AS DiaSemana,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY DATEPART(WEEKDAY, EMISSAO), DATENAME(WEEKDAY, EMISSAO)
                ORDER BY DiaSemanaNumero";

            var rows = await connection.QueryAsync(sql, parameters);
            var map = new Dictionary<int, string>
            {
                [1] = "Dom",
                [2] = "Seg",
                [3] = "Ter",
                [4] = "Qua",
                [5] = "Qui",
                [6] = "Sex",
                [7] = "Sab"
            };

            var points = rows.Select(row =>
            {
                var index = Convert.ToInt32(SafeToDecimal(row, "DiaSemanaNumero"));
                var label = map.TryGetValue(index, out string? shortLabel) ? shortLabel : (SafeToString(row, "DiaSemana") ?? "-");
                var total = SafeToDecimal(row, "TotalValor");
                return new SalesBudgetChartPointDto
                {
                    Label = label,
                    Value = total,
                    Amount = total,
                    Count = 1,
                    Percentage = total
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = "overview_weekday_heatmap",
                Title = "Mapa de calor por dia da semana",
                Visualization = "heatmap",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildGroupedHeaderChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string visualization,
            string groupColumn,
            string labelAlias,
            string metricSql,
            string metricKind,
            string? extraCondition = null,
            int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            if (!string.IsNullOrWhiteSpace(extraCondition))
            {
                conditions.Add(extraCondition);
            }

            var where = BuildWhere(conditions);
            var isStatusGrouping = string.Equals(groupColumn, "STATUS", StringComparison.OrdinalIgnoreCase);
            var groupExpression = isStatusGrouping
                ? CanonicalStatusLabelExpression()
                : $"ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação')";
            var sortSelect = isStatusGrouping
                ? $", MIN({StatusOrderExpression()}) AS SortOrder"
                : string.Empty;
            var orderBy = isStatusGrouping
                ? "SortOrder ASC, Valor DESC"
                : "Valor DESC";
            var sql = $@"
                SELECT TOP {top}
                    {groupExpression} AS Grupo,
                    {metricSql} AS Valor
                    {sortSelect}
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupExpression}
                ORDER BY {orderBy}";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var value = SafeToDecimal(row, "Valor");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informação",
                    Value = value,
                    Amount = metricKind == "currency" ? value : null,
                    Count = metricKind == "number" ? value : null
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = visualization,
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = metricKind == "number" ? new List<string> { $"{labelAlias} ordenado por quantidade." } : new List<string>()
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildGroupedItemChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string visualization,
            string groupColumn,
            string labelAlias,
            string metricSql,
            string metricKind,
            int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    {metricSql} AS Valor
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                ORDER BY Valor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var value = SafeToDecimal(row, "Valor");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informação",
                    Value = value,
                    Amount = metricKind == "currency" ? value : null,
                    Count = metricKind == "number" ? value : null
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = visualization,
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildConversionKpiChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, string title)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var totalSql = $"SELECT {DistinctBudgetCountSql()} AS Valor FROM VW_SWIA_ORCAMENTO {where}";
            var approvedSql = $"SELECT {DistinctBudgetCountSql()} AS Valor FROM VW_SWIA_ORCAMENTO {AppendCondition(where, ApprovedStatusCondition())}";

            var total = await connection.ExecuteScalarAsync<int>(totalSql, parameters);
            var approved = await connection.ExecuteScalarAsync<int>(approvedSql, parameters);
            var ratio = total > 0 ? (decimal)approved / total : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Conversão", Value = ratio, Percentage = ratio }
                },
                Totals = new Dictionary<string, decimal>
                {
                    ["approved"] = approved,
                    ["total"] = total,
                    ["conversionRate"] = ratio
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "A taxa depende do mapeamento atual de STATUS." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildLostCancelRateChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var totalSql = $"SELECT {DistinctBudgetCountSql()} AS Valor FROM VW_SWIA_ORCAMENTO {where}";
            var lostSql = $"SELECT {DistinctBudgetCountSql()} AS Valor FROM VW_SWIA_ORCAMENTO {AppendCondition(where, LostStatusCondition())}";

            var total = await connection.ExecuteScalarAsync<int>(totalSql, parameters);
            var lost = await connection.ExecuteScalarAsync<int>(lostSql, parameters);
            var ratio = total > 0 ? (decimal)lost / total : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Taxa de perda/cancelamento de orÃ§amentos",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Perdas", Value = ratio, Percentage = ratio }
                },
                Totals = new Dictionary<string, decimal>
                {
                    ["lost"] = lost,
                    ["total"] = total,
                    ["lostRate"] = ratio
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "A taxa depende do mapeamento atual de STATUS." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildConversionEvolutionChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    YEAR(EMISSAO) AS Ano,
                    MONTH(EMISSAO) AS Mes,
                    CONCAT(RIGHT('00' + CAST(MONTH(EMISSAO) AS VARCHAR(2)), 2), '/', CAST(YEAR(EMISSAO) AS VARCHAR(4))) AS MesAno,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos,
                    SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY YEAR(EMISSAO), MONTH(EMISSAO)
                ORDER BY YEAR(EMISSAO), MONTH(EMISSAO)";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var total = SafeToDecimal(row, "TotalOrçamentos");
                var approved = SafeToDecimal(row, "Aprovados");
                var ratio = total > 0 ? approved / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "MesAno") ?? "-",
                    Date = SafeToString(row, "MesAno"),
                    Value = ratio,
                    Percentage = ratio,
                    Count = total
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
	                Title = "EvoluÃ§Ã£o da conversÃ£o ao longo do tempo",
                Visualization = "line",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["avgConversion"] = points.Count > 0 ? points.Average(x => x.Value ?? 0m) : 0m
                },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "ConversÃ£o mensal aproximada com base no STATUS atual." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildStatusParticipationChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 15)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var totalSql = $"SELECT {DistinctBudgetCountSql()} AS Valor FROM VW_SWIA_ORCAMENTO {where}";
            var total = await connection.ExecuteScalarAsync<int>(totalSql, parameters);
            var statusLabelExpression = CanonicalStatusLabelExpression();
            var statusOrderExpression = StatusOrderExpression();

            var sql = $@"
                SELECT TOP {top}
                    {statusLabelExpression} AS StatusLabel,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    MIN({statusOrderExpression}) AS SortOrder
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {statusLabelExpression}
                HAVING {DistinctBudgetCountSql()} > 0
                ORDER BY SortOrder, TotalOrçamentos DESC, TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            decimal cumulative = 0m;
            var points = rows.Select(row =>
            {
                var label = SafeToString(row, "StatusLabel") ?? "Sem status";
                var count = Convert.ToInt32(SafeToDecimal(row, "TotalOrçamentos"));
                var pct = total > 0 ? (decimal)count / total : 0m;
                cumulative += pct;

                return new SalesBudgetChartPointDto
                {
                    Label = $"{label} (cum {cumulative:P0})",
                    Value = pct,
                    Percentage = pct,
                    Count = count
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
	                Title = "Percentual de conversÃ£o por status",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["totalBudgets"] = total,
                    ["shownStatuses"] = points.Count
                },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "Leitura inicial: percentual de participaÃ§Ã£o (por quantidade) em cada STATUS; o acumulado aparece no label." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildOpenApprovedLostChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    ISNULL(SUM(CASE WHEN {OpenStatusCondition()} THEN VALORTOTAL ELSE 0 END), 0) AS OpenAmount,
                    ISNULL(SUM(CASE WHEN {ApprovedStatusCondition()} THEN VALORTOTAL ELSE 0 END), 0) AS ApprovedAmount,
                    ISNULL(SUM(CASE WHEN {LostStatusCondition()} THEN VALORTOTAL ELSE 0 END), 0) AS LostAmount,
                    SUM(CASE WHEN {OpenStatusCondition()} THEN 1 ELSE 0 END) AS OpenCount,
                    SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS ApprovedCount,
                    SUM(CASE WHEN {LostStatusCondition()} THEN 1 ELSE 0 END) AS LostCount
                FROM VW_SWIA_ORCAMENTO
                {where}";

            var row = await connection.QuerySingleAsync(sql, parameters);
            var openAmount = SafeToDecimal(row, "OpenAmount");
            var approvedAmount = SafeToDecimal(row, "ApprovedAmount");
            var lostAmount = SafeToDecimal(row, "LostAmount");
            var openCount = SafeToDecimal(row, "OpenCount");
            var approvedCount = SafeToDecimal(row, "ApprovedCount");
            var lostCount = SafeToDecimal(row, "LostCount");

            var points = new List<SalesBudgetChartPointDto>
            {
                new() { Label = "Projeto/Aberto/Parcial", Value = openAmount, Amount = openAmount, Count = openCount },
                new() { Label = "Fechado/Pedido", Value = approvedAmount, Amount = approvedAmount, Count = approvedCount },
                new() { Label = "Perdeu", Value = lostAmount, Amount = lostAmount, Count = lostCount },
            };

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "OrÃ§amentos em aberto x aprovados x perdidos",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["openAmount"] = openAmount,
                    ["approvedAmount"] = approvedAmount,
                    ["lostAmount"] = lostAmount
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Resumo consolidado em tres grupos: Projeto/Aberto/Parcial, Fechado/Pedido e Perdeu." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildOpenPipelineLikeChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, string title)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            conditions.Add(OpenStatusCondition());
            var where = BuildWhere(conditions);
            var sql = $@"SELECT ISNULL(SUM(VALORTOTAL), 0) AS Valor FROM VW_SWIA_ORCAMENTO {where}";
            var value = await connection.ExecuteScalarAsync<decimal>(sql, parameters);

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Valor em aberto", Value = value, Amount = value }
                },
                Totals = new Dictionary<string, decimal> { ["openAmount"] = value },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Considera os status Projeto, Aberto e Parcial como pipeline em aberto." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildNewCustomersChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var endDate = filters.EndDate?.Date ?? DateTime.Today.Date;
            var startDate = filters.StartDate?.Date ?? endDate.AddMonths(-6);

            var sql = $@"
                WITH first_budget AS (
                    SELECT
                        CLIENTE,
                        MIN(EMISSAO) AS FirstEmissão
                    FROM VW_SWIA_ORCAMENTO
                    WHERE CLIENTE IS NOT NULL
                    GROUP BY CLIENTE
                )
                SELECT
                    YEAR(FirstEmissão) AS Ano,
                    MONTH(FirstEmissão) AS Mes,
                    CONCAT(RIGHT('00' + CAST(MONTH(FirstEmissão) AS VARCHAR(2)), 2), '/', CAST(YEAR(FirstEmissão) AS VARCHAR(4))) AS MesAno,
                    COUNT(*) AS NovosClientes
                FROM first_budget
                WHERE FirstEmissão >= @StartDate AND FirstEmissão <= @EndDate
                GROUP BY YEAR(FirstEmissão), MONTH(FirstEmissão)
                ORDER BY YEAR(FirstEmissão), MONTH(FirstEmissão)";

            var rows = await connection.QueryAsync(sql, new { StartDate = startDate, EndDate = endDate });
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "MesAno") ?? "-",
                Date = SafeToString(row, "MesAno"),
                Value = SafeToDecimal(row, "NovosClientes"),
                Count = SafeToDecimal(row, "NovosClientes"),
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Clientes novos por período",
                Visualization = "line",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["newCustomers"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Conta clientes cuja primeira emissão ocorreu dentro do período selecionado." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildInactiveCustomersChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int daysWithoutBudget = 30, int top = 12)
        {
            var endDate = filters.EndDate?.Date ?? DateTime.Today.Date;
            var startDate = filters.StartDate?.Date ?? endDate.AddMonths(-6);
            var referenceDate = endDate.AddDays(-daysWithoutBudget);

            var sql = $@"
                SELECT TOP {top}
                    CLIENTE,
                    MAX(EMISSAO) AS UltimaEmissão,
                    {DistinctBudgetCountSql()} AS OrçamentosNoPeríodo
                FROM VW_SWIA_ORCAMENTO
                WHERE EMISSAO >= @StartDate AND EMISSAO <= @EndDate
                  AND CLIENTE IS NOT NULL
                GROUP BY CLIENTE
                HAVING MAX(EMISSAO) <= @ReferenceDate
                ORDER BY MAX(EMISSAO) ASC, {DistinctBudgetCountSql()} DESC";

            var rows = await connection.QueryAsync(sql, new { StartDate = startDate, EndDate = endDate, ReferenceDate = referenceDate });
            var points = rows.Select(row =>
            {
                var lastDate = row?.UltimaEmissão is DateTime dt ? dt.Date : endDate;
                var days = (decimal)(endDate.Date - lastDate).TotalDays;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "CLIENTE") ?? "Sem cliente",
                    Value = days,
                    Count = SafeToDecimal(row, "OrçamentosNoPeríodo"),
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Clientes sem compra/orÃ§amento recente",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["customers"] = points.Count },
	                Meta = new SalesBudgetChartMetaDto
	                {
	                    Warnings = new List<string> { $"Recorte inicial: clientes sem orÃ§amento nos Ãºltimos {daysWithoutBudget} dias atÃ© a data final do filtro." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildLowConversionCustomersChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int minBudgets = 5, int top = 12)
        {
            var parameters = new DynamicParameters();
            parameters.Add("MinBudgets", minBudgets);
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(CLIENTE AS NVARCHAR(200)), 'Sem cliente') AS Cliente,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos,
                    SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY CLIENTE
                HAVING {DistinctBudgetCountSql()} >= @MinBudgets
                ORDER BY (CAST(SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS DECIMAL(18,6)) / NULLIF({DistinctBudgetCountSql()}, 0)) ASC,
                         {DistinctBudgetCountSql()} DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var total = SafeToDecimal(row, "TotalOrçamentos");
                var approved = SafeToDecimal(row, "Aprovados");
                var ratio = total > 0 ? approved / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Cliente") ?? "Sem cliente",
                    Value = ratio,
                    Percentage = ratio,
                    Count = total,
                    Amount = approved
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
	                Title = "Clientes com baixa conversÃ£o",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["minBudgets"] = minBudgets,
                    ["items"] = points.Count
                },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { $"Ordenado por menor conversÃ£o (mÃ­nimo {minBudgets} orÃ§amentos)." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildDistinctCustomerCountByGroupChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string groupColumn,
            string labelAlias,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    {DistinctCustomerCountSql()} AS TotalClientes
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                ORDER BY TotalClientes DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var value = SafeToDecimal(row, "TotalClientes");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informação",
                    Value = value,
                    Count = value
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { $"{labelAlias} ordenado por clientes distintos." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAverageTicketByItemChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    ISNULL(SUM(I.VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql("I")} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY I.ITEM
                HAVING {DistinctBudgetCountSql("I")} > 0
                ORDER BY (ISNULL(SUM(I.VALORTOTAL), 0) / NULLIF({DistinctBudgetCountSql("I")}, 0)) DESC, TotalOrçamentos DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var total = SafeToDecimal(row, "TotalValor");
                var count = SafeToDecimal(row, "TotalOrçamentos");
                var avg = count > 0 ? total / count : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Item") ?? "Sem informação",
                    Value = avg,
                    Amount = avg,
                    Count = count
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Produtos com maior ticket medio",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAveragePercentByItemChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string percentColumn,
            string amountColumn,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    ISNULL(AVG(ISNULL(I.{percentColumn}, 0)), 0) AS AvgPercent,
                    ISNULL(SUM(ISNULL(I.{amountColumn}, 0)), 0) AS TotalAmount
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY I.ITEM
                ORDER BY AvgPercent DESC, TotalAmount DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var avgRaw = SafeToDecimal(row, "AvgPercent");
                var avgNormalized = NormalizePercent(avgRaw);
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Item") ?? "Sem informação",
                    Value = avgNormalized,
                    Percentage = avgNormalized,
                    Count = SafeToDecimal(row, "TotalAmount")
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Percentual normalizado (se a base vier em 0-100, converte para 0-1)." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAverageNumberByItemChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string metricSql,
            string metricKind,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    {metricSql} AS Valor
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY I.ITEM
                ORDER BY Valor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var value = SafeToDecimal(row, "Valor");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Item") ?? "Sem informação",
                    Value = value,
                    Amount = metricKind == "currency" ? value : null,
                    Count = metricKind == "number" ? value : null
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildLeastQuotedProductsChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    {DistinctBudgetCountSql("I")} AS Orçamentos
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY I.ITEM
                HAVING {DistinctBudgetCountSql("I")} > 0
                ORDER BY Orçamentos ASC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var value = SafeToDecimal(row, "Orçamentos");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Item") ?? "Sem informação",
                    Value = value,
                    Count = value
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Produtos menos orçados",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildShareByItemChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12,
            string title = "Participação de cada produto no total")
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var totalSql = $"SELECT ISNULL(SUM(I.VALORTOTAL), 0) AS Valor FROM VW_SWIA_ORCAMENTO_ITEM I INNER JOIN VW_SWIA_ORCAMENTO O ON O.CODEMPRESA = I.CODEMPRESA AND O.ORCAMENTO = I.ORCAMENTO {where}";
            var total = await connection.ExecuteScalarAsync<decimal>(totalSql, parameters);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    ISNULL(SUM(I.VALORTOTAL), 0) AS Valor
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY I.ITEM
                ORDER BY Valor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "Valor");
                var pct = total > 0 ? amount / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Item") ?? "Sem informação",
                    Value = amount,
                    Amount = amount,
                    Percentage = pct
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "pie",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = total },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAbcItemChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var totalSql = $"SELECT ISNULL(SUM(I.VALORTOTAL), 0) AS Valor FROM VW_SWIA_ORCAMENTO_ITEM I INNER JOIN VW_SWIA_ORCAMENTO O ON O.CODEMPRESA = I.CODEMPRESA AND O.ORCAMENTO = I.ORCAMENTO {where}";
            var total = await connection.ExecuteScalarAsync<decimal>(totalSql, parameters);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    ISNULL(SUM(I.VALORTOTAL), 0) AS Valor
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY I.ITEM
                ORDER BY Valor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            decimal cumulative = 0m;
            var points = rows.Select(row =>
            {
                var value = SafeToDecimal(row, "Valor");
                cumulative += value;
                var cumulativePct = total > 0 ? cumulative / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Item") ?? "Sem informação",
                    Value = value,
                    Amount = value,
                    Percentage = cumulativePct
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Curva ABC de produtos",
                Visualization = "line",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = total },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Curva acumulada por produto (valor)." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildMonthlyEvolutionByItemChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int topItems = 5)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var topSql = $@"
                SELECT TOP {topItems}
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    ISNULL(SUM(I.VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY I.ITEM
                ORDER BY TotalValor DESC";

            var topRows = (await connection.QueryAsync(topSql, parameters)).ToList();
            var itemSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var items = new List<string>();
            foreach (var row in topRows)
            {
                var item = SafeToString(row, "Item") ?? "Sem informação";
                if (!string.IsNullOrWhiteSpace(item) && itemSet.Add(item))
                {
                    items.Add(item);
                }
            }

            if (items.Count == 0)
            {
                return new SalesBudgetChartDatasetDto
                {
                    ChartId = chartId,
                    Title = "Evolucao mensal por produto",
                    Visualization = "line",
                    Data = new List<SalesBudgetChartPointDto>(),
                    Totals = new Dictionary<string, decimal>(),
                    Meta = new SalesBudgetChartMetaDto { Warnings = new List<string> { "Sem dados para montar a evolucao." } }
                };
            }

            parameters.Add("Items", items);
            var whereWithItems = AppendCondition(where, "ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') IN @Items");

            var sql = $@"
                SELECT
                    YEAR(O.EMISSAO) AS Ano,
                    MONTH(O.EMISSAO) AS Mes,
                    CONCAT(RIGHT('00' + CAST(MONTH(O.EMISSAO) AS VARCHAR(2)), 2), '/', CAST(YEAR(O.EMISSAO) AS VARCHAR(4))) AS MesAno,
                    ISNULL(SUM(I.VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {whereWithItems}
                GROUP BY YEAR(O.EMISSAO), MONTH(O.EMISSAO)
                ORDER BY YEAR(O.EMISSAO), MONTH(O.EMISSAO)";

            var rows = (await connection.QueryAsync(sql, parameters)).ToList();
            var points = rows.Select(row =>
            {
                var value = SafeToDecimal(row, "TotalValor");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "MesAno") ?? "-",
                    Date = SafeToString(row, "MesAno"),
                    Value = value,
                    Amount = value
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Evolucao mensal por produto",
                Visualization = "line",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = items.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { $"Versao inicial: soma do total mensal dos top {items.Count} produtos." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildGrossNetGapByItemChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    ISNULL(AVG(ISNULL(I.VALORUNITARIOBRUTO, 0)), 0) AS BrutoMedio,
                    ISNULL(AVG(ISNULL(I.VALORUNITARIOLIQUIDO, 0)), 0) AS LíquidoMedio
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY I.ITEM
                ORDER BY (ISNULL(AVG(ISNULL(I.VALORUNITARIOBRUTO, 0)), 0) - ISNULL(AVG(ISNULL(I.VALORUNITARIOLIQUIDO, 0)), 0)) DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var bruto = SafeToDecimal(row, "BrutoMedio");
                var líquido = SafeToDecimal(row, "LíquidoMedio");
                var gap = bruto - líquido;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Item") ?? "Sem informação",
                    Value = gap,
                    Amount = gap,
                    Count = líquido
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Diferenca entre valor bruto e líquido por produto",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Versao inicial: Value=(bruto medio - líquido medio), Count=líquido medio." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildProductDemandDropChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var endDate = filters.EndDate?.Date ?? DateTime.Today;
            var startDate = filters.StartDate?.Date ?? endDate.AddMonths(-2);
            var previousStart = startDate.AddDays(-(endDate - startDate).TotalDays - 1);
            var previousEnd = startDate.AddDays(-1);

            var sql = @"
                WITH atual AS (
                    SELECT TOP (@Top)
                        I.ITEM,
                        SUM(I.VALORTOTAL) AS ValorAtual
                    FROM VW_SWIA_ORCAMENTO_ITEM I
                    INNER JOIN VW_SWIA_ORCAMENTO O
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    WHERE O.EMISSAO >= @StartDate AND O.EMISSAO <= @EndDate
                    GROUP BY I.ITEM
                ),
                anterior AS (
                    SELECT
                        I.ITEM,
                        SUM(I.VALORTOTAL) AS ValorAnterior
                    FROM VW_SWIA_ORCAMENTO_ITEM I
                    INNER JOIN VW_SWIA_ORCAMENTO O
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    WHERE O.EMISSAO >= @PreviousStart AND O.EMISSAO <= @PreviousEnd
                    GROUP BY I.ITEM
                )
                SELECT TOP (@Top)
                    ISNULL(a.ITEM, b.ITEM) AS Item,
                    ISNULL(a.ValorAtual, 0) AS ValorAtual,
                    ISNULL(b.ValorAnterior, 0) AS ValorAnterior,
                    ISNULL(a.ValorAtual, 0) - ISNULL(b.ValorAnterior, 0) AS Crescimento
                FROM atual a
                FULL OUTER JOIN anterior b ON a.ITEM = b.ITEM
                ORDER BY Crescimento ASC";

            var rows = await connection.QueryAsync(sql, new
            {
                StartDate = startDate,
                EndDate = endDate,
                PreviousStart = previousStart,
                PreviousEnd = previousEnd,
                Top = top
            });

            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Item") ?? "Sem informação",
                Value = SafeToDecimal(row, "Crescimento"),
                Amount = SafeToDecimal(row, "ValorAtual"),
                Count = SafeToDecimal(row, "ValorAnterior")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Produtos com queda de demanda",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["dropTotal"] = points.Sum(x => x.Value ?? 0m)
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Comparativo entre o período selecionado e a janela imediatamente anterior." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildConversionByGroupChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, string title, string groupColumn, string labelAlias, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos,
                    SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                HAVING {DistinctBudgetCountSql()} > 0
                ORDER BY Aprovados DESC, TotalOrçamentos DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var total = SafeToDecimal(row, "TotalOrçamentos");
                var approved = SafeToDecimal(row, "Aprovados");
                var ratio = total > 0 ? approved / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informação",
                    Value = ratio * 100m,
                    Percentage = ratio,
                    Count = total
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["groups"] = points.Count,
                    ["avgConversion"] = points.Count > 0 ? points.Average(x => x.Value ?? 0m) : 0m
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { $"Conversão aproximada por {labelAlias.ToLowerInvariant()} com base no STATUS atual." }
                }
            };
        }

        private static decimal NormalizePercent(decimal value)
        {
            if (value < 0m) return value;
            return value > 1m ? value / 100m : value;
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAverageTicketByGroupChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string groupColumn,
            string labelAlias,
            int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                HAVING {DistinctBudgetCountSql()} > 0
                ORDER BY TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var totalAmount = SafeToDecimal(row, "TotalValor");
                var totalCount = SafeToDecimal(row, "TotalOrçamentos");
                var avgTicket = totalCount > 0 ? totalAmount / totalCount : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informação",
                    Value = avgTicket,
                    Amount = avgTicket,
                    Count = totalCount
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["groups"] = points.Count,
                    ["avgTicket"] = points.Count > 0 ? points.Average(x => x.Value ?? 0m) : 0m
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { $"Ticket médio por {labelAlias.ToLowerInvariant()} (valor total / quantidade)." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAveragePercentByGroupChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string groupColumn,
            string labelAlias,
            string percentColumn,
            string amountColumn,
            int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    ISNULL(AVG(ISNULL({percentColumn}, 0)), 0) AS AvgPercent,
                    ISNULL(SUM(ISNULL({amountColumn}, 0)), 0) AS TotalAmount
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                ORDER BY AvgPercent DESC, TotalAmount DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var avgRaw = SafeToDecimal(row, "AvgPercent");
                var avgNormalized = NormalizePercent(avgRaw);
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informação",
                    Value = avgNormalized,
                    Percentage = avgNormalized,
                    Count = SafeToDecimal(row, "TotalAmount")
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["groups"] = points.Count
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Percentual normalizado (se a base vier em 0-100, converte para 0-1)." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildRankingAvgTicketBySellerChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int minBudgets = 5, int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            parameters.Add("MinBudgets", minBudgets);
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                WITH base_data AS (
                    SELECT
                        ISNULL(VENDEDOR, 'Sem vendedor') AS Vendedor,
                        ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                        {DistinctBudgetCountSql()} AS TotalOrçamentos
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY VENDEDOR
                )
                SELECT TOP {top}
                    Vendedor,
                    TotalValor / NULLIF(TotalOrçamentos, 0) AS TicketMedio,
                    TotalOrçamentos
                FROM base_data
                WHERE TotalOrçamentos >= @MinBudgets
                ORDER BY TicketMedio DESC, TotalOrçamentos DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Vendedor") ?? "Sem vendedor",
                Value = SafeToDecimal(row, "TicketMedio"),
                Amount = SafeToDecimal(row, "TicketMedio"),
                Count = SafeToDecimal(row, "TotalOrçamentos")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Ranking de vendedores por ticket medio",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["minBudgets"] = minBudgets,
                    ["items"] = points.Count
                },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { $"Aplica mÃ­nimo de {minBudgets} orÃ§amentos para evitar distorÃ§Ã£o." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildTopCitiesTicketChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12,
            int minBudgets = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            parameters.Add("MinBudgets", minBudgets);
            var where = BuildWhere(conditions);

            var sql = $@"
                WITH base_data AS (
                    SELECT
                        ISNULL(CAST(CIDADE AS NVARCHAR(200)), 'Sem informação') AS Cidade,
                        ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                        {DistinctBudgetCountSql()} AS TotalOrçamentos
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY CIDADE
                )
                SELECT TOP {top}
                    Cidade,
                    TotalValor / NULLIF(TotalOrçamentos, 0) AS TicketMedio,
                    TotalOrçamentos
                FROM base_data
                WHERE TotalOrçamentos >= @MinBudgets
                ORDER BY TicketMedio DESC, TotalOrçamentos DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Cidade") ?? "Sem informação",
                Value = SafeToDecimal(row, "TicketMedio"),
                Amount = SafeToDecimal(row, "TicketMedio"),
                Count = SafeToDecimal(row, "TotalOrçamentos")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Ranking de cidades com maior ticket medio",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["minBudgets"] = minBudgets,
                    ["items"] = points.Count
                },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { $"Aplica mÃ­nimo de {minBudgets} orÃ§amentos por cidade." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildRankingAvgMarkupBySellerChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int minBudgets = 5, int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            parameters.Add("MinBudgets", minBudgets);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(VENDEDOR, 'Sem vendedor') AS Vendedor,
                    ISNULL(AVG(ISNULL(MARKUP, 0)), 0) AS AvgMarkup,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY VENDEDOR
                HAVING {DistinctBudgetCountSql()} >= @MinBudgets
                ORDER BY AvgMarkup DESC, TotalOrçamentos DESC";

            var rows = await connection.QueryAsync(sql, parameters);

            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Vendedor") ?? "Sem vendedor",
                Value = SafeToDecimal(row, "AvgMarkup"),
                Count = SafeToDecimal(row, "TotalOrçamentos")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Ranking de vendedores por margem/markup",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["minBudgets"] = minBudgets,
                    ["items"] = points.Count
                },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildStatusCountAmountByGroupChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string groupColumn,
            string labelAlias,
            string statusCondition,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string> { statusCondition };
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                HAVING {DistinctBudgetCountSql()} > 0
                ORDER BY TotalOrçamentos DESC, TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Grupo") ?? "Sem informação",
                Value = SafeToDecimal(row, "TotalOrçamentos"),
                Count = SafeToDecimal(row, "TotalOrçamentos"),
                Amount = SafeToDecimal(row, "TotalValor")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { $"Ranking por quantidade (desempate por valor) por {labelAlias.ToLowerInvariant()}." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildMonthlyEvolutionByGroupChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string groupColumn,
            int topGroups = 5)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var topGroupsSql = $@"
                SELECT TOP {topGroups}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                ORDER BY TotalValor DESC";

            var groupRows = (await connection.QueryAsync(topGroupsSql, parameters)).ToList();
            var groupSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var groups = new List<string>();
            foreach (var row in groupRows)
            {
                var group = SafeToString(row, "Grupo") ?? "Sem informação";
                if (!string.IsNullOrWhiteSpace(group))
                {
                    if (groupSet.Add(group))
                    {
                        groups.Add(group);
                    }
                }
            }

            if (groups.Count == 0)
            {
                return new SalesBudgetChartDatasetDto
                {
                    ChartId = chartId,
                    Title = title,
                    Visualization = "line",
                    Data = new List<SalesBudgetChartPointDto>(),
                    Totals = new Dictionary<string, decimal>(),
                    Meta = new SalesBudgetChartMetaDto
                    {
                        Warnings = new List<string> { "Sem dados para montar a evolucao." }
                    }
                };
            }

            parameters.Add("Groups", groups);
            var whereWithGroups = AppendCondition(where, $"ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') IN @Groups");

            var sql = $@"
                SELECT
                    YEAR(EMISSAO) AS Ano,
                    MONTH(EMISSAO) AS Mes,
                    CONCAT(RIGHT('00' + CAST(MONTH(EMISSAO) AS VARCHAR(2)), 2), '/', CAST(YEAR(EMISSAO) AS VARCHAR(4))) AS MesAno,
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO
                {whereWithGroups}
                GROUP BY YEAR(EMISSAO), MONTH(EMISSAO), {groupColumn}
                ORDER BY YEAR(EMISSAO), MONTH(EMISSAO)";

            var rows = (await connection.QueryAsync(sql, parameters)).ToList();

            var monthSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var monthBuckets = new List<string>();
            foreach (var row in rows)
            {
                var month = SafeToString(row, "MesAno") ?? "-";
                if (monthSet.Add(month))
                {
                    monthBuckets.Add(month);
                }
            }

            var normalizedRows = rows.Select(row => new
            {
                Month = SafeToString(row, "MesAno") ?? "-",
                Group = SafeToString(row, "Grupo") ?? "Sem informação",
                Value = SafeToDecimal(row, "TotalValor")
            }).ToList();

            var lookup = new Dictionary<string, Dictionary<string, decimal>>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in normalizedRows)
            {
                if (!lookup.TryGetValue(item.Month, out Dictionary<string, decimal>? monthMap))
                {
                    monthMap = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
                    lookup[item.Month] = monthMap;
                }

                monthMap[item.Group] = item.Value;
            }

            var points = monthBuckets.Select(month =>
            {
                var dict = lookup.TryGetValue(month, out var monthData) ? monthData : new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
                var totalForMonth = groups.Sum(g => dict.TryGetValue(g, out var v) ? v : 0m);
                return new SalesBudgetChartPointDto
                {
                    Label = month,
                    Date = month,
                    Value = totalForMonth,
                    Amount = totalForMonth
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "line",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["groups"] = groups.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { $"Versao inicial: soma do total mensal dos top {groups.Count} {groupColumn.ToLowerInvariant()}." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildSellerComparisonChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 8)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(VENDEDOR AS NVARCHAR(200)), 'Sem vendedor') AS Vendedor,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos,
                    ISNULL(AVG(ISNULL(MARKUP, 0)), 0) AS AvgMarkup,
                    ISNULL(AVG(ISNULL(PERCENTUALDESCONTO, 0)), 0) AS AvgDiscount
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY VENDEDOR
                ORDER BY TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var totalAmount = SafeToDecimal(row, "TotalValor");
                var totalCount = SafeToDecimal(row, "TotalOrçamentos");
                var avgTicket = totalCount > 0 ? totalAmount / totalCount : 0m;
                var avgDiscount = NormalizePercent(SafeToDecimal(row, "AvgDiscount"));
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Vendedor") ?? "Sem vendedor",
                    Value = totalAmount,
                    Amount = totalAmount,
                    Count = totalCount,
                    Percentage = avgDiscount,
                    Date = avgTicket.ToString("0.##")
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Comparativo entre vendedores",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Versao inicial: compara valor total (barras). Metricas extras: Count=qtd, Percentage=desconto medio, Date=ticket medio (texto)." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildShareByGroupChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string groupColumn,
            string labelAlias,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var totalSql = $"SELECT ISNULL(SUM(VALORTOTAL), 0) AS Valor FROM VW_SWIA_ORCAMENTO {where}";
            var total = await connection.ExecuteScalarAsync<decimal>(totalSql, parameters);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    ISNULL(SUM(VALORTOTAL), 0) AS Valor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                ORDER BY Valor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "Valor");
                var pct = total > 0 ? amount / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informação",
                    Value = amount,
                    Amount = amount,
                    Percentage = pct
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "pie",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["total"] = total
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { $"Participação (aproximada) do {labelAlias.ToLowerInvariant()} no total do período." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildTopByGroupChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string group1Column,
            string group1Label,
            string group2Column,
            string group2Label,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    CONCAT(
                        ISNULL(CAST({group1Column} AS NVARCHAR(200)), 'Sem {group1Label.ToLowerInvariant()}'),
                        ' / ',
                        ISNULL(CAST({group2Column} AS NVARCHAR(200)), 'Sem {group2Label.ToLowerInvariant()}')
                    ) AS Grupo,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {group1Column}, {group2Column}
                ORDER BY TotalOrçamentos DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Grupo") ?? "Sem informação",
                Value = SafeToDecimal(row, "TotalOrçamentos"),
                Count = SafeToDecimal(row, "TotalOrçamentos")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAbcChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, string title, string groupColumn, string labelAlias, int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    SUM(VALORTOTAL) AS Valor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                ORDER BY Valor DESC";

            var rows = (await connection.QueryAsync(sql, parameters)).Cast<object>().ToList();
            var values = rows.Select(row =>
            {
                string label = SafeToString(row, "Grupo") ?? "Sem informação";
                decimal value = SafeToDecimal(row, "Valor");

                return (Label: label, Value: value);
            }).ToList();

            decimal total = values.Sum(item => item.Value);
            var cumulative = 0m;
            var points = values.Select(item =>
            {
                cumulative += item.Value;
                var cumulativePct = total > 0 ? cumulative / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = item.Label,
                    Value = item.Value,
                    Amount = item.Value,
                    Percentage = cumulativePct
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "line",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["total"] = total,
                    ["topItems"] = points.Count
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { $"Curva acumulada por {labelAlias.ToLowerInvariant()}." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildTopCrossChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, string title, string group1Column, string group1Label, string group2Column, string group2Label, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT TOP {top}
                    CONCAT(
                        ISNULL(CAST({group1Column} AS NVARCHAR(200)), 'Sem {group1Label.ToLowerInvariant()}'),
                        ' / ',
                        ISNULL(CAST({group2Column} AS NVARCHAR(200)), 'Sem {group2Label.ToLowerInvariant()}')
                    ) AS Grupo,
                    SUM(I.VALORTOTAL) AS Valor
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY {group1Column}, {group2Column}
                ORDER BY Valor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var value = SafeToDecimal(row, "Valor");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informação",
                    Value = value,
                    Amount = value
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildProductDemandGrowthChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var endDate = filters.EndDate?.Date ?? DateTime.Today;
            var startDate = filters.StartDate?.Date ?? endDate.AddMonths(-2);
            var previousStart = startDate.AddDays(-(endDate - startDate).TotalDays - 1);
            var previousEnd = startDate.AddDays(-1);

            var sql = @"
                WITH atual AS (
                    SELECT TOP (@Top)
                        I.ITEM,
                        SUM(I.VALORTOTAL) AS ValorAtual
                    FROM VW_SWIA_ORCAMENTO_ITEM I
                    INNER JOIN VW_SWIA_ORCAMENTO O
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    WHERE O.EMISSAO >= @StartDate AND O.EMISSAO <= @EndDate
                    GROUP BY I.ITEM
                ),
                anterior AS (
                    SELECT
                        I.ITEM,
                        SUM(I.VALORTOTAL) AS ValorAnterior
                    FROM VW_SWIA_ORCAMENTO_ITEM I
                    INNER JOIN VW_SWIA_ORCAMENTO O
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    WHERE O.EMISSAO >= @PreviousStart AND O.EMISSAO <= @PreviousEnd
                    GROUP BY I.ITEM
                )
                SELECT TOP (@Top)
                    ISNULL(a.ITEM, b.ITEM) AS Item,
                    ISNULL(a.ValorAtual, 0) AS ValorAtual,
                    ISNULL(b.ValorAnterior, 0) AS ValorAnterior,
                    ISNULL(a.ValorAtual, 0) - ISNULL(b.ValorAnterior, 0) AS Crescimento
                FROM atual a
                FULL OUTER JOIN anterior b ON a.ITEM = b.ITEM
                ORDER BY Crescimento DESC";

            var rows = await connection.QueryAsync(sql, new
            {
                StartDate = startDate,
                EndDate = endDate,
                PreviousStart = previousStart,
                PreviousEnd = previousEnd,
                Top = top
            });

            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Item") ?? "Sem informação",
                Value = SafeToDecimal(row, "Crescimento"),
                Amount = SafeToDecimal(row, "ValorAtual"),
                Count = SafeToDecimal(row, "ValorAnterior")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Produtos com crescimento de demanda",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["growthTotal"] = points.Sum(x => x.Value ?? 0m)
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Comparativo entre o período selecionado e a janela imediatamente anterior." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildProductMixPerBudgetChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);
            var sql = $@"
                WITH mix AS (
                    SELECT
                        CONCAT(CAST(O.CODEMPRESA AS NVARCHAR(50)), ':', CAST(O.ORCAMENTO AS NVARCHAR(50))) AS OrçamentoKey,
                        COUNT(DISTINCT I.CODIGOITEM) AS ItensDistintos
                    FROM VW_SWIA_ORCAMENTO O
                    INNER JOIN VW_SWIA_ORCAMENTO_ITEM I
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    {where}
                    GROUP BY O.CODEMPRESA, O.ORCAMENTO
                )
                SELECT
                    CAST(ItensDistintos AS NVARCHAR(30)) AS Faixa,
                    COUNT(*) AS Valor
                FROM mix
                GROUP BY ItensDistintos
                ORDER BY ItensDistintos";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = $"{SafeToString(row, "Faixa")} itens",
                Value = SafeToDecimal(row, "Valor"),
                Count = SafeToDecimal(row, "Valor")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
	                Title = "Mix de produtos por orÃ§amento",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["budgets"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildProductCooccurrenceChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT TOP {top}
                    CONCAT(I1.ITEM, ' + ', I2.ITEM) AS ParProdutos,
                    COUNT(*) AS Frequência
                FROM VW_SWIA_ORCAMENTO O
                INNER JOIN VW_SWIA_ORCAMENTO_ITEM I1
                  ON O.CODEMPRESA = I1.CODEMPRESA
                 AND O.ORCAMENTO = I1.ORCAMENTO
                INNER JOIN VW_SWIA_ORCAMENTO_ITEM I2
                  ON O.CODEMPRESA = I2.CODEMPRESA
                 AND O.ORCAMENTO = I2.ORCAMENTO
                 AND I1.CODIGOITEM < I2.CODIGOITEM
                {where}
                GROUP BY I1.ITEM, I2.ITEM
                ORDER BY Frequência DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "ParProdutos") ?? "Sem informação",
                Value = SafeToDecimal(row, "Frequência"),
                Count = SafeToDecimal(row, "Frequência")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Produtos que mais aparecem juntos",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["pairs"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Consulta de coocorrencia limitada aos principais pares para evitar carga excessiva." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildDiscountVsConversionChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT
                    CASE
                        WHEN PERCENTUALDESCONTO IS NULL OR PERCENTUALDESCONTO = 0 THEN '0%'
                        WHEN PERCENTUALDESCONTO <= 5 THEN '0-5%'
                        WHEN PERCENTUALDESCONTO <= 10 THEN '5-10%'
                        WHEN PERCENTUALDESCONTO <= 20 THEN '10-20%'
                        ELSE '20%+'
                    END AS FaixaDesconto,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos,
                    SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY CASE
                    WHEN PERCENTUALDESCONTO IS NULL OR PERCENTUALDESCONTO = 0 THEN '0%'
                    WHEN PERCENTUALDESCONTO <= 5 THEN '0-5%'
                    WHEN PERCENTUALDESCONTO <= 10 THEN '5-10%'
                    WHEN PERCENTUALDESCONTO <= 20 THEN '10-20%'
                    ELSE '20%+'
                END";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var total = SafeToDecimal(row, "TotalOrçamentos");
                var approved = SafeToDecimal(row, "Aprovados");
                var ratio = total > 0 ? approved / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "FaixaDesconto") ?? "-",
                    Value = ratio,
                    Percentage = ratio,
                    Count = total
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
	                Title = "RelaÃ§Ã£o desconto x conversÃ£o",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["avgConversion"] = points.Count > 0 ? points.Average(x => x.Value ?? 0m) : 0m },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "ConversÃ£o aproximada por faixa de desconto." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildSinglePercentKpiChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string percentColumn)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = $"SELECT ISNULL(AVG(ISNULL({percentColumn}, 0)), 0) AS Valor FROM VW_SWIA_ORCAMENTO {where}";

            var rawValue = await connection.ExecuteScalarAsync<decimal>(sql, parameters);
            var normalizedValue = NormalizePercent(rawValue);

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = title, Value = normalizedValue, Percentage = normalizedValue }
                },
                Totals = new Dictionary<string, decimal>
                {
                    ["rawAvg"] = rawValue,
                    ["avg"] = normalizedValue
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Percentual normalizado (se a base vier em 0-100, converte para 0-1)." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildDiscountImpactTotalChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalLíquido,
                    ISNULL(SUM(VALORDESCONTO), 0) AS TotalDesconto
                FROM VW_SWIA_ORCAMENTO
                {where}";

            var row = await connection.QuerySingleAsync(sql, parameters);
            var netTotal = SafeToDecimal(row, "TotalLíquido");
            var discountTotal = SafeToDecimal(row, "TotalDesconto");
            var denominator = netTotal + discountTotal;
            var ratio = denominator > 0 ? discountTotal / denominator : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Impacto do desconto no valor total",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Impacto do desconto", Value = ratio, Percentage = ratio }
                },
                Totals = new Dictionary<string, decimal>
                {
                    ["netTotal"] = netTotal,
                    ["discountTotal"] = discountTotal,
                    ["impactRatio"] = ratio
                },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildHighestDiscountRankingChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrçamentoKey,
                    ISNULL(CLIENTE, 'Sem cliente') AS Cliente,
                    ISNULL(VALORTOTAL, 0) AS ValorTotal,
                    ISNULL(VALORDESCONTO, 0) AS ValorDesconto,
                    ISNULL(PERCENTUALDESCONTO, 0) AS PercentualDesconto
                FROM VW_SWIA_ORCAMENTO
                {where}
                ORDER BY ISNULL(VALORDESCONTO, 0) DESC, ISNULL(PERCENTUALDESCONTO, 0) DESC, ISNULL(VALORTOTAL, 0) DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var discountValue = SafeToDecimal(row, "ValorDesconto");
                var percent = NormalizePercent(SafeToDecimal(row, "PercentualDesconto"));
                var key = SafeToString(row, "OrçamentoKey") ?? "-";
                var customer = SafeToString(row, "Cliente") ?? "Sem cliente";
                return new SalesBudgetChartPointDto
                {
                    Label = $"{customer} - {key} ({percent:P0})",
                    Value = discountValue,
                    Amount = discountValue,
                    Percentage = percent,
                    Count = SafeToDecimal(row, "ValorTotal")
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Ranking de maiores descontos",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "Value=valor do desconto; Count=valor total do orÃ§amento; percentual aproximado no label." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildAboveAvgDiscountBudgetsChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var avgSql = $"SELECT ISNULL(AVG(ISNULL(PERCENTUALDESCONTO, 0)), 0) AS Valor FROM VW_SWIA_ORCAMENTO {where}";
            var avgRaw = await connection.ExecuteScalarAsync<decimal>(avgSql, parameters);
            parameters.Add("AvgDiscount", avgRaw);

            var sql = $@"
                SELECT TOP {top}
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrçamentoKey,
                    ISNULL(CLIENTE, 'Sem cliente') AS Cliente,
                    ISNULL(PERCENTUALDESCONTO, 0) AS PercentualDesconto,
                    ISNULL(VALORDESCONTO, 0) AS ValorDesconto,
                    ISNULL(VALORTOTAL, 0) AS ValorTotal
                FROM VW_SWIA_ORCAMENTO
                {AppendCondition(where, "ISNULL(PERCENTUALDESCONTO, 0) > @AvgDiscount")}
                ORDER BY ISNULL(PERCENTUALDESCONTO, 0) DESC, ISNULL(VALORDESCONTO, 0) DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var percent = NormalizePercent(SafeToDecimal(row, "PercentualDesconto"));
                var key = SafeToString(row, "OrçamentoKey") ?? "-";
                var customer = SafeToString(row, "Cliente") ?? "Sem cliente";
                return new SalesBudgetChartPointDto
                {
                    Label = $"{customer} - {key}",
                    Value = percent,
                    Percentage = percent,
                    Count = SafeToDecimal(row, "ValorDesconto")
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Orçamentos com desconto acima da media",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["avgDiscount"] = NormalizePercent(avgRaw),
                    ["items"] = points.Count
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Value=percentual de desconto; Count=valor do desconto. Media calculada no período." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildDiscountVsSellerChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(VENDEDOR AS NVARCHAR(200)), 'Sem vendedor') AS Vendedor,
                    ISNULL(AVG(ISNULL(PERCENTUALDESCONTO, 0)), 0) AS AvgDiscount,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos,
                    SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY VENDEDOR
                HAVING {DistinctBudgetCountSql()} > 0
                ORDER BY AvgDiscount DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var discountAvg = NormalizePercent(SafeToDecimal(row, "AvgDiscount"));
                var total = SafeToDecimal(row, "TotalOrçamentos");
                var approved = SafeToDecimal(row, "Aprovados");
                var conversion = total > 0 ? approved / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Vendedor") ?? "Sem vendedor",
                    Value = discountAvg,
                    Percentage = discountAvg,
                    Count = conversion
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Relação desconto x vendedor",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "VersÃ£o inicial: Value=desconto mÃ©dio (%). Count=conversÃ£o aproximada (0-1)." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildGrossVsNetChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalLíquido,
                    ISNULL(SUM(VALORDESCONTO), 0) AS TotalDesconto,
                    ISNULL(SUM(VALORACRESCIMO), 0) AS TotalAcrescimo
                FROM VW_SWIA_ORCAMENTO
                {where}";

            var row = await connection.QuerySingleAsync(sql, parameters);
            var netTotal = SafeToDecimal(row, "TotalLíquido");
            var discountTotal = SafeToDecimal(row, "TotalDesconto");
            var surchargeTotal = SafeToDecimal(row, "TotalAcrescimo");
            var grossApprox = netTotal + discountTotal - surchargeTotal;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Comparativo valor bruto x valor líquido",
                Visualization = "kpi_grid",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Bruto aprox.", Amount = grossApprox },
                    new() { Label = "Líquido (total)", Amount = netTotal },
                    new() { Label = "Desconto total", Amount = discountTotal },
                    new() { Label = "Acrescimo total", Amount = surchargeTotal },
                },
                Totals = new Dictionary<string, decimal>
                {
                    ["grossApprox"] = grossApprox,
                    ["netTotal"] = netTotal,
                    ["discountTotal"] = discountTotal,
                    ["surchargeTotal"] = surchargeTotal
                },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Bruto aproximado = líquido + desconto - acrescimo (heuristica inicial)." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildLowMarkupBudgetsChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10, string title = "Orçamentos com possível margem ruim")
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT TOP {top}
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrçamentoKey,
                    ISNULL(MARKUP, 0) AS Markup,
                    ISNULL(VALORTOTAL, 0) AS ValorTotal
                FROM VW_SWIA_ORCAMENTO
                {where}
                ORDER BY ISNULL(MARKUP, 0) ASC, ISNULL(VALORTOTAL, 0) DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "OrçamentoKey") ?? "-",
                Value = SafeToDecimal(row, "Markup"),
                Amount = SafeToDecimal(row, "ValorTotal")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildVolumeLowConversionChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, string title, string groupColumn, string labelAlias, int top = 10)
        {
            var baseChart = await BuildConversionByGroupChartAsync(connection, filters, chartId, title, groupColumn, labelAlias, top);
            baseChart.Data = baseChart.Data
                .OrderByDescending(x => x.Count ?? 0m)
                .ThenBy(x => x.Value ?? 0m)
                .Take(top)
                .ToList();
            baseChart.Meta.Warnings.Add("Priorizado por maior volume e menor conversÃ£o.");
            return baseChart;
        }

        private async Task<SalesBudgetChartDatasetDto> BuildLowVolumeHighTicketByOriginChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                WITH origin_data AS (
                    SELECT
                        ISNULL(CAST(ORIGEM AS NVARCHAR(200)), 'Sem informação') AS Origem,
                        ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                        {DistinctBudgetCountSql()} AS TotalOrçamentos
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY ORIGEM
                ),
                scored AS (
                    SELECT
                        Origem,
                        TotalValor,
                        TotalOrçamentos,
                        TotalValor / NULLIF(TotalOrçamentos, 0) AS TicketMedio,
                        AVG(CAST(TotalOrçamentos AS DECIMAL(18,6))) OVER () AS AvgOrçamentos
                    FROM origin_data
                )
                SELECT TOP {top}
                    Origem,
                    TicketMedio,
                    TotalOrçamentos,
                    TotalValor,
                    AvgOrçamentos
                FROM scored
                WHERE TotalOrçamentos <= AvgOrçamentos
                ORDER BY TicketMedio DESC, TotalOrçamentos ASC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Origem") ?? "Sem informação",
                Value = SafeToDecimal(row, "TicketMedio"),
                Amount = SafeToDecimal(row, "TicketMedio"),
                Count = SafeToDecimal(row, "TotalOrçamentos")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Canais que geram menos volume, mas maior ticket",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["items"] = points.Count
                },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "VersÃ£o inicial: considera 'baixo volume' como abaixo (ou igual) Ã  mÃ©dia de orÃ§amentos por origem no perÃ­odo." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildApprovedAmountByGroupChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, string title, string groupColumn, string labelAlias, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            conditions.Add(ApprovedStatusCondition());
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    SUM(VALORTOTAL) AS Valor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                ORDER BY Valor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Grupo") ?? "Sem informação",
                Value = SafeToDecimal(row, "Valor"),
                Amount = SafeToDecimal(row, "Valor")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["approvedAmount"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { $"{labelAlias} considerando apenas status aprovados." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildFreightRatioChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = $@"SELECT ISNULL(SUM(VALORFRETE), 0) AS TotalFrete, ISNULL(SUM(VALORTOTAL), 0) AS TotalOrçado FROM VW_SWIA_ORCAMENTO {where}";
            var row = await connection.QuerySingleAsync(sql, parameters);
            var totalFrete = SafeToDecimal(row, "TotalFrete");
            var totalOrçado = SafeToDecimal(row, "TotalOrçado");
            var ratio = totalOrçado > 0 ? totalFrete / totalOrçado : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Frete em relação ao valor total",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Relação frete", Value = ratio, Percentage = ratio, Amount = totalFrete }
                },
                Totals = new Dictionary<string, decimal>
                {
                    ["totalFreight"] = totalFrete,
                    ["totalBudget"] = totalOrçado,
                    ["ratio"] = ratio
                },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAverageFreightPerBudgetChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    ISNULL(SUM(VALORFRETE), 0) AS TotalFrete,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}";

            var row = await connection.QuerySingleAsync(sql, parameters);
            var totalFrete = SafeToDecimal(row, "TotalFrete");
            var totalBudgets = Convert.ToInt32(SafeToDecimal(row, "TotalOrçamentos"));
            var avgFreight = totalBudgets > 0 ? totalFrete / totalBudgets : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
	                Title = "Frete mÃ©dio por orÃ§amento",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
	                    new() { Label = "Frete mÃ©dio", Value = avgFreight, Amount = avgFreight }
	                },
                Totals = new Dictionary<string, decimal>
                {
                    ["totalFreight"] = totalFrete,
                    ["totalBudgets"] = totalBudgets,
                    ["avgFreight"] = avgFreight
                },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildHighestFreightBudgetsChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrçamentoKey,
                    ISNULL(CLIENTE, 'Sem cliente') AS Cliente,
                    ISNULL(VALORFRETE, 0) AS ValorFrete,
                    ISNULL(VALORTOTAL, 0) AS ValorTotal
                FROM VW_SWIA_ORCAMENTO
                {where}
                ORDER BY ISNULL(VALORFRETE, 0) DESC, ISNULL(VALORTOTAL, 0) DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var freight = SafeToDecimal(row, "ValorFrete");
                var key = SafeToString(row, "OrçamentoKey") ?? "-";
                var customer = SafeToString(row, "Cliente") ?? "Sem cliente";
                return new SalesBudgetChartPointDto
                {
                    Label = $"{customer} - {key}",
                    Value = freight,
                    Amount = freight,
                    Count = SafeToDecimal(row, "ValorTotal")
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Orçamentos com frete mais alto",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "Count=valor total do orÃ§amento." }
	                }
	            };
	        }

	        private async Task<SalesBudgetChartDatasetDto> BuildConversionByFreightTypeChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
	            => await BuildConversionByGroupChartAsync(connection, filters, chartId, "RelaÃ§Ã£o frete x conversÃ£o", "TIPOFRETE", "Tipo de frete");

        private async Task<SalesBudgetChartDatasetDto> BuildExecutiveDashboardChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var kpis = await GetKpisAsync(new SalesBudgetKpiRequestDto
            {
                Filters = filters,
                KpiIds = new List<string>
                {
                    "kpi_total_budget_amount",
                    "kpi_budget_count",
                    "kpi_avg_ticket",
                    "kpi_conversion_rate"
                }
            });

            var points = kpis.Items.Select(item => new SalesBudgetChartPointDto
            {
                Label = item.Label,
                Value = item.Value,
                Percentage = item.Format == "percentage" ? item.Value : null,
                Amount = item.Format == "currency" ? item.Value : null,
                Count = item.Format == "number" ? item.Value : null
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Dashboard executivo de vendas",
                Visualization = "kpi_grid",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["cards"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Resumo executivo consolidado a partir dos KPIs principais." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildOpenPipelineChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            conditions.Add(OpenStatusCondition());
            var where = BuildWhere(conditions);
            var sql = $@"SELECT ISNULL(SUM(VALORTOTAL), 0) AS Valor FROM VW_SWIA_ORCAMENTO {where}";
            var value = await connection.ExecuteScalarAsync<decimal>(sql, parameters);

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Pipeline comercial em aberto",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Pipeline", Value = value, Amount = value }
                },
                Totals = new Dictionary<string, decimal> { ["pipeline"] = value },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Pipeline aproximado com base nos status classificados como abertos." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildTextKpiChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, string title, string groupColumn)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT TOP 1
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                    SUM(VALORTOTAL) AS Valor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                ORDER BY Valor DESC";

            var row = await connection.QueryFirstOrDefaultAsync(sql, parameters);
            var label = row == null ? "Sem dados" : SafeToString(row, "Grupo") ?? "Sem dados";
            var value = row == null ? 0m : SafeToDecimal(row, "Valor");

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "kpi_text",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = label, Value = value, Amount = value }
                },
                Totals = new Dictionary<string, decimal> { ["total"] = value },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildPendingFollowupChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var endDate = filters.EndDate?.Date ?? DateTime.Today;
            var parameters = new DynamicParameters();
            var conditions = new List<string> { OpenStatusCondition(), "EMISSAO <= @FollowupLimit" };
            AddDateFilters(parameters, filters, conditions);
            parameters.Add("FollowupLimit", endDate.AddDays(-7));
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT TOP {top}
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrçamentoKey,
                    CLIENTE,
                    VALORTOTAL
                FROM VW_SWIA_ORCAMENTO
                {where}
                ORDER BY EMISSAO ASC, VALORTOTAL DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = $"{SafeToString(row, "CLIENTE") ?? "Sem cliente"} - {SafeToString(row, "OrçamentoKey") ?? "-"}",
                Value = SafeToDecimal(row, "VALORTOTAL"),
                Amount = SafeToDecimal(row, "VALORTOTAL")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Orçamentos pendentes de follow-up",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Recorte inicial: status em aberto emitidos há mais de 7 dias." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildHighValueNoReturnChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var endDate = filters.EndDate?.Date ?? DateTime.Today;
            var parameters = new DynamicParameters();
            var conditions = new List<string> { OpenStatusCondition(), "EMISSAO <= @NoReturnLimit" };
            AddDateFilters(parameters, filters, conditions);
            parameters.Add("NoReturnLimit", endDate.AddDays(-15));
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT TOP {top}
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrçamentoKey,
                    CLIENTE,
                    VALORTOTAL
                FROM VW_SWIA_ORCAMENTO
                {where}
                ORDER BY VALORTOTAL DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = $"{SafeToString(row, "CLIENTE") ?? "Sem cliente"} - {SafeToString(row, "OrçamentoKey") ?? "-"}",
                Value = SafeToDecimal(row, "VALORTOTAL"),
                Amount = SafeToDecimal(row, "VALORTOTAL")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Orçamentos de alto valor sem retorno",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Recorte inicial: status em aberto há mais de 15 dias." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildSellerVsTeamChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = $@"
                WITH seller_data AS (
                    SELECT
                        ISNULL(VENDEDOR, 'Sem vendedor') AS Vendedor,
                        SUM(VALORTOTAL) AS TotalValor,
                        {DistinctBudgetCountSql()} AS TotalOrçamentos
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY VENDEDOR
                )
                SELECT TOP {top}
                    Vendedor,
                    TotalValor / NULLIF(TotalOrçamentos, 0) AS TicketMedio
                FROM seller_data
                ORDER BY TicketMedio DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Vendedor") ?? "Sem informação",
                Value = SafeToDecimal(row, "TicketMedio"),
                Amount = SafeToDecimal(row, "TicketMedio")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Comparativo do vendedor com a equipe",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>
                {
                    ["teamAverage"] = points.Count > 0 ? points.Average(x => x.Value ?? 0m) : 0m
                },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildRepurchasePotentialChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var endDate = filters.EndDate?.Date ?? DateTime.Today;
            var sql = $@"
                SELECT TOP {top}
                    CLIENTE,
                    MAX(EMISSAO) AS UltimaEmissão,
                    {DistinctBudgetCountSql()} AS Orçamentos
                FROM VW_SWIA_ORCAMENTO
                WHERE EMISSAO <= @EndDate
                GROUP BY CLIENTE
                HAVING MAX(EMISSAO) <= @ReferenceDate
                ORDER BY Orçamentos DESC, MAX(EMISSAO) ASC";

            var rows = await connection.QueryAsync(sql, new
            {
                EndDate = endDate,
                ReferenceDate = endDate.AddDays(-30)
            });

            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "CLIENTE") ?? "Sem cliente",
                Value = SafeToDecimal(row, "Orçamentos"),
                Count = SafeToDecimal(row, "Orçamentos")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Clientes com potencial de recompra",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["customers"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "HeurÃ­stica inicial: clientes com histÃ³rico e sem orÃ§amento recente nos Ãºltimos 30 dias." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildMonthlyAmountSeriesChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT
                    YEAR(EMISSAO) AS Ano,
                    MONTH(EMISSAO) AS Mes,
                    CONCAT(RIGHT('00' + CAST(MONTH(EMISSAO) AS VARCHAR(2)), 2), '/', CAST(YEAR(EMISSAO) AS VARCHAR(4))) AS MesAno,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY YEAR(EMISSAO), MONTH(EMISSAO)
                ORDER BY YEAR(EMISSAO), MONTH(EMISSAO)";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "TotalValor");
                var label = SafeToString(row, "MesAno") ?? "-";
                return new SalesBudgetChartPointDto
                {
                    Label = label,
                    Date = label,
                    Value = amount,
                    Amount = amount
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "line",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["totalAmount"] = points.Sum(x => x.Amount ?? 0m) },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildMonthlyGrowthChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId)
        {
            var baseSeries = await BuildMonthlyAmountSeriesChartAsync(connection, filters, chartId, "Evolucao de crescimento mensal");

            var points = new List<SalesBudgetChartPointDto>();
            decimal? previous = null;
            foreach (var point in baseSeries.Data)
            {
                var current = point.Amount ?? point.Value ?? 0m;
                if (previous.HasValue)
                {
                    var delta = current - previous.Value;
                    var pct = previous.Value != 0m ? delta / previous.Value : 0m;
                    points.Add(new SalesBudgetChartPointDto
                    {
                        Label = point.Label,
                        Date = point.Date,
                        Value = delta,
                        Amount = delta,
                        Percentage = pct
                    });
                }
                previous = current;
            }

            baseSeries.Data = points;
            baseSeries.Totals = new Dictionary<string, decimal>
            {
                ["months"] = points.Count,
                ["deltaTotal"] = points.Sum(x => x.Value ?? 0m)
            };
            baseSeries.Meta.Warnings.Add("Delta mensal: Value/Amount=variacao (mes atual - mes anterior). Percentage=variacao relativa.");
            return baseSeries;
        }

        private async Task<SalesBudgetChartDatasetDto> BuildBudgetRankingChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string statusCondition,
            int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string> { statusCondition };
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrçamentoKey,
                    ISNULL(CLIENTE, 'Sem cliente') AS Cliente,
                    ISNULL(STATUS, '') AS Status,
                    ISNULL(VALORTOTAL, 0) AS ValorTotal
                FROM VW_SWIA_ORCAMENTO
                {where}
                ORDER BY ISNULL(VALORTOTAL, 0) DESC, EMISSAO ASC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "ValorTotal");
                var key = SafeToString(row, "OrçamentoKey") ?? "-";
                var customer = SafeToString(row, "Cliente") ?? "Sem cliente";
                var status = SafeToString(row, "Status") ?? "";
                var statusSuffix = string.IsNullOrWhiteSpace(status) ? "" : $" ({status})";
                return new SalesBudgetChartPointDto
                {
                    Label = $"{customer} - {key}{statusSuffix}",
                    Value = amount,
                    Amount = amount
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Ranking inicial por maior VALORTOTAL no período (com filtro por status)." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildStrategicRankingByGroupChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string groupColumn,
            int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                WITH data AS (
                    SELECT
                        ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                        ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                        {DistinctBudgetCountSql()} AS TotalOrçamentos,
                        SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY {groupColumn}
                )
                SELECT TOP {top}
                    Grupo,
                    TotalValor,
                    TotalOrçamentos,
                    Aprovados,
                    CASE WHEN TotalOrçamentos > 0 THEN CAST(Aprovados AS DECIMAL(18,6)) / CAST(TotalOrçamentos AS DECIMAL(18,6)) ELSE 0 END AS Conversão,
                    TotalValor * (0.5 + (CASE WHEN TotalOrçamentos > 0 THEN CAST(Aprovados AS DECIMAL(18,6)) / CAST(TotalOrçamentos AS DECIMAL(18,6)) ELSE 0 END)) AS Score
                FROM data
                WHERE TotalValor > 0
                ORDER BY Score DESC, TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "TotalValor");
                var conversion = SafeToDecimal(row, "Conversão");
                var totalCount = SafeToDecimal(row, "TotalOrçamentos");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informação",
                    Value = amount,
                    Amount = amount,
                    Count = totalCount,
                    Percentage = conversion
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string>
                    {
	                        "VersÃ£o inicial: ordena por score=TotalValor*(0.5+Conversão) e mostra barras por TotalValor. Percentage=conversÃ£o (0-1)."
	                    }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildStrategicRankingByItemChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var budgetKeySql = BudgetKeySql("O");
            var sql = $@"
                WITH data AS (
                    SELECT
                        ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                        ISNULL(SUM(I.VALORTOTAL), 0) AS TotalValor,
                        COUNT(DISTINCT {budgetKeySql}) AS TotalOrçamentos,
                        COUNT(DISTINCT CASE WHEN {ApprovedStatusCondition("O")} THEN {budgetKeySql} END) AS Aprovados
                    FROM VW_SWIA_ORCAMENTO_ITEM I
                    INNER JOIN VW_SWIA_ORCAMENTO O
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    {where}
                    GROUP BY I.ITEM
                )
                SELECT TOP {top}
                    Item,
                    TotalValor,
                    TotalOrçamentos,
                    Aprovados,
                    CASE WHEN TotalOrçamentos > 0 THEN CAST(Aprovados AS DECIMAL(18,6)) / CAST(TotalOrçamentos AS DECIMAL(18,6)) ELSE 0 END AS Conversão,
                    TotalValor * (0.5 + (CASE WHEN TotalOrçamentos > 0 THEN CAST(Aprovados AS DECIMAL(18,6)) / CAST(TotalOrçamentos AS DECIMAL(18,6)) ELSE 0 END)) AS Score
                FROM data
                WHERE TotalValor > 0
                ORDER BY Score DESC, TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "TotalValor");
                var conversion = SafeToDecimal(row, "Conversão");
                var totalCount = SafeToDecimal(row, "TotalOrçamentos");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Item") ?? "Sem informação",
                    Value = amount,
                    Amount = amount,
                    Count = totalCount,
                    Percentage = conversion
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string>
                    {
	                        "VersÃ£o inicial: ordena por score=TotalValor*(0.5+Conversão) e mostra barras por TotalValor. Percentage=conversÃ£o (0-1)."
	                    }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildSalesDropAlertsChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 10)
        {
            var endDate = (filters.EndDate?.Date ?? DateTime.Today.Date);
            var startDate = (filters.StartDate?.Date ?? endDate.AddDays(-30));
            if (startDate > endDate) startDate = endDate.AddDays(-30);
            var spanDays = (endDate - startDate).TotalDays;
            var previousStart = startDate.AddDays(-(spanDays + 1));
            var previousEnd = startDate.AddDays(-1);

            var sql = $@"
                WITH atual AS (
                    SELECT
                        ISNULL(VENDEDOR, 'Sem vendedor') AS Grupo,
                        ISNULL(SUM(VALORTOTAL), 0) AS ValorAtual
                    FROM VW_SWIA_ORCAMENTO
                    WHERE EMISSAO >= @StartDate AND EMISSAO <= @EndDate
                    GROUP BY VENDEDOR
                ),
                anterior AS (
                    SELECT
                        ISNULL(VENDEDOR, 'Sem vendedor') AS Grupo,
                        ISNULL(SUM(VALORTOTAL), 0) AS ValorAnterior
                    FROM VW_SWIA_ORCAMENTO
                    WHERE EMISSAO >= @PreviousStart AND EMISSAO <= @PreviousEnd
                    GROUP BY VENDEDOR
                )
                SELECT TOP (@Top)
                    ISNULL(a.Grupo, b.Grupo) AS Grupo,
                    ISNULL(a.ValorAtual, 0) AS ValorAtual,
                    ISNULL(b.ValorAnterior, 0) AS ValorAnterior,
                    ISNULL(b.ValorAnterior, 0) - ISNULL(a.ValorAtual, 0) AS Queda
                FROM atual a
                FULL OUTER JOIN anterior b ON a.Grupo = b.Grupo
                WHERE ISNULL(b.ValorAnterior, 0) > ISNULL(a.ValorAtual, 0)
                ORDER BY Queda DESC";

            var rows = await connection.QueryAsync(sql, new
            {
                StartDate = startDate,
                EndDate = endDate,
                PreviousStart = previousStart,
                PreviousEnd = previousEnd,
                Top = top
            });

            var points = rows.Select(row =>
            {
                var drop = SafeToDecimal(row, "Queda");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem vendedor",
                    Value = drop,
                    Amount = drop,
                    Count = SafeToDecimal(row, "ValorAnterior")
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Alertas de queda de vendas",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string>
                    {
                        $"Queda por vendedor vs janela anterior equivalente (atual: {startDate:yyyy-MM-dd}..{endDate:yyyy-MM-dd}; anterior: {previousStart:yyyy-MM-dd}..{previousEnd:yyyy-MM-dd}). Value/Amount=queda (anterior - atual). Count=valor anterior."
                    }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildCustomersHighConversionChanceChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                WITH data AS (
                    SELECT
                        ISNULL(CLIENTE, 'Sem cliente') AS Cliente,
                        {DistinctBudgetCountSql()} AS TotalOrçamentos,
                        SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados,
                        ISNULL(SUM(CASE WHEN {OpenStatusCondition()} THEN VALORTOTAL ELSE 0 END), 0) AS ValorAberto
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY CLIENTE
                )
                SELECT TOP {top}
                    Cliente,
                    TotalOrçamentos,
                    Aprovados,
                    ValorAberto,
                    CASE WHEN TotalOrçamentos > 0 THEN CAST(Aprovados AS DECIMAL(18,6)) / CAST(TotalOrçamentos AS DECIMAL(18,6)) ELSE 0 END AS Conversão
                FROM data
                WHERE ValorAberto > 0
                ORDER BY Conversão DESC, ValorAberto DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var openAmount = SafeToDecimal(row, "ValorAberto");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Cliente") ?? "Sem cliente",
                    Value = openAmount,
                    Amount = openAmount,
                    Count = SafeToDecimal(row, "TotalOrçamentos"),
                    Percentage = SafeToDecimal(row, "Conversão")
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
	                Title = "Clientes com maior chance de conversÃ£o",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["customers"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "VersÃ£o inicial: barras por valor em aberto; Percentage=conversÃ£o histÃ³rica (0-1) no perÃ­odo; requer mapeamento de STATUS." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildCustomersStoppedQuotingChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var endDate = filters.EndDate?.Date ?? DateTime.Today.Date;
            var referenceDate = endDate.AddDays(-30);

            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CLIENTE, 'Sem cliente') AS Cliente,
                    MAX(EMISSAO) AS UltimaEmissão,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql()} AS Orçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY CLIENTE
                HAVING MAX(EMISSAO) <= @ReferenceDate
                ORDER BY TotalValor DESC, MAX(EMISSAO) ASC";

            parameters.Add("ReferenceDate", referenceDate);
            var rows = await connection.QueryAsync(sql, parameters);

            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "TotalValor");
                var last = SafeToString(row, "UltimaEmissão") ?? "";
                return new SalesBudgetChartPointDto
                {
                    Label = $"{SafeToString(row, "Cliente") ?? "Sem cliente"} (última: {last})",
                    Value = amount,
                    Amount = amount,
                    Count = SafeToDecimal(row, "Orçamentos")
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Clientes que pararam de orcar",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["customers"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Versao inicial: clientes cuja última emissão no período filtrado foi há >=30 dias da data final." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildRecommendedProductsByCustomerChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var topCustomerSql = $@"
                SELECT TOP 1
                    ISNULL(CLIENTE, 'Sem cliente') AS Cliente,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY CLIENTE
                ORDER BY TotalValor DESC";

            var topRow = await connection.QueryFirstOrDefaultAsync(topCustomerSql, parameters);
            var customer = topRow == null ? null : SafeToString(topRow, "Cliente");
            if (string.IsNullOrWhiteSpace(customer))
            {
                return new SalesBudgetChartDatasetDto
                {
                    ChartId = chartId,
                    Title = "Produtos mais indicados por cliente",
                    Visualization = "bar",
                    Data = new List<SalesBudgetChartPointDto>(),
                    Totals = new Dictionary<string, decimal>(),
                    Meta = new SalesBudgetChartMetaDto { Warnings = new List<string> { "Sem dados para identificar cliente de referencia." } }
                };
            }

            parameters.Add("Customer", customer);
            var whereCustomer = AppendCondition(where, "ISNULL(O.CLIENTE, 'Sem cliente') = @Customer");
            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    ISNULL(SUM(I.VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {whereCustomer}
                GROUP BY I.ITEM
                ORDER BY TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "TotalValor");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Item") ?? "Sem informação",
                    Value = amount,
                    Amount = amount
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = $"Produtos mais indicados por cliente ({customer})",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Versao inicial: usa como referencia o cliente com maior valor no período e lista seus top produtos." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildTopProductsByRegionChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var topRegionSql = $@"
                SELECT TOP 1
                    ISNULL(UF, 'Sem UF') AS UF,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY UF
                ORDER BY TotalValor DESC";

            var topRow = await connection.QueryFirstOrDefaultAsync(topRegionSql, parameters);
            var uf = topRow == null ? null : SafeToString(topRow, "UF");
            if (string.IsNullOrWhiteSpace(uf))
            {
                return new SalesBudgetChartDatasetDto
                {
                    ChartId = chartId,
                    Title = "Produtos mais vendidos por região",
                    Visualization = "bar",
                    Data = new List<SalesBudgetChartPointDto>(),
                    Totals = new Dictionary<string, decimal>(),
                    Meta = new SalesBudgetChartMetaDto { Warnings = new List<string> { "Sem dados para identificar UF de referencia." } }
                };
            }

            parameters.Add("UF", uf);
            var whereUf = AppendCondition(where, "ISNULL(O.UF, 'Sem UF') = @UF");
            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    ISNULL(SUM(I.VALORTOTAL), 0) AS TotalValor
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {whereUf}
                GROUP BY I.ITEM
                ORDER BY TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "TotalValor");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Item") ?? "Sem informação",
                    Value = amount,
                    Amount = amount
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = $"Produtos mais vendidos por região (UF: {uf})",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Versao inicial: usa como referencia a UF com maior valor no período e lista seus top produtos." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildHighAcceptanceProductsChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var budgetKeySql = BudgetKeySql("O");
            var sql = $@"
                WITH data AS (
                    SELECT
                        ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                        COUNT(DISTINCT {budgetKeySql}) AS TotalOrçamentos,
                        COUNT(DISTINCT CASE WHEN {ApprovedStatusCondition("O")} THEN {budgetKeySql} END) AS Aprovados
                    FROM VW_SWIA_ORCAMENTO_ITEM I
                    INNER JOIN VW_SWIA_ORCAMENTO O
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    {where}
                    GROUP BY I.ITEM
                )
                SELECT TOP {top}
                    Item,
                    TotalOrçamentos,
                    Aprovados,
                    CASE WHEN TotalOrçamentos > 0 THEN CAST(Aprovados AS DECIMAL(18,6)) / CAST(TotalOrçamentos AS DECIMAL(18,6)) ELSE 0 END AS Conversão
                FROM data
                WHERE TotalOrçamentos >= 3
                ORDER BY Conversão DESC, TotalOrçamentos DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var ratio = SafeToDecimal(row, "Conversão");
                var pctPoints = ratio * 100m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Item") ?? "Sem informação",
                    Value = pctPoints,
                    Count = SafeToDecimal(row, "TotalOrçamentos"),
                    Percentage = ratio
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Produtos com maior aceitacao",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Value em pontos percentuais (0-100). Percentage em 0-1. Requer mapeamento de STATUS." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildOldOpenBudgetsChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 10)
        {
            var endDate = filters.EndDate?.Date ?? DateTime.Today;
            var parameters = new DynamicParameters();
            var conditions = new List<string> { OpenStatusCondition(), "EMISSAO <= @OldLimit" };
            AddDateFilters(parameters, filters, conditions);
            parameters.Add("OldLimit", endDate.AddDays(-30));
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrçamentoKey,
                    CLIENTE,
                    VALORTOTAL
                FROM VW_SWIA_ORCAMENTO
                {where}
                ORDER BY EMISSAO ASC, VALORTOTAL DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = $"{SafeToString(row, "CLIENTE") ?? "Sem cliente"} - {SafeToString(row, "OrçamentoKey") ?? "-"}",
                Value = SafeToDecimal(row, "VALORTOTAL"),
                Amount = SafeToDecimal(row, "VALORTOTAL")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Orçamentos antigos ainda em aberto",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Recorte inicial: status em aberto há mais de 30 dias." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildDiscountSensitiveCustomersChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CLIENTE, 'Sem cliente') AS Cliente,
                    ISNULL(AVG(ISNULL(PERCENTUALDESCONTO, 0)), 0) AS AvgDiscountRaw,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos,
                    SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY CLIENTE
                HAVING {DistinctBudgetCountSql()} > 0
                ORDER BY AvgDiscountRaw DESC, Aprovados DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var raw = SafeToDecimal(row, "AvgDiscountRaw");
                var normalized = NormalizePercent(raw);
                var pctPoints = normalized * 100m;
                var total = SafeToDecimal(row, "TotalOrçamentos");
                var approved = SafeToDecimal(row, "Aprovados");
                var conversion = total > 0 ? approved / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Cliente") ?? "Sem cliente",
                    Value = pctPoints,
                    Count = total,
                    Percentage = conversion
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Clientes sensíveis a desconto",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["customers"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "Value=desconto mÃ©dio (pontos percentuais, 0-100). Percentage=conversÃ£o (0-1)." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildLowDiscountCustomersChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CLIENTE, 'Sem cliente') AS Cliente,
                    ISNULL(AVG(ISNULL(PERCENTUALDESCONTO, 0)), 0) AS AvgDiscountRaw,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos,
                    SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY CLIENTE
                HAVING SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) > 0
                ORDER BY AvgDiscountRaw ASC, Aprovados DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var raw = SafeToDecimal(row, "AvgDiscountRaw");
                var normalized = NormalizePercent(raw);
                var pctPoints = normalized * 100m;
                var total = SafeToDecimal(row, "TotalOrçamentos");
                var approved = SafeToDecimal(row, "Aprovados");
                var conversion = total > 0 ? approved / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Cliente") ?? "Sem cliente",
                    Value = pctPoints,
                    Count = total,
                    Percentage = conversion
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Clientes que compram sem muito desconto",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["customers"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Value=desconto medio (pontos percentuais, 0-100). Filtra clientes com pelo menos 1 aprovado no período." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildBestOriginBySellerChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                WITH data AS (
                    SELECT
                        ISNULL(VENDEDOR, 'Sem vendedor') AS Vendedor,
                        ISNULL(ORIGEM, 'Sem origem') AS Origem,
                        ISNULL(SUM(VALORTOTAL), 0) AS TotalValor
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY VENDEDOR, ORIGEM
                ),
                ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY Vendedor ORDER BY TotalValor DESC) AS rn
                    FROM data
                )
                SELECT TOP {top}
                    Vendedor,
                    Origem,
                    TotalValor
                FROM ranked
                WHERE rn = 1
                ORDER BY TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "TotalValor");
                var seller = SafeToString(row, "Vendedor") ?? "Sem vendedor";
                var origin = SafeToString(row, "Origem") ?? "Sem origem";
                return new SalesBudgetChartPointDto
                {
                    Label = $"{seller} - {origin}",
                    Value = amount,
                    Amount = amount
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Melhor origem para cada vendedor",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto { Warnings = new List<string> { "Versao inicial: melhor origem por vendedor por valor total." } }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildBestProductBySellerChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var sql = $@"
                WITH data AS (
                    SELECT
                        ISNULL(O.VENDEDOR, 'Sem vendedor') AS Vendedor,
                        ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                        ISNULL(SUM(I.VALORTOTAL), 0) AS TotalValor
                    FROM VW_SWIA_ORCAMENTO_ITEM I
                    INNER JOIN VW_SWIA_ORCAMENTO O
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    {where}
                    GROUP BY O.VENDEDOR, I.ITEM
                ),
                ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY Vendedor ORDER BY TotalValor DESC) AS rn
                    FROM data
                )
                SELECT TOP {top}
                    Vendedor,
                    Item,
                    TotalValor
                FROM ranked
                WHERE rn = 1
                ORDER BY TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "TotalValor");
                var seller = SafeToString(row, "Vendedor") ?? "Sem vendedor";
                var item = SafeToString(row, "Item") ?? "Sem informação";
                return new SalesBudgetChartPointDto
                {
                    Label = $"{seller} - {item}",
                    Value = amount,
                    Amount = amount
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Melhor produto para cada vendedor",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto { Warnings = new List<string> { "Versao inicial: melhor produto por vendedor por valor total em itens." } }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildBestRegionBySellerChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                WITH data AS (
                    SELECT
                        ISNULL(VENDEDOR, 'Sem vendedor') AS Vendedor,
                        ISNULL(UF, 'Sem UF') AS UF,
                        ISNULL(SUM(VALORTOTAL), 0) AS TotalValor
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY VENDEDOR, UF
                ),
                ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY Vendedor ORDER BY TotalValor DESC) AS rn
                    FROM data
                )
                SELECT TOP {top}
                    Vendedor,
                    UF,
                    TotalValor
                FROM ranked
                WHERE rn = 1
                ORDER BY TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "TotalValor");
                var seller = SafeToString(row, "Vendedor") ?? "Sem vendedor";
                var uf = SafeToString(row, "UF") ?? "Sem UF";
                return new SalesBudgetChartPointDto
                {
                    Label = $"{seller} - {uf}",
                    Value = amount,
                    Amount = amount
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Melhor região para cada vendedor",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto { Warnings = new List<string> { "Versao inicial: melhor UF por vendedor por valor total." } }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildPersonalSellerRankingChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(VENDEDOR, 'Sem vendedor') AS Vendedor,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY VENDEDOR
                ORDER BY TotalValor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var amount = SafeToDecimal(row, "TotalValor");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Vendedor") ?? "Sem vendedor",
                    Value = amount,
                    Amount = amount,
                    Count = SafeToDecimal(row, "TotalOrçamentos")
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Ranking pessoal do vendedor",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Versao inicial: ranking geral por vendedor (sem identificar vendedor logado nesta etapa)." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildIndividualMonthlyEvolutionChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId)
        {
            return await BuildMonthlyAmountSeriesChartAsync(connection, filters, chartId, "Evolucao mensal individual");
        }

        private async Task<SalesBudgetChartDatasetDto> BuildUnderusedProductsBySellerChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            int top = 12)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var sql = $@"
                WITH company_top AS (
                    SELECT TOP {top}
                        ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                        ISNULL(SUM(I.VALORTOTAL), 0) AS TotalValor
                    FROM VW_SWIA_ORCAMENTO_ITEM I
                    INNER JOIN VW_SWIA_ORCAMENTO O
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    {where}
                    GROUP BY I.ITEM
                    ORDER BY TotalValor DESC
                ),
                sellers AS (
                    SELECT
                        ISNULL(O.VENDEDOR, 'Sem vendedor') AS Vendedor
                    FROM VW_SWIA_ORCAMENTO O
                    {where}
                    GROUP BY O.VENDEDOR
                ),
                seller_totals AS (
                    SELECT
                        ISNULL(O.VENDEDOR, 'Sem vendedor') AS Vendedor,
                        ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                        ISNULL(SUM(I.VALORTOTAL), 0) AS TotalValor
                    FROM VW_SWIA_ORCAMENTO_ITEM I
                    INNER JOIN VW_SWIA_ORCAMENTO O
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    {where}
                    GROUP BY O.VENDEDOR, I.ITEM
                )
                SELECT TOP {top}
                    s.Vendedor,
                    ct.Item,
                    ISNULL(st.TotalValor, 0) AS TotalValor,
                    ct.TotalValor AS CompanyTotalValor
                FROM sellers s
                CROSS JOIN company_top ct
                LEFT JOIN seller_totals st
                  ON st.Vendedor = s.Vendedor
                 AND st.Item = ct.Item
                WHERE ISNULL(st.TotalValor, 0) = 0
                ORDER BY ct.TotalValor DESC, s.Vendedor ASC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = $"{SafeToString(row, "Vendedor") ?? "Sem vendedor"} - {SafeToString(row, "Item") ?? "Sem informação"}",
                Value = SafeToDecimal(row, "CompanyTotalValor"),
                Amount = SafeToDecimal(row, "CompanyTotalValor"),
                Count = SafeToDecimal(row, "TotalValor")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Produtos que o vendedor vende pouco, mas a empresa vende bem",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["items"] = points.Count },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Versao inicial: top produtos da empresa (Amount=valor da empresa) onde o vendedor tem TotalValor=0 no período." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAverageTicketKpiChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"SELECT 
                            ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                            {DistinctBudgetCountSql()} AS TotalOrçamentos
                         FROM VW_SWIA_ORCAMENTO {where}";

            var row = await connection.QuerySingleAsync(sql, parameters);
            var totalValor = SafeToDecimal(row, "TotalValor");
            var totalOrçamentos = Convert.ToInt32(SafeToDecimal(row, "TotalOrçamentos"));
            var avgTicket = totalOrçamentos > 0 ? totalValor / totalOrçamentos : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = title, Value = avgTicket, Amount = avgTicket }
                },
                Totals = new Dictionary<string, decimal> { ["avgTicket"] = avgTicket },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildStatusAmountKpiChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string statusCondition)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string> { statusCondition };
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = $@"SELECT ISNULL(SUM(VALORTOTAL), 0) AS Valor FROM VW_SWIA_ORCAMENTO {where}";
            var value = await connection.ExecuteScalarAsync<decimal>(sql, parameters);

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = title, Value = value, Amount = value }
                },
                Totals = new Dictionary<string, decimal> { ["total"] = value },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Depende do mapeamento atual de STATUS." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildBestItemKpiChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP 1
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    ISNULL(SUM(I.VALORTOTAL), 0) AS Valor
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY I.ITEM
                ORDER BY Valor DESC";

            var row = await connection.QueryFirstOrDefaultAsync(sql, parameters);
            var label = row == null ? "Sem dados" : SafeToString(row, "Item") ?? "Sem dados";
            var value = row == null ? 0m : SafeToDecimal(row, "Valor");

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "kpi_text",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = label, Value = value, Amount = value }
                },
                Totals = new Dictionary<string, decimal> { ["total"] = value },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildHighestDiscountKpiChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP 1
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrçamentoKey,
                    ISNULL(CLIENTE, 'Sem cliente') AS Cliente,
                    ISNULL(VALORDESCONTO, 0) AS ValorDesconto
                FROM VW_SWIA_ORCAMENTO
                {where}
                ORDER BY ISNULL(VALORDESCONTO, 0) DESC, ISNULL(VALORTOTAL, 0) DESC";

            var row = await connection.QueryFirstOrDefaultAsync(sql, parameters);
            var customer = row == null ? "Sem dados" : SafeToString(row, "Cliente") ?? "Sem cliente";
            var key = row == null ? "-" : SafeToString(row, "OrçamentoKey") ?? "-";
            var value = row == null ? 0m : SafeToDecimal(row, "ValorDesconto");

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Maior desconto concedido",
                Visualization = "kpi_text",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = $"{customer} - {key}", Value = value, Amount = value }
                },
                Totals = new Dictionary<string, decimal> { ["discount"] = value },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildMostQuotedProductKpiChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions, "O.EMISSAO");
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP 1
                    ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                    {DistinctBudgetCountSql("O")} AS Orçamentos
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O
                  ON O.CODEMPRESA = I.CODEMPRESA
                 AND O.ORCAMENTO = I.ORCAMENTO
                {where}
                GROUP BY I.ITEM
                HAVING {DistinctBudgetCountSql("O")} > 0
                ORDER BY Orçamentos DESC";

            var row = await connection.QueryFirstOrDefaultAsync(sql, parameters);
            var label = row == null ? "Sem dados" : SafeToString(row, "Item") ?? "Sem informação";
            var count = row == null ? 0m : SafeToDecimal(row, "Orçamentos");

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Produto mais orçado",
                Visualization = "kpi_text",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = label, Value = count, Amount = count }
                },
                Totals = new Dictionary<string, decimal> { ["budgets"] = count },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "Value exibido em moeda no modo kpi_text; representa quantidade de orÃ§amentos (heurÃ­stica de exibiÃ§Ã£o)." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildHighestPotentialCustomerKpiChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                WITH data AS (
                    SELECT
                        ISNULL(CLIENTE, 'Sem cliente') AS Cliente,
                        ISNULL(SUM(CASE WHEN {OpenStatusCondition()} THEN VALORTOTAL ELSE 0 END), 0) AS ValorAberto,
                        {DistinctBudgetCountSql()} AS TotalOrçamentos,
                        SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY CLIENTE
                )
                SELECT TOP 1
                    Cliente,
                    ValorAberto,
                    CASE WHEN TotalOrçamentos > 0 THEN CAST(Aprovados AS DECIMAL(18,6)) / CAST(TotalOrçamentos AS DECIMAL(18,6)) ELSE 0 END AS Conversão
                FROM data
                WHERE ValorAberto > 0
                ORDER BY ValorAberto DESC";

            var row = await connection.QueryFirstOrDefaultAsync(sql, parameters);
            var customer = row == null ? "Sem dados" : SafeToString(row, "Cliente") ?? "Sem cliente";
            var openAmount = row == null ? 0m : SafeToDecimal(row, "ValorAberto");
            var conversion = row == null ? 0m : SafeToDecimal(row, "Conversão");

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Cliente com maior potencial",
                Visualization = "kpi_text",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = $"{customer} (aberto: {openAmount:0.##})", Value = openAmount, Amount = openAmount, Percentage = conversion }
                },
                Totals = new Dictionary<string, decimal> { ["openAmount"] = openAmount },
                Meta = new SalesBudgetChartMetaDto
                {
	                    Warnings = new List<string> { "VersÃ£o inicial: escolhe o cliente com maior valor em aberto. Percentage=conversÃ£o (0-1). Depende do mapeamento de STATUS." }
	                }
	            };
	        }

        private async Task<SalesBudgetChartDatasetDto> BuildHighestDeltaByGroupKpiChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string groupColumn,
            string kind,
            string mode)
        {
            var endDate = (filters.EndDate?.Date ?? DateTime.Today.Date);
            var startDate = (filters.StartDate?.Date ?? endDate.AddDays(-30));
            if (startDate > endDate) startDate = endDate.AddDays(-30);
            var spanDays = (endDate - startDate).TotalDays;
            var previousStart = startDate.AddDays(-(spanDays + 1));
            var previousEnd = startDate.AddDays(-1);

            var orderDirection = mode == "drop" ? "ASC" : "DESC";
            var sql = $@"
                WITH atual AS (
                    SELECT
                        ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                        ISNULL(SUM(VALORTOTAL), 0) AS ValorAtual
                    FROM VW_SWIA_ORCAMENTO
                    WHERE EMISSAO >= @StartDate AND EMISSAO <= @EndDate
                    GROUP BY {groupColumn}
                ),
                anterior AS (
                    SELECT
                        ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informação') AS Grupo,
                        ISNULL(SUM(VALORTOTAL), 0) AS ValorAnterior
                    FROM VW_SWIA_ORCAMENTO
                    WHERE EMISSAO >= @PreviousStart AND EMISSAO <= @PreviousEnd
                    GROUP BY {groupColumn}
                )
                SELECT TOP 1
                    ISNULL(a.Grupo, b.Grupo) AS Grupo,
                    ISNULL(a.ValorAtual, 0) AS ValorAtual,
                    ISNULL(b.ValorAnterior, 0) AS ValorAnterior,
                    ISNULL(a.ValorAtual, 0) - ISNULL(b.ValorAnterior, 0) AS Delta
                FROM atual a
                FULL OUTER JOIN anterior b ON a.Grupo = b.Grupo
                ORDER BY Delta {orderDirection}";

            var row = await connection.QueryFirstOrDefaultAsync(sql, new
            {
                StartDate = startDate,
                EndDate = endDate,
                PreviousStart = previousStart,
                PreviousEnd = previousEnd
            });

            var label = row == null ? "Sem dados" : SafeToString(row, "Grupo") ?? "Sem informação";
            var delta = row == null ? 0m : SafeToDecimal(row, "Delta");
            var displayDelta = mode == "drop" ? Math.Abs(delta) : delta;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "kpi_text",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = label, Value = displayDelta, Amount = displayDelta }
                },
                Totals = new Dictionary<string, decimal> { ["delta"] = displayDelta },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { $"Delta em valor total ({kind}) entre período atual e janela anterior equivalente." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildHighestDeltaByItemKpiChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string mode)
        {
            var endDate = (filters.EndDate?.Date ?? DateTime.Today.Date);
            var startDate = (filters.StartDate?.Date ?? endDate.AddDays(-30));
            if (startDate > endDate) startDate = endDate.AddDays(-30);
            var spanDays = (endDate - startDate).TotalDays;
            var previousStart = startDate.AddDays(-(spanDays + 1));
            var previousEnd = startDate.AddDays(-1);

            var orderDirection = mode == "drop" ? "ASC" : "DESC";
            var sql = $@"
                WITH atual AS (
                    SELECT
                        ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                        ISNULL(SUM(I.VALORTOTAL), 0) AS ValorAtual
                    FROM VW_SWIA_ORCAMENTO_ITEM I
                    INNER JOIN VW_SWIA_ORCAMENTO O
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    WHERE O.EMISSAO >= @StartDate AND O.EMISSAO <= @EndDate
                    GROUP BY I.ITEM
                ),
                anterior AS (
                    SELECT
                        ISNULL(CAST(I.ITEM AS NVARCHAR(200)), 'Sem informação') AS Item,
                        ISNULL(SUM(I.VALORTOTAL), 0) AS ValorAnterior
                    FROM VW_SWIA_ORCAMENTO_ITEM I
                    INNER JOIN VW_SWIA_ORCAMENTO O
                      ON O.CODEMPRESA = I.CODEMPRESA
                     AND O.ORCAMENTO = I.ORCAMENTO
                    WHERE O.EMISSAO >= @PreviousStart AND O.EMISSAO <= @PreviousEnd
                    GROUP BY I.ITEM
                )
                SELECT TOP 1
                    ISNULL(a.Item, b.Item) AS Item,
                    ISNULL(a.ValorAtual, 0) AS ValorAtual,
                    ISNULL(b.ValorAnterior, 0) AS ValorAnterior,
                    ISNULL(a.ValorAtual, 0) - ISNULL(b.ValorAnterior, 0) AS Delta
                FROM atual a
                FULL OUTER JOIN anterior b ON a.Item = b.Item
                ORDER BY Delta {orderDirection}";

            var row = await connection.QueryFirstOrDefaultAsync(sql, new
            {
                StartDate = startDate,
                EndDate = endDate,
                PreviousStart = previousStart,
                PreviousEnd = previousEnd
            });

            var label = row == null ? "Sem dados" : SafeToString(row, "Item") ?? "Sem informação";
            var delta = row == null ? 0m : SafeToDecimal(row, "Delta");
            var displayDelta = mode == "drop" ? Math.Abs(delta) : delta;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "kpi_text",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = label, Value = displayDelta, Amount = displayDelta }
                },
                Totals = new Dictionary<string, decimal> { ["delta"] = displayDelta },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Delta em valor total (itens) entre período atual e janela anterior equivalente." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildBestConversionOriginKpiTextChartAsync(
            IDbConnection connection,
            SalesBudgetFilterDto filters,
            string chartId,
            string title,
            string mode)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var orderDirection = mode == "lowest" ? "ASC" : "DESC";
            var sql = $@"
                WITH data AS (
                    SELECT
                        ISNULL(ORIGEM, 'Sem origem') AS Origem,
                        {DistinctBudgetCountSql()} AS TotalOrçamentos,
                        SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY ORIGEM
                )
                SELECT TOP 1
                    Origem,
                    CASE WHEN TotalOrçamentos > 0 THEN CAST(Aprovados AS DECIMAL(18,6)) / CAST(TotalOrçamentos AS DECIMAL(18,6)) ELSE 0 END AS Conversão
                FROM data
                WHERE TotalOrçamentos >= 3
                ORDER BY Conversão {orderDirection}, TotalOrçamentos DESC";

            var row = await connection.QueryFirstOrDefaultAsync(sql, parameters);
            var origin = row == null ? "Sem dados" : SafeToString(row, "Origem") ?? "Sem origem";
            var ratio = row == null ? 0m : SafeToDecimal(row, "Conversão");
            var pct = ratio * 100m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = "kpi_text",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = $"{origin} ({pct:0.#}%)" }
                },
                Totals = new Dictionary<string, decimal> { ["conversion"] = ratio },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Conversão aproximada por origem (depende do mapeamento de STATUS). KPI_text mostra apenas o label." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildHighDiscountVolumeChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT 
                    CASE 
                        WHEN PERCENTUALDESCONTO >= 20 THEN 'Desconto Extremo (>= 20%)'
                        WHEN PERCENTUALDESCONTO >= 10 THEN 'Alto Desconto (10-19%)'
                        WHEN PERCENTUALDESCONTO > 0 THEN 'Desconto PadrÃ£o (1-9%)'
                        ELSE 'Sem Desconto'
                    END AS Faixa,
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalValor,
                    {DistinctBudgetCountSql()} AS TotalOrçamentos
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY 
                    CASE 
                        WHEN PERCENTUALDESCONTO >= 20 THEN 'Desconto Extremo (>= 20%)'
                        WHEN PERCENTUALDESCONTO >= 10 THEN 'Alto Desconto (10-19%)'
                        WHEN PERCENTUALDESCONTO > 0 THEN 'Desconto PadrÃ£o (1-9%)'
                        ELSE 'Sem Desconto'
                    END
                ORDER BY SUM(VALORTOTAL) DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Faixa") ?? "Desconhecido",
                Amount = SafeToDecimal(row, "TotalValor"),
                Count = SafeToDecimal(row, "TotalOrçamentos"),
                Value = SafeToDecimal(row, "TotalValor")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Volume dependente de descontos",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["totalAmount"] = points.Sum(p => p.Amount ?? 0m) },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildWinRateVsTimeChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var approvedCond = ApprovedStatusCondition();
            var sql = $@"
                SELECT 
                    CASE 
                        WHEN DATEDIFF(day, EMISSAO, GETDATE()) <= 7 THEN 'A. 0 a 7 dias'
                        WHEN DATEDIFF(day, EMISSAO, GETDATE()) <= 15 THEN 'B. 8 a 15 dias'
                        WHEN DATEDIFF(day, EMISSAO, GETDATE()) <= 30 THEN 'C. 16 a 30 dias'
                        ELSE 'D. Mais de 30 dias'
                    END AS FaixaIdade,
                    COUNT(*) AS TotalCount,
                    SUM(CASE WHEN {approvedCond} THEN 1 ELSE 0 END) AS ApprovedCount
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY 
                    CASE 
                        WHEN DATEDIFF(day, EMISSAO, GETDATE()) <= 7 THEN 'A. 0 a 7 dias'
                        WHEN DATEDIFF(day, EMISSAO, GETDATE()) <= 15 THEN 'B. 8 a 15 dias'
                        WHEN DATEDIFF(day, EMISSAO, GETDATE()) <= 30 THEN 'C. 16 a 30 dias'
                        ELSE 'D. Mais de 30 dias'
                    END
                ORDER BY FaixaIdade";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => {
                decimal total = SafeToDecimal(row, "TotalCount");
                decimal approved = SafeToDecimal(row, "ApprovedCount");
                decimal rate = total > 0 ? (approved / total) * 100m : 0m;
                var label = SafeToString(row, "FaixaIdade")?.Substring(3) ?? "Desconhecido";
                return new SalesBudgetChartPointDto
                {
                    Label = label,
                    Value = rate,
                    Percentage = rate,
                    Count = total
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Taxa de ganho vs Idade do orÃ§amento",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal>(),
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildQuoteToCloseRatioChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            
            var approvedCond = ApprovedStatusCondition();
            var sql = $@"
                SELECT 
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalAmount,
                    ISNULL(SUM(CASE WHEN {approvedCond} THEN VALORTOTAL ELSE 0 END), 0) AS ApprovedAmount
                FROM VW_SWIA_ORCAMENTO
                {where}";

            var row = await connection.QueryFirstOrDefaultAsync(sql, parameters);
            decimal total = SafeToDecimal(row, "TotalAmount");
            decimal approved = SafeToDecimal(row, "ApprovedAmount");
            decimal ratio = total > 0 ? (approved / total) * 100m : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Volume orÃ§ado vs fechado",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "ConversÃ£o de Volume", Value = ratio, Percentage = ratio }
                },
                Totals = new Dictionary<string, decimal> { ["total"] = total, ["approved"] = approved },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAbandonmentRateChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            
            var openCond = OpenStatusCondition();
            // Abandono = Abertos a mais de 30 dias sobre o total de Abertos
            var sql = $@"
                SELECT 
                    SUM(CASE WHEN {openCond} THEN 1 ELSE 0 END) AS TotalOpen,
                    SUM(CASE WHEN {openCond} AND DATEDIFF(day, EMISSAO, GETDATE()) > 30 THEN 1 ELSE 0 END) AS AbandonedOpen
                FROM VW_SWIA_ORCAMENTO
                {where}";

            var row = await connection.QueryFirstOrDefaultAsync(sql, parameters);
            decimal totalOpen = SafeToDecimal(row, "TotalOpen");
            decimal abandonedOpen = SafeToDecimal(row, "AbandonedOpen");
            decimal rate = totalOpen > 0 ? (abandonedOpen / totalOpen) * 100m : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Taxa de Abandono",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "OrÃ§amentos Abandonados", Value = rate, Percentage = rate }
                },
                Totals = new Dictionary<string, decimal> { ["abandonedCount"] = abandonedOpen, ["openCount"] = totalOpen },
                Meta = new SalesBudgetChartMetaDto { Warnings = new List<string> { "Considera abandonado: Status Aberto e EmissÃ£o > 30 dias atrÃ¡s" } }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildAvgItemsPerTicketChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            
            var sql = $@"
                SELECT 
                    COUNT(I.SEQUENCIAITEM) AS TotalItems,
                    {DistinctBudgetCountSql("O")} AS TotalBudgets
                FROM VW_SWIA_ORCAMENTO_ITEM I
                INNER JOIN VW_SWIA_ORCAMENTO O ON O.CODEMPRESA = I.CODEMPRESA AND O.ORCAMENTO = I.ORCAMENTO
                {where.Replace("EMISSAO", "O.EMISSAO")}";

            var row = await connection.QueryFirstOrDefaultAsync(sql, parameters);
            decimal items = SafeToDecimal(row, "TotalItems");
            decimal budgets = SafeToDecimal(row, "TotalBudgets");
            decimal avg = budgets > 0 ? items / budgets : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "EficiÃªncia de Mix",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Itens por orÃ§amento", Value = avg, Count = avg }
                },
                Totals = new Dictionary<string, decimal>(),
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildSalesForecastChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            
            var approvedCond = ApprovedStatusCondition();
            var openCond = OpenStatusCondition();
            
            var sql = $@"
                SELECT 
                    ISNULL(SUM(VALORTOTAL), 0) AS TotalAmount,
                    ISNULL(SUM(CASE WHEN {approvedCond} THEN VALORTOTAL ELSE 0 END), 0) AS ApprovedAmount,
                    ISNULL(SUM(CASE WHEN {openCond} THEN VALORTOTAL ELSE 0 END), 0) AS OpenAmount
                FROM VW_SWIA_ORCAMENTO
                {where}";

            var row = await connection.QueryFirstOrDefaultAsync(sql, parameters);
            decimal totalAmount = SafeToDecimal(row, "TotalAmount");
            decimal approvedAmount = SafeToDecimal(row, "ApprovedAmount");
            decimal openAmount = SafeToDecimal(row, "OpenAmount");
            
            decimal historicalRate = totalAmount > 0 ? (approvedAmount / totalAmount) : 0m;
            decimal forecast = openAmount * historicalRate;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "PrevisÃ£o de Fechamento",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Forecast Atual", Value = forecast, Amount = forecast }
                },
                Totals = new Dictionary<string, decimal> { ["openAmount"] = openAmount, ["historicalRate"] = historicalRate * 100m },
                Meta = new SalesBudgetChartMetaDto { Warnings = new List<string> { "Calculado multiplicando o Valor Aberto atual pela Taxa HistÃ³rica de ConversÃ£o do perÃ­odo" } }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildChurnRiskChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            parameters.Add("ReferenceDate", filters?.EndDate?.Date ?? DateTime.Today);
            
            // Churn Risk: Customers with >= 3 budgets, but their last budget is more than 30 days older than their historical average interval
            var sql = $@"
                WITH CustomerStats AS (
                    SELECT 
                        CLIENTE,
                        COUNT(*) AS BudgetCount,
                        MIN(EMISSAO) AS FirstEmission,
                        MAX(EMISSAO) AS LastEmission
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY CLIENTE
                    HAVING COUNT(*) >= 3
                ),
                RiskAnalysis AS (
                    SELECT 
                        CLIENTE,
                        BudgetCount,
                        CAST(DATEDIFF(day, FirstEmission, LastEmission) AS DECIMAL(18,6)) / NULLIF(CAST(BudgetCount - 1 AS DECIMAL(18,6)), 0) AS AvgDaysBetween,
                        DATEDIFF(day, LastEmission, @ReferenceDate) AS DaysSinceLast
                    FROM CustomerStats
                )
                SELECT TOP 10
                    CLIENTE AS Label,
                    DaysSinceLast AS Value,
                    AvgDaysBetween AS Count
                FROM RiskAnalysis
                WHERE AvgDaysBetween IS NOT NULL
                  AND DaysSinceLast > (AvgDaysBetween * 2)
                  AND DaysSinceLast > 30
                ORDER BY DaysSinceLast DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Label") ?? "Sem Cliente",
                Value = SafeToDecimal(row, "Value"),
                Count = SafeToDecimal(row, "Count"),
                Percentage = 0 // Using this for something else if needed
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Risco de Churn (Inatividade)",
                Visualization = "table",
                Data = points,
                Totals = new Dictionary<string, decimal>(),
                Meta = new SalesBudgetChartMetaDto { Warnings = new List<string> { "Clientes com intervalo sem compras maior que o dobro do seu intervalo mÃ©dio histÃ³rico." } }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildHighProbabilityDealsChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            
            var openCond = OpenStatusCondition();
            // Alta probabilidade: Status aberto, emissÃ£o nos últimos 10 dias, de clientes que jÃ¡ compraram antes
            var sql = $@"
                WITH Buyers AS (
                    SELECT DISTINCT CLIENTE 
                    FROM VW_SWIA_ORCAMENTO 
                    WHERE {ApprovedStatusCondition()}
                )
                SELECT TOP 10
                    O.CLIENTE AS Cliente,
                    O.ORCAMENTO AS Orçamento,
                    O.VALORTOTAL AS Valor
                FROM VW_SWIA_ORCAMENTO O
                INNER JOIN Buyers B ON O.CLIENTE = B.CLIENTE
                {where.Replace("EMISSAO", "O.EMISSAO")} AND {openCond.Replace("STATUS", "O.STATUS")}
                AND DATEDIFF(day, O.EMISSAO, GETDATE()) <= 10
                ORDER BY O.VALORTOTAL DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = $"{SafeToString(row, "Cliente")} (Orc: {SafeToString(row, "Orçamento")})",
                Value = SafeToDecimal(row, "Valor"),
                Amount = SafeToDecimal(row, "Valor")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "OrÃ§amentos com alta probabilidade",
                Visualization = "table",
                Data = points,
                Totals = new Dictionary<string, decimal>(),
                Meta = new SalesBudgetChartMetaDto { Warnings = new List<string> { "OrÃ§amentos recentes em aberto de clientes que jÃ¡ possuem histÃ³rico de aprovaÃ§Ã£o." } }
            };
        }
    }
}

