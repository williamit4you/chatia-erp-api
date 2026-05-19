using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
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

        private static string ApprovedStatusCondition(string alias = "")
        {
            var prefix = string.IsNullOrWhiteSpace(alias) ? string.Empty : $"{alias}.";
            return $"(UPPER(ISNULL({prefix}STATUS, '')) LIKE '%APROV%' OR UPPER(ISNULL({prefix}STATUS, '')) LIKE '%FECH%' OR UPPER(ISNULL({prefix}STATUS, '')) LIKE '%CONVERT%')";
        }

        private static string OpenStatusCondition(string alias = "")
        {
            var prefix = string.IsNullOrWhiteSpace(alias) ? string.Empty : $"{alias}.";
            return $"(UPPER(ISNULL({prefix}STATUS, '')) LIKE '%ABERT%' OR UPPER(ISNULL({prefix}STATUS, '')) LIKE '%PEND%' OR UPPER(ISNULL({prefix}STATUS, '')) LIKE '%NEGOCI%')";
        }

        private static string LostStatusCondition(string alias = "")
        {
            var prefix = string.IsNullOrWhiteSpace(alias) ? string.Empty : $"{alias}.";
            return $"(UPPER(ISNULL({prefix}STATUS, '')) LIKE '%PERD%' OR UPPER(ISNULL({prefix}STATUS, '')) LIKE '%CANCEL%' OR UPPER(ISNULL({prefix}STATUS, '')) LIKE '%REPROV%')";
        }

        private static string AppendCondition(string where, string condition)
            => string.IsNullOrWhiteSpace(where) ? $" WHERE {condition}" : $"{where} AND {condition}";

        private static decimal SafeToDecimal(dynamic row, string key)
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

        private static string? SafeToString(dynamic row, string key)
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
                new() { KpiId = "kpi_total_budget_amount", Label = "Valor total orcado", Value = totalAmount, Format = "currency" },
                new() { KpiId = "kpi_budget_count", Label = "Quantidade de orcamentos", Value = totalCount, Format = "number" },
                new() { KpiId = "kpi_avg_ticket", Label = "Ticket medio", Value = avgTicket, Format = "currency" },
                new() { KpiId = "kpi_open_amount", Label = "Valor em aberto", Value = openAmount, Format = "currency" },
                new() { KpiId = "kpi_approved_amount", Label = "Valor aprovado", Value = approvedAmount, Format = "currency" },
                new() { KpiId = "kpi_lost_amount", Label = "Valor perdido", Value = lostAmount, Format = "currency" },
                new() { KpiId = "kpi_conversion_rate", Label = "Taxa de conversao", Value = conversionRate, Format = "percentage", Warning = "Depende do mapeamento atual de STATUS." },
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
                        _logger.LogWarning(ex, "Falha ao construir grafico '{ChartId}': {Message}", chartId, ex.Message);
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
                                Warnings = new List<string> { $"Erro ao gerar este grafico: {ex.Message}" }
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
                                _logger.LogError(reconnectEx, "Falha ao reconectar apos erro no grafico '{ChartId}'.", chartId);
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

        private async Task<SalesBudgetChartDatasetDto> BuildChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
        {
            return chartId switch
            {
                "overview_total_amount_period" => await BuildSingleValueChartAsync(connection, filters, chartId, "Valor total de orcamentos por periodo", "kpi", "SELECT ISNULL(SUM(VALORTOTAL), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Valor total"),
                "overview_total_count_period" => await BuildSingleValueChartAsync(connection, filters, chartId, "Quantidade de orcamentos por periodo", "kpi", $"SELECT {DistinctBudgetCountSql()} AS Valor FROM VW_SWIA_ORCAMENTO {{0}}", "Quantidade"),
                "overview_avg_ticket" => await BuildAverageTicketChartAsync(connection, filters),
                "overview_monthly_evolution" => await BuildMonthlyEvolutionChartAsync(connection, filters),
                "overview_amount_by_company" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total por empresa/filial", "bar", "EMPRESA", "Empresa", "SUM(VALORTOTAL)", "currency"),
                "overview_count_by_company" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Quantidade de orcamentos por empresa/filial", "bar", "EMPRESA", "Empresa", DistinctBudgetCountSql(), "number"),
                "overview_weekday_heatmap" => await BuildWeekdayHeatmapChartAsync(connection, filters),

                "funnel_by_status" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Funil por status do orcamento", "bar", "STATUS", "Status", "SUM(VALORTOTAL)", "currency"),
                "funnel_approval_rate" => await BuildConversionKpiChartAsync(connection, filters, chartId, "Taxa de aprovacao de orcamentos"),
                "funnel_conversion_by_seller" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "Conversao por vendedor", "VENDEDOR", "Vendedor"),
                "funnel_blocking_status_ranking" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Ranking de status que mais travam vendas", "bar", "STATUS", "Status", "SUM(VALORTOTAL)", "currency", OpenStatusCondition()),

                "seller_total_amount" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total orcado por vendedor", "bar", "VENDEDOR", "Vendedor", "SUM(VALORTOTAL)", "currency"),
                "seller_conversion" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "Conversao por vendedor", "VENDEDOR", "Vendedor"),
                "seller_abc_curve" => await BuildAbcChartAsync(connection, filters, chartId, "Curva ABC de vendedores", "VENDEDOR", "Vendedor"),
                "seller_top_product" => await BuildTopCrossChartAsync(connection, filters, chartId, "Vendedor x produto mais orcado", "O.VENDEDOR", "Vendedor", "I.ITEM", "Produto"),

                "customer_top_amount" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Top clientes por valor orcado", "bar", "CLIENTE", "Cliente", "SUM(VALORTOTAL)", "currency"),
                "customer_recurring" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Clientes recorrentes", "bar", "CLIENTE", "Cliente", DistinctBudgetCountSql(), "number"),
                "customer_highest_conversion" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "Clientes com maior taxa de conversao", "CLIENTE", "Cliente"),
                "customer_top_products" => await BuildTopCrossChartAsync(connection, filters, chartId, "Cliente x produtos mais orcados", "O.CLIENTE", "Cliente", "I.ITEM", "Produto"),

                "product_top_amount" => await BuildGroupedItemChartAsync(connection, filters, chartId, "Top produtos por valor total", "bar", "I.ITEM", "Produto", "SUM(I.VALORTOTAL)", "currency"),
                "product_demand_growth" => await BuildProductDemandGrowthChartAsync(connection, filters, chartId),
                "product_mix_per_budget" => await BuildProductMixPerBudgetChartAsync(connection, filters, chartId),
                "product_cooccurrence" => await BuildProductCooccurrenceChartAsync(connection, filters, chartId),

                "margin_total_discount" => await BuildSingleValueChartAsync(connection, filters, chartId, "Valor total de descontos concedidos", "kpi", "SELECT ISNULL(SUM(VALORDESCONTO), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Desconto total"),
                "margin_discount_vs_conversion" => await BuildDiscountVsConversionChartAsync(connection, filters, chartId),
                "margin_avg_markup_general" => await BuildSingleValueChartAsync(connection, filters, chartId, "Markup medio geral", "kpi", "SELECT ISNULL(AVG(MARKUP), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Markup medio"),
                "margin_possible_bad_margin_budgets" => await BuildLowMarkupBudgetsChartAsync(connection, filters, chartId),

                "source_total_amount" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total por origem", "pie", "ORIGEM", "Origem", "SUM(VALORTOTAL)", "currency"),
                "source_conversion" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "Conversao por origem", "ORIGEM", "Origem"),
                "source_best_channels" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Ranking de melhores canais de venda", "bar", "ORIGEM", "Origem", "SUM(VALORTOTAL)", "currency"),
                "source_high_volume_low_conversion" => await BuildVolumeLowConversionChartAsync(connection, filters, chartId, "Canais com muito orcamento e pouca conversao", "ORIGEM", "Origem"),

                "geo_amount_by_uf" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total por UF", "bar", "UF", "UF", "SUM(VALORTOTAL)", "currency"),
                "geo_count_by_city" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Quantidade por cidade", "bar", "CIDADE", "Cidade", DistinctBudgetCountSql(), "number"),
                "geo_state_heatmap" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Mapa de calor por estado", "heatmap", "UF", "UF", "SUM(VALORTOTAL)", "currency"),
                "geo_growth_opportunity_regions" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Regioes com maior oportunidade de crescimento", "bar", "UF", "UF", "SUM(VALORDESCONTO)", "currency"),

                "payment_total_amount" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Valor total por condicao de pagamento", "bar", "CONDPAG", "Condicao", "SUM(VALORTOTAL)", "currency"),
                "payment_conversion" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "Conversao por condicao de pagamento", "CONDPAG", "Condicao"),
                "payment_most_used" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Condicoes de pagamento mais usadas", "pie", "CONDPAG", "Condicao", DistinctBudgetCountSql(), "number"),
                "payment_vs_approval" => await BuildApprovedAmountByGroupChartAsync(connection, filters, chartId, "Condicao de pagamento x aprovacao", "CONDPAG", "Condicao"),

                "freight_total_amount" => await BuildSingleValueChartAsync(connection, filters, chartId, "Valor total de frete", "kpi", "SELECT ISNULL(SUM(VALORFRETE), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Frete total"),
                "freight_by_type" => await BuildGroupedHeaderChartAsync(connection, filters, chartId, "Frete por tipo de frete", "pie", "TIPOFRETE", "Tipo de frete", "SUM(VALORFRETE)", "currency"),
                "freight_ratio_total" => await BuildFreightRatioChartAsync(connection, filters, chartId),
                "freight_vs_conversion" => await BuildConversionByFreightTypeChartAsync(connection, filters, chartId),

                "exec_dashboard" => await BuildExecutiveDashboardChartAsync(connection, filters, chartId),
                "exec_open_pipeline" => await BuildOpenPipelineChartAsync(connection, filters, chartId),
                "exec_goal_vs_actual" => BuildPlannedChart(chartId, "Meta x realizado"),
                "exec_sales_forecast" => BuildPlannedChart(chartId, "Forecast de vendas"),

                "insight_pending_followup_budgets" => await BuildPendingFollowupChartAsync(connection, filters, chartId),
                "insight_high_value_no_return" => await BuildHighValueNoReturnChartAsync(connection, filters, chartId),
                "insight_seller_vs_team_avg" => await BuildSellerVsTeamChartAsync(connection, filters, chartId),
                "insight_repurchase_potential_customers" => await BuildRepurchasePotentialChartAsync(connection, filters, chartId),

                "kpi_total_budget_amount" => await BuildSingleValueChartAsync(connection, filters, chartId, "Valor total orcado", "kpi", "SELECT ISNULL(SUM(VALORTOTAL), 0) AS Valor FROM VW_SWIA_ORCAMENTO {0}", "Valor total"),
                "kpi_conversion_rate" => await BuildConversionKpiChartAsync(connection, filters, chartId, "Taxa de conversao"),
                "kpi_best_seller" => await BuildTextKpiChartAsync(connection, filters, chartId, "Melhor vendedor", "VENDEDOR"),
                "kpi_channel_highest_conversion" => await BuildConversionByGroupChartAsync(connection, filters, chartId, "Canal com maior conversao", "ORIGEM", "Origem"),
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
                    Warnings = new List<string> { "Grafico ainda nao implementado neste batch inicial." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildSingleValueChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, string title, string visualization, string sqlTemplate, string label)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = string.Format(sqlTemplate, where);
            var value = await connection.ExecuteScalarAsync<decimal>(sql, parameters);

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = title,
                Visualization = visualization,
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = label, Value = value, Amount = value }
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
                            {DistinctBudgetCountSql()} AS TotalOrcamentos
                         FROM VW_SWIA_ORCAMENTO {where}";

            var row = await connection.QuerySingleAsync(sql, parameters);
            var totalValor = SafeToDecimal(row, "TotalValor");
            var totalOrcamentos = Convert.ToInt32(SafeToDecimal(row, "TotalOrcamentos"));
            var avgTicket = totalOrcamentos > 0 ? totalValor / totalOrcamentos : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = "overview_avg_ticket",
                Title = "Ticket medio dos orcamentos",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Ticket medio", Value = avgTicket, Amount = avgTicket }
                },
                Totals = new Dictionary<string, decimal> { ["avgTicket"] = avgTicket },
                Meta = new SalesBudgetChartMetaDto()
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
                    {DistinctBudgetCountSql()} AS TotalOrcamentos
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
                Count = SafeToDecimal(row, "TotalOrcamentos"),
                Value = SafeToDecimal(row, "TotalValor")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = "overview_monthly_evolution",
                Title = "Evolucao mensal de orcamentos",
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
            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informacao') AS Grupo,
                    {metricSql} AS Valor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                ORDER BY Valor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var value = SafeToDecimal(row, "Valor");
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informacao",
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
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informacao') AS Grupo,
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
                    Label = SafeToString(row, "Grupo") ?? "Sem informacao",
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
                    new() { Label = "Conversao", Value = ratio, Percentage = ratio }
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

        private async Task<SalesBudgetChartDatasetDto> BuildConversionByGroupChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, string title, string groupColumn, string labelAlias, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);

            var sql = $@"
                SELECT TOP {top}
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informacao') AS Grupo,
                    {DistinctBudgetCountSql()} AS TotalOrcamentos,
                    SUM(CASE WHEN {ApprovedStatusCondition()} THEN 1 ELSE 0 END) AS Aprovados
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                HAVING {DistinctBudgetCountSql()} > 0
                ORDER BY Aprovados DESC, TotalOrcamentos DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row =>
            {
                var total = SafeToDecimal(row, "TotalOrcamentos");
                var approved = SafeToDecimal(row, "Aprovados");
                var ratio = total > 0 ? approved / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informacao",
                    Value = ratio,
                    Percentage = ratio,
                    Count = total,
                    Amount = approved
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
                    Warnings = new List<string> { $"Conversao aproximada por {labelAlias.ToLowerInvariant()} com base no STATUS atual." }
                }
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
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informacao') AS Grupo,
                    SUM(VALORTOTAL) AS Valor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                ORDER BY Valor DESC";

            var rows = (await connection.QueryAsync(sql, parameters)).ToList();
            var total = rows.Sum(row => SafeToDecimal(row, "Valor"));
            var cumulative = 0m;
            var points = rows.Select(row =>
            {
                var value = SafeToDecimal(row, "Valor");
                cumulative += value;
                var cumulativePct = total > 0 ? cumulative / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "Grupo") ?? "Sem informacao",
                    Value = value,
                    Amount = value,
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
                    Label = SafeToString(row, "Grupo") ?? "Sem informacao",
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
                Label = SafeToString(row, "Item") ?? "Sem informacao",
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
                    Warnings = new List<string> { "Comparativo entre o periodo selecionado e a janela imediatamente anterior." }
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
                        CONCAT(CAST(O.CODEMPRESA AS NVARCHAR(50)), ':', CAST(O.ORCAMENTO AS NVARCHAR(50))) AS OrcamentoKey,
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
                Title = "Mix de produtos por orcamento",
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
                    COUNT(*) AS Frequencia
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
                ORDER BY Frequencia DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "ParProdutos") ?? "Sem informacao",
                Value = SafeToDecimal(row, "Frequencia"),
                Count = SafeToDecimal(row, "Frequencia")
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
                    {DistinctBudgetCountSql()} AS TotalOrcamentos,
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
                var total = SafeToDecimal(row, "TotalOrcamentos");
                var approved = SafeToDecimal(row, "Aprovados");
                var ratio = total > 0 ? approved / total : 0m;
                return new SalesBudgetChartPointDto
                {
                    Label = SafeToString(row, "FaixaDesconto") ?? "-",
                    Value = ratio,
                    Percentage = ratio,
                    Count = total,
                    Amount = approved
                };
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Relacao desconto x conversao",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["avgConversion"] = points.Count > 0 ? points.Average(x => x.Value ?? 0m) : 0m },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Conversao aproximada por faixa de desconto." }
                }
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildLowMarkupBudgetsChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId, int top = 10)
        {
            var parameters = new DynamicParameters();
            var conditions = new List<string>();
            AddDateFilters(parameters, filters, conditions);
            var where = BuildWhere(conditions);
            var sql = $@"
                SELECT TOP {top}
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrcamentoKey,
                    ISNULL(MARKUP, 0) AS Markup,
                    ISNULL(VALORTOTAL, 0) AS ValorTotal
                FROM VW_SWIA_ORCAMENTO
                {where}
                ORDER BY ISNULL(MARKUP, 0) ASC, ISNULL(VALORTOTAL, 0) DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "OrcamentoKey") ?? "-",
                Value = SafeToDecimal(row, "Markup"),
                Amount = SafeToDecimal(row, "ValorTotal")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Orcamentos com possivel margem ruim",
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
            baseChart.Meta.Warnings.Add("Priorizado por maior volume e menor conversao.");
            return baseChart;
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
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informacao') AS Grupo,
                    SUM(VALORTOTAL) AS Valor
                FROM VW_SWIA_ORCAMENTO
                {where}
                GROUP BY {groupColumn}
                ORDER BY Valor DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Grupo") ?? "Sem informacao",
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
            var sql = $@"SELECT ISNULL(SUM(VALORFRETE), 0) AS TotalFrete, ISNULL(SUM(VALORTOTAL), 0) AS TotalOrcado FROM VW_SWIA_ORCAMENTO {where}";
            var row = await connection.QuerySingleAsync(sql, parameters);
            var totalFrete = SafeToDecimal(row, "TotalFrete");
            var totalOrcado = SafeToDecimal(row, "TotalOrcado");
            var ratio = totalOrcado > 0 ? totalFrete / totalOrcado : 0m;

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Frete em relacao ao valor total",
                Visualization = "kpi",
                Data = new List<SalesBudgetChartPointDto>
                {
                    new() { Label = "Relacao frete", Value = ratio, Percentage = ratio, Amount = totalFrete }
                },
                Totals = new Dictionary<string, decimal>
                {
                    ["totalFreight"] = totalFrete,
                    ["totalBudget"] = totalOrcado,
                    ["ratio"] = ratio
                },
                Meta = new SalesBudgetChartMetaDto()
            };
        }

        private async Task<SalesBudgetChartDatasetDto> BuildConversionByFreightTypeChartAsync(IDbConnection connection, SalesBudgetFilterDto filters, string chartId)
            => await BuildConversionByGroupChartAsync(connection, filters, chartId, "Relacao frete x conversao", "TIPOFRETE", "Tipo de frete");

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
                    ISNULL(CAST({groupColumn} AS NVARCHAR(200)), 'Sem informacao') AS Grupo,
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
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrcamentoKey,
                    CLIENTE,
                    VALORTOTAL
                FROM VW_SWIA_ORCAMENTO
                {where}
                ORDER BY EMISSAO ASC, VALORTOTAL DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = $"{SafeToString(row, "CLIENTE") ?? "Sem cliente"} - {SafeToString(row, "OrcamentoKey") ?? "-"}",
                Value = SafeToDecimal(row, "VALORTOTAL"),
                Amount = SafeToDecimal(row, "VALORTOTAL")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Orcamentos pendentes de follow-up",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Recorte inicial: status em aberto emitidos ha mais de 7 dias." }
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
                    CONCAT(CAST(CODEMPRESA AS NVARCHAR(30)), '/', CAST(ORCAMENTO AS NVARCHAR(30))) AS OrcamentoKey,
                    CLIENTE,
                    VALORTOTAL
                FROM VW_SWIA_ORCAMENTO
                {where}
                ORDER BY VALORTOTAL DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = $"{SafeToString(row, "CLIENTE") ?? "Sem cliente"} - {SafeToString(row, "OrcamentoKey") ?? "-"}",
                Value = SafeToDecimal(row, "VALORTOTAL"),
                Amount = SafeToDecimal(row, "VALORTOTAL")
            }).ToList();

            return new SalesBudgetChartDatasetDto
            {
                ChartId = chartId,
                Title = "Orcamentos de alto valor sem retorno",
                Visualization = "bar",
                Data = points,
                Totals = new Dictionary<string, decimal> { ["total"] = points.Sum(x => x.Value ?? 0m) },
                Meta = new SalesBudgetChartMetaDto
                {
                    Warnings = new List<string> { "Recorte inicial: status em aberto ha mais de 15 dias." }
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
                        {DistinctBudgetCountSql()} AS TotalOrcamentos
                    FROM VW_SWIA_ORCAMENTO
                    {where}
                    GROUP BY VENDEDOR
                )
                SELECT TOP {top}
                    Vendedor,
                    TotalValor / NULLIF(TotalOrcamentos, 0) AS TicketMedio
                FROM seller_data
                ORDER BY TicketMedio DESC";

            var rows = await connection.QueryAsync(sql, parameters);
            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "Vendedor") ?? "Sem informacao",
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
                    MAX(EMISSAO) AS UltimaEmissao,
                    {DistinctBudgetCountSql()} AS Orcamentos
                FROM VW_SWIA_ORCAMENTO
                WHERE EMISSAO <= @EndDate
                GROUP BY CLIENTE
                HAVING MAX(EMISSAO) <= @ReferenceDate
                ORDER BY Orcamentos DESC, MAX(EMISSAO) ASC";

            var rows = await connection.QueryAsync(sql, new
            {
                EndDate = endDate,
                ReferenceDate = endDate.AddDays(-30)
            });

            var points = rows.Select(row => new SalesBudgetChartPointDto
            {
                Label = SafeToString(row, "CLIENTE") ?? "Sem cliente",
                Value = SafeToDecimal(row, "Orcamentos"),
                Count = SafeToDecimal(row, "Orcamentos")
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
                    Warnings = new List<string> { "Heuristica inicial: clientes com historico e sem orcamento recente nos ultimos 30 dias." }
                }
            };
        }
    }
}
