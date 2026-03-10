using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Dapper;
using IT4You.Application.FinanceAnalytics.DTOs;
using IT4You.Application.FinanceAnalytics.Interfaces;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace IT4You.Infrastructure.Repositories
{
    public class FinanceAnalyticsRepository : IFinanceAnalyticsRepository
    {
        private readonly string _connectionString;

        public FinanceAnalyticsRepository(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection") 
                ?? throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");
        }

        private IDbConnection CreateConnection()
        {
            return new SqlConnection(_connectionString);
        }

        public async Task<FinanceSummaryDto> GetSummaryAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null)
        {
            using var connection = CreateConnection();
            var parameters = new DynamicParameters();
            
            string whereAberto = "";
            string wherePago = "";

            if (startDate.HasValue)
            {
                whereAberto += " AND DATAVENCIMENTO >= @StartDate";
                wherePago += " AND DATAPAGAMENTO >= @StartDate";
                parameters.Add("StartDate", startDate.Value);
            }
            if (endDate.HasValue)
            {
                whereAberto += " AND DATAVENCIMENTO <= @EndDate";
                wherePago += " AND DATAPAGAMENTO <= @EndDate";
                parameters.Add("EndDate", endDate.Value);
            }

            var pagAbertoSql = $"SELECT SUM(VALORORIG - ISNULL(VALORPAG, 0)) FROM VW_DOC_FIN_PAG_ABERTO WHERE 1=1 {whereAberto}";
            var recAbertoSql = $"SELECT SUM(VALORORIG - ISNULL(VALORPAG, 0)) FROM VW_DOC_FIN_REC_ABERTO WHERE 1=1 {whereAberto}";
            var pagoSql = $"SELECT SUM(VALORPAG) FROM VW_DOC_FIN_PAG_PAGO WHERE 1=1 {wherePago}";
            var recebidoSql = $"SELECT SUM(VALORPAG) FROM VW_DOC_FIN_REC_PAGO WHERE 1=1 {wherePago}";

            var pagAberto = await connection.ExecuteScalarAsync<decimal?>(pagAbertoSql, parameters) ?? 0;
            var recAberto = await connection.ExecuteScalarAsync<decimal?>(recAbertoSql, parameters) ?? 0;
            var pago = await connection.ExecuteScalarAsync<decimal?>(pagoSql, parameters) ?? 0;
            var recebido = await connection.ExecuteScalarAsync<decimal?>(recebidoSql, parameters) ?? 0;

            return new FinanceSummaryDto
            {
                TotalContasPagarAberto = pagAberto,
                TotalContasReceberAberto = recAberto,
                TotalPago = pago,
                TotalRecebido = recebido,
                SaldoProjetado = (recebido + recAberto) - (pago + pagAberto)
            };
        }

        public async Task<IEnumerable<MonthlyFlowDto>> GetMonthlyFlowAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null)
        {
            using var connection = CreateConnection();
            var parameters = new DynamicParameters();
            
            string wherePago = "";
            string whereAberto = "";

            if (startDate.HasValue)
            {
                wherePago += " AND DATAPAGAMENTO >= @StartDate";
                whereAberto += " AND DATAVENCIMENTO >= @StartDate";
                parameters.Add("StartDate", startDate.Value);
            }
            if (endDate.HasValue)
            {
                wherePago += " AND DATAPAGAMENTO <= @EndDate";
                whereAberto += " AND DATAVENCIMENTO <= @EndDate";
                parameters.Add("EndDate", endDate.Value);
            }

            // Using FORMAT for grouping by MM/yyyy
            var sqlRecebidos = $@"
                SELECT FORMAT(DATAPAGAMENTO, 'MM/yyyy') as Mes, SUM(VALORPAG) as Valor
                FROM VW_DOC_FIN_REC_PAGO 
                WHERE DATAPAGAMENTO IS NOT NULL {wherePago}
                GROUP BY FORMAT(DATAPAGAMENTO, 'MM/yyyy')
            ";

            var sqlPagos = $@"
                SELECT FORMAT(DATAPAGAMENTO, 'MM/yyyy') as Mes, SUM(VALORPAG) as Valor
                FROM VW_DOC_FIN_PAG_PAGO 
                WHERE DATAPAGAMENTO IS NOT NULL {wherePago}
                GROUP BY FORMAT(DATAPAGAMENTO, 'MM/yyyy')
            ";

            var sqlRecAberto = $@"
                SELECT FORMAT(DATAVENCIMENTO, 'MM/yyyy') as Mes, SUM(VALORORIG - ISNULL(VALORPAG, 0)) as Valor
                FROM VW_DOC_FIN_REC_ABERTO 
                WHERE DATAVENCIMENTO IS NOT NULL {whereAberto}
                GROUP BY FORMAT(DATAVENCIMENTO, 'MM/yyyy')
            ";

            var sqlPagAberto = $@"
                SELECT FORMAT(DATAVENCIMENTO, 'MM/yyyy') as Mes, SUM(VALORORIG - ISNULL(VALORPAG, 0)) as Valor
                FROM VW_DOC_FIN_PAG_ABERTO 
                WHERE DATAVENCIMENTO IS NOT NULL {whereAberto}
                GROUP BY FORMAT(DATAVENCIMENTO, 'MM/yyyy')
            ";

            var recebidosList = await connection.QueryAsync(sqlRecebidos, parameters);
            var pagosList = await connection.QueryAsync(sqlPagos, parameters);
            var recAbertoList = await connection.QueryAsync(sqlRecAberto, parameters);
            var pagAbertoList = await connection.QueryAsync(sqlPagAberto, parameters);

            var dictMap = new Dictionary<string, MonthlyFlowDto>();

            void AddToMap(IEnumerable<dynamic> list, Action<MonthlyFlowDto, decimal> action)
            {
                foreach (var item in list)
                {
                    if (item.Mes == null) continue;
                    string mes = item.Mes;
                    decimal valor = item.Valor != null ? (decimal)item.Valor : 0m;
                    
                    if (!dictMap.ContainsKey(mes))
                        dictMap[mes] = new MonthlyFlowDto { Mes = mes };
                        
                    action(dictMap[mes], valor);
                }
            }

            AddToMap(recebidosList, (dto, val) => dto.ValoresRecebidos = val);
            AddToMap(pagosList, (dto, val) => dto.ValoresPagos = val);
            AddToMap(recAbertoList, (dto, val) => dto.ValoresAVencer += val);
            AddToMap(pagAbertoList, (dto, val) => dto.ValoresAVencer -= val); // Pagar acts as negative pending flow

            // Sort by month/year logic (simple Parse)
            var result = new List<MonthlyFlowDto>(dictMap.Values);
            result.Sort((a, b) => 
            {
                DateTime da, db;
                bool sa = DateTime.TryParseExact(a.Mes, "MM/yyyy", null, System.Globalization.DateTimeStyles.None, out da);
                bool sb = DateTime.TryParseExact(b.Mes, "MM/yyyy", null, System.Globalization.DateTimeStyles.None, out db);
                if (sa && sb) return da.CompareTo(db);
                return string.Compare(a.Mes, b.Mes);
            });

            return result;
        }

        public async Task<IEnumerable<TopDebtorDto>> GetTopDebtorsAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null)
        {
            using var connection = CreateConnection();
            var parameters = new DynamicParameters();
            
            string whereClause = "";
            if (startDate.HasValue)
            {
                whereClause += " AND DATAVENCIMENTO >= @StartDate";
                parameters.Add("StartDate", startDate.Value);
            }
            if (endDate.HasValue)
            {
                whereClause += " AND DATAVENCIMENTO <= @EndDate";
                parameters.Add("EndDate", endDate.Value);
            }

            var sql = $@"
                SELECT TOP 5 
                    CLIENTE as Cliente, 
                    SUM(VALORORIG - ISNULL(VALORPAG, 0)) as ValorTotalEmAberto, 
                    COUNT(1) as QuantidadeParcelas 
                FROM VW_DOC_FIN_REC_ABERTO 
                WHERE 1=1 {whereClause}
                GROUP BY CLIENTE 
                ORDER BY ValorTotalEmAberto DESC;
            ";
            var result = await connection.QueryAsync<TopDebtorDto>(sql, parameters);
            return result;
        }

        public async Task<AiAnalysisDto> GetAiAnalysisDataAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null)
        {
            var summary = await GetSummaryAsync(tenantId, startDate, endDate);
            var topDebtors = await GetTopDebtorsAsync(tenantId, startDate, endDate);
            
            return new AiAnalysisDto
            {
                TotalReceberAberto = summary.TotalContasReceberAberto,
                TotalPagarAberto = summary.TotalContasPagarAberto,
                TotalRecebidoMes = summary.TotalRecebido,
                TotalPagoMes = summary.TotalPago,
                TopDevedores = topDebtors
            };
        }

        public async Task<AdvancedDashboardDto> GetAdvancedAnalyticsAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null)
        {
            using var connection = CreateConnection();
            var parameters = new DynamicParameters();
            parameters.Add("TenantId", tenantId);
            
            string dateFilterRec = "";
            string dateFilterPag = "";
            if (startDate.HasValue) 
            {
                dateFilterRec += " AND DATAVENCIMENTO >= @Start";
                dateFilterPag += " AND DATAPAGAMENTO >= @Start";
                parameters.Add("Start", startDate.Value);
            }
            if (endDate.HasValue)
            {
                dateFilterRec += " AND DATAVENCIMENTO <= @End";
                dateFilterPag += " AND DATAPAGAMENTO <= @End";
                parameters.Add("End", endDate.Value);
            }

            // 1. Aging (Atraso)
            var sqlAging = $@"
                SELECT 
                    CASE 
                        WHEN DATEDIFF(day, DATAVENCIMENTO, GETDATE()) <= 0 THEN 'A vencer'
                        WHEN DATEDIFF(day, DATAVENCIMENTO, GETDATE()) BETWEEN 1 AND 30 THEN '1-30 dias'
                        WHEN DATEDIFF(day, DATAVENCIMENTO, GETDATE()) BETWEEN 31 AND 60 THEN '31-60 dias'
                        WHEN DATEDIFF(day, DATAVENCIMENTO, GETDATE()) BETWEEN 61 AND 90 THEN '61-90 dias'
                        ELSE 'Mais de 90 dias'
                    END as Faixa,
                    SUM(VALORORIG - ISNULL(VALORPAG, 0)) as Valor
                FROM VW_DOC_FIN_REC_ABERTO
                WHERE 1=1 {dateFilterRec}
                GROUP BY 
                    CASE 
                        WHEN DATEDIFF(day, DATAVENCIMENTO, GETDATE()) <= 0 THEN 'A vencer'
                        WHEN DATEDIFF(day, DATAVENCIMENTO, GETDATE()) BETWEEN 1 AND 30 THEN '1-30 dias'
                        WHEN DATEDIFF(day, DATAVENCIMENTO, GETDATE()) BETWEEN 31 AND 60 THEN '31-60 dias'
                        WHEN DATEDIFF(day, DATAVENCIMENTO, GETDATE()) BETWEEN 61 AND 90 THEN '61-90 dias'
                        ELSE 'Mais de 90 dias'
                    END";

            // 2. Geográfico (UF)
            var sqlGeo = $@"
                SELECT TOP 10 UF as Local, SUM(VALORORIG) as Valor
                FROM VW_DOC_FIN_REC_ABERTO
                WHERE UF IS NOT NULL {dateFilterRec}
                GROUP BY UF ORDER BY Valor DESC";

            // 3. Distribuição Clientes (Pie)
            var sqlDist = $@"
                SELECT TOP 10 CLIENTE as Label, SUM(VALORORIG) as Valor
                FROM VW_DOC_FIN_REC_ABERTO
                WHERE 1=1 {dateFilterRec}
                GROUP BY CLIENTE ORDER BY Valor DESC";

            // 4. Performance (Prazo vs Atraso)
            var sqlPerformance = $@"
                SELECT 
                    CASE 
                        WHEN DATAPAGAMENTO <= DATAVENCIMENTO THEN 'No prazo'
                        ELSE 'Com atraso'
                    END as Categoria,
                    SUM(VALORPAG) as Valor
                FROM VW_DOC_FIN_REC_PAGO
                WHERE DATAPAGAMENTO IS NOT NULL {dateFilterPag}
                GROUP BY CASE WHEN DATAPAGAMENTO <= DATAVENCIMENTO THEN 'No prazo' ELSE 'Com atraso' END";

            // 5. Previsão de Caixa (Próximos 30 dias)
            var sqlProjection = $@"
                SELECT 
                    DATAVENCIMENTO as Data,
                    SUM(CASE WHEN Tipo = 'REC' THEN Valor ELSE 0 END) as Recebimentos,
                    SUM(CASE WHEN Tipo = 'PAG' THEN Valor ELSE 0 END) as Pagamentos
                FROM (
                    SELECT DATAVENCIMENTO, (VALORORIG - ISNULL(VALORPAG, 0)) as Valor, 'REC' as Tipo 
                    FROM VW_DOC_FIN_REC_ABERTO 
                    WHERE DATAVENCIMENTO >= CAST(GETDATE() AS DATE)
                    UNION ALL
                    SELECT DATAVENCIMENTO, (VALORORIG - ISNULL(VALORPAG, 0)) as Valor, 'PAG' as Tipo 
                    FROM VW_DOC_FIN_PAG_ABERTO
                    WHERE DATAVENCIMENTO >= CAST(GETDATE() AS DATE)
                ) t
                WHERE DATAVENCIMENTO <= DATEADD(day, 30, GETDATE())
                GROUP BY DATAVENCIMENTO ORDER BY DATAVENCIMENTO";

            var aging = await connection.QueryAsync<AgingDto>(sqlAging, parameters);
            var geo = await connection.QueryAsync<GeographicDto>(sqlGeo, parameters);
            var dist = await connection.QueryAsync<DistributionDto>(sqlDist, parameters);
            var perf = await connection.QueryAsync<PerformanceDto>(sqlPerformance, parameters);
            var projRaw = await connection.QueryAsync<dynamic>(sqlProjection, parameters);

            // Calculate KPIs
            var totalRec = await connection.ExecuteScalarAsync<decimal?>("SELECT SUM(VALORORIG) FROM VW_DOC_FIN_REC_ABERTO", parameters) ?? 0;
            var totalVencido = await connection.ExecuteScalarAsync<decimal?>("SELECT SUM(VALORORIG - ISNULL(VALORPAG,0)) FROM VW_DOC_FIN_REC_ABERTO WHERE DATAVENCIMENTO < GETDATE()", parameters) ?? 0;
            
            var inadimplencia = totalRec > 0 ? (totalVencido / totalRec) * 100 : 0;
            
            // Map Projection
            var projections = new List<CashProjectionDto>();
            foreach(var item in projRaw) {
                projections.Add(new CashProjectionDto {
                    Data = item.Data,
                    Recebimentos = (decimal)item.Recebimentos,
                    Pagamentos = (decimal)item.Pagamentos,
                    SaldoPrevisto = (decimal)item.Recebimentos - (decimal)item.Pagamentos
                });
            }

            return new AdvancedDashboardDto
            {
                Aging = aging,
                Geografico = geo,
                DistribuicaoReceber = dist,
                PerformanceRecebimento = perf,
                PrevisaoCaixa = projections,
                SaudeFinanceira = new FinancialHealthDto {
                    Score = 85,
                    Inadimplencia = inadimplencia,
                    DSO = 42,
                    ConcentracaoReceita = 18
                }
            };
        }
    }
}
