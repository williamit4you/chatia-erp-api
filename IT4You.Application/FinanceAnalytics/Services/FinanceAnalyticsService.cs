using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using IT4You.Application.FinanceAnalytics.Interfaces;
using IT4You.Application.FinanceAnalytics.DTOs;
using System.Linq;
using System.Text;

namespace IT4You.Application.FinanceAnalytics.Services
{
    public class FinanceAnalyticsService : IFinanceAnalyticsService
    {
        private readonly IFinanceAnalyticsRepository _repository;

        public FinanceAnalyticsService(IFinanceAnalyticsRepository repository)
        {
            _repository = repository;
        }

        public async Task<FinanceSummaryDto> GetSummaryAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null)
        {
            return await _repository.GetSummaryAsync(tenantId, rights, startDate, endDate);
        }

        public async Task<IEnumerable<MonthlyFlowDto>> GetMonthlyFlowAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null)
        {
            return await _repository.GetMonthlyFlowAsync(tenantId, rights, startDate, endDate);
        }

        public async Task<IEnumerable<TopDebtorDto>> GetTopDebtorsAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null)
        {
            return await _repository.GetTopDebtorsAsync(tenantId, rights, startDate, endDate);
        }

        public async Task<AiAnalysisDto> GetAiAnalysisDataAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null)
        {
            return await _repository.GetAiAnalysisDataAsync(tenantId, rights, startDate, endDate);
        }

        public async Task<AdvancedDashboardDto> GetAdvancedAnalyticsAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null)
        {
            return await _repository.GetAdvancedAnalyticsAsync(tenantId, rights, startDate, endDate);
        }

        public async Task<IEnumerable<ChartQueryDetailsItemDto>> GetChartQueryDetailsAsync(int tenantId, FinanceRightsDto rights, IEnumerable<string> chartIds, DateTime? startDate = null, DateTime? endDate = null)
        {
            return await _repository.GetChartQueryDetailsAsync(tenantId, rights, chartIds, startDate, endDate);
        }

        public async Task<ChartMetricsResponseDto> GetChartMetricsAsync(int tenantId, FinanceRightsDto rights, ChartMetricsRequestDto request)
        {
            var chartIds = request?.ChartIds?
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Select(id => id.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList() ?? new();

            if (chartIds.Count == 0)
                return new ChartMetricsResponseDto();

            if (!request.StartDate.HasValue || !request.EndDate.HasValue)
                return new ChartMetricsResponseDto { Items = chartIds.Select(id => new ChartMetricsItemDto { ChartId = id }).ToList() };

            var start = request.StartDate.Value.Date;
            var end = request.EndDate.Value.Date;
            if (end < start)
            {
                var tmp = start;
                start = end;
                end = tmp;
            }

            var spanDays = (end - start).Days + 1;
            var prevEnd = start.AddDays(-1);
            var prevStart = prevEnd.AddDays(-(spanDays - 1));

            var needSummary = chartIds.Contains("summary", StringComparer.OrdinalIgnoreCase);
            var needFlow = chartIds.Contains("flow", StringComparer.OrdinalIgnoreCase);
            var needAdvanced = chartIds.Any(id => !id.Equals("summary", StringComparison.OrdinalIgnoreCase) && !id.Equals("flow", StringComparison.OrdinalIgnoreCase));

            FinanceSummaryDto? curSummary = null;
            FinanceSummaryDto? prevSummary = null;
            IEnumerable<MonthlyFlowDto>? curFlow = null;
            IEnumerable<MonthlyFlowDto>? prevFlow = null;
            AdvancedDashboardDto? curAdv = null;
            AdvancedDashboardDto? prevAdv = null;

            if (needSummary)
            {
                curSummary = await _repository.GetSummaryAsync(tenantId, rights, start, end);
                prevSummary = await _repository.GetSummaryAsync(tenantId, rights, prevStart, prevEnd);
            }

            if (needFlow)
            {
                curFlow = await _repository.GetMonthlyFlowAsync(tenantId, rights, start, end);
                prevFlow = await _repository.GetMonthlyFlowAsync(tenantId, rights, prevStart, prevEnd);
            }

            if (needAdvanced)
            {
                curAdv = await _repository.GetAdvancedAnalyticsAsync(tenantId, rights, start, end);
                prevAdv = await _repository.GetAdvancedAnalyticsAsync(tenantId, rights, prevStart, prevEnd);
            }

            static decimal Sum<T>(IEnumerable<T>? items, Func<T, decimal> selector)
                => items?.Sum(selector) ?? 0m;

            static decimal LastBy<T>(IEnumerable<T>? items, Func<T, DateTime> by, Func<T, decimal> selector)
            {
                if (items == null) return 0m;
                var last = items.OrderBy(by).LastOrDefault();
                return last == null ? 0m : selector(last);
            }

            decimal Extract(string chartId, FinanceSummaryDto? summary, IEnumerable<MonthlyFlowDto>? flow, AdvancedDashboardDto? adv)
            {
                switch (chartId)
                {
                    case "summary":
                        return summary?.SaldoProjetado ?? 0m;
                    case "flow":
                        return (flow?.Sum(x => x.ValoresRecebidos) ?? 0m) - (flow?.Sum(x => x.ValoresPagos) ?? 0m);
                    case "projection":
                        return LastBy(adv?.PrevisaoCaixa, x => x.Data, x => x.SaldoPrevisto);
                    case "aging":
                        return Sum(adv?.Aging, x => x.Valor);
                    case "performance":
                        return Sum(adv?.PerformanceRecebimento, x => x.Valor);
                    case "dist_pag_fornecedor":
                        return Sum(adv?.DistribuicaoPagarFornecedor, x => x.Valor);
                    case "geo_pagar":
                        return Sum(adv?.GeograficoPagar, x => x.Valor);
                    case "dist_tipo_pag":
                        return Sum(adv?.DistribuicaoTipoPagamento, x => x.Valor);
                    case "dist_cond_pag":
                        return Sum(adv?.DistribuicaoCondicaoPagamento, x => x.Valor);
                    case "evolucao_pag":
                        return Sum(adv?.EvolucaoMensalPagamento, x => x.Valor);
                    case "curva_pag":
                        return Sum(adv?.CurvaVencimentoPagar, x => x.Valor);
                    case "top_pag":
                        return Sum(adv?.TopContasPagar, x => x.Valor);
                    case "faixa_pag":
                        return Sum(adv?.DistribuicaoFaixaValorPagar, x => x.Valor);
                    case "dist_rec_cliente":
                        return Sum(adv?.DistribuicaoReceberCliente, x => x.Valor);
                    case "geo_receber":
                        return Sum(adv?.GeograficoReceber, x => x.Valor);
                    case "evolucao_rec":
                        return Sum(adv?.EvolucaoMensalRecebimento, x => x.Valor);
                    case "curva_rec":
                        return Sum(adv?.CurvaVencimentoReceber, x => x.Valor);
                    case "top_rec":
                        return Sum(adv?.TopContasReceber, x => x.Valor);
                    case "faixa_rec":
                        return Sum(adv?.DistribuicaoFaixaValorReceber, x => x.Valor);
                    case "vol_dia_mes":
                        return Sum(adv?.VolumePorDia, x => x.Valor);
                    case "vol_dia_semana":
                        return Sum(adv?.VolumePorDiaSemana, x => x.Valor);
                    case "liq_empresa":
                        return adv?.IndiceLiquidezPorEmpresa?.Any() == true ? adv.IndiceLiquidezPorEmpresa.Average(x => x.Valor) : 0m;
                    case "fluxo_diario_proj":
                        return Sum(adv?.FluxoCaixaDiarioProjetado, x => x.Valor);
                    case "vol_cpf_cnpj":
                        return Sum(adv?.VolumePorCpfCnpj, x => x.Valor);
                    case "saldo_acumulado":
                        return LastBy(adv?.EvolucaoSaldo, x => x.Data, x => x.SaldoAcumulado);
                    case "dist_faixa_prazo":
                        return Sum(adv?.DistribuicaoFaixaPrazoVencimento, x => x.Valor);
                    case "pm_rec_cli":
                        return adv?.PrazoMedioRecebimentoPorCliente?.Any() == true ? adv.PrazoMedioRecebimentoPorCliente.Average(x => x.Valor) : 0m;
                    case "pm_pag_for":
                        return adv?.PrazoMedioPagamentoPorFornecedor?.Any() == true ? adv.PrazoMedioPagamentoPorFornecedor.Average(x => x.Valor) : 0m;
                    case "tm_rec_cli":
                        return adv?.TicketMedioPorCliente?.Any() == true ? adv.TicketMedioPorCliente.Average(x => x.Valor) : 0m;
                    case "tm_pag_for":
                        return adv?.TicketMedioPorFornecedor?.Any() == true ? adv.TicketMedioPorFornecedor.Average(x => x.Valor) : 0m;
                    case "docs_cli":
                        return Sum(adv?.DocumentosPorClienteAtivo, x => x.Valor);
                    case "docs_for":
                        return Sum(adv?.DocumentosPorFornecedorAtivo, x => x.Valor);
                    case "kpis":
                    case "efficiency_kpis":
                        return adv?.SaudeFinanceira?.Score ?? 0m;
                    default:
                        return 0m;
                }
            }

            var items = new List<ChartMetricsItemDto>();
            foreach (var chartId in chartIds)
            {
                var currentValue = Extract(chartId, curSummary, curFlow, curAdv);
                var previousValue = Extract(chartId, prevSummary, prevFlow, prevAdv);
                var deltaAbs = currentValue - previousValue;
                decimal? deltaPct = null;
                if (previousValue != 0m)
                {
                    deltaPct = deltaAbs / previousValue;
                }

                var direction = deltaAbs > 0m ? "up" : deltaAbs < 0m ? "down" : "flat";
                items.Add(new ChartMetricsItemDto
                {
                    ChartId = chartId,
                    CurrentValue = currentValue,
                    PreviousValue = previousValue,
                    DeltaAbs = deltaAbs,
                    DeltaPct = deltaPct,
                    Direction = direction,
                });
            }

            return new ChartMetricsResponseDto { Items = items };
        }

        public async Task<IEnumerable<Dictionary<string, object?>>> GetChartExportDatasetAsync(int tenantId, FinanceRightsDto rights, string chartId, DateTime? startDate = null, DateTime? endDate = null, string? entityValue = null)
        {
            return await _repository.GetChartExportDatasetAsync(tenantId, rights, chartId, startDate, endDate, entityValue);
        }

        public byte[] BuildCsv(IEnumerable<Dictionary<string, object?>> rows)
        {
            var list = rows?.ToList() ?? new();
            var headers = list
                .SelectMany(r => r.Keys)
                .Distinct()
                .ToList();

            string Escape(string value)
            {
                if (value == null) return "";
                var mustQuote = value.Contains('"') || value.Contains('\n') || value.Contains('\r') || value.Contains(';');
                var escaped = value.Replace("\"", "\"\"");
                return mustQuote ? $"\"{escaped}\"" : escaped;
            }

            var sb = new StringBuilder();
            sb.AppendLine(string.Join(";", headers.Select(Escape)));
            foreach (var row in list)
            {
                sb.AppendLine(string.Join(";", headers.Select(h => Escape(row.TryGetValue(h, out var v) ? (v?.ToString() ?? "") : ""))));
            }

            // UTF-8 with BOM to improve Excel compatibility in pt-BR locales.
            var preamble = Encoding.UTF8.GetPreamble();
            var body = Encoding.UTF8.GetBytes(sb.ToString());
            var result = new byte[preamble.Length + body.Length];
            Buffer.BlockCopy(preamble, 0, result, 0, preamble.Length);
            Buffer.BlockCopy(body, 0, result, preamble.Length, body.Length);
            return result;
        }

        public async Task<ChartDrilldownResponseDto> GetChartDrilldownAsync(int tenantId, FinanceRightsDto rights, ChartDrilldownRequestDto request)
        {
            return await _repository.GetChartDrilldownAsync(tenantId, rights, request);
        }
    }
}
