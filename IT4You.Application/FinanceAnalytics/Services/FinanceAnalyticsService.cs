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
