using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using IT4You.Application.FinanceAnalytics.DTOs;

namespace IT4You.Application.FinanceAnalytics.Interfaces
{
    public interface IFinanceAnalyticsService
    {
        Task<FinanceSummaryDto> GetSummaryAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null);
        Task<IEnumerable<MonthlyFlowDto>> GetMonthlyFlowAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null);
        Task<IEnumerable<TopDebtorDto>> GetTopDebtorsAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null);
        Task<AiAnalysisDto> GetAiAnalysisDataAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null);
        Task<AdvancedDashboardDto> GetAdvancedAnalyticsAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null);
        Task<IEnumerable<ChartQueryDetailsItemDto>> GetChartQueryDetailsAsync(int tenantId, FinanceRightsDto rights, IEnumerable<string> chartIds, DateTime? startDate = null, DateTime? endDate = null);
        Task<ChartMetricsResponseDto> GetChartMetricsAsync(int tenantId, FinanceRightsDto rights, ChartMetricsRequestDto request);

        Task<IEnumerable<Dictionary<string, object?>>> GetChartExportDatasetAsync(int tenantId, FinanceRightsDto rights, string chartId, DateTime? startDate = null, DateTime? endDate = null, string? entityValue = null);
        byte[] BuildCsv(IEnumerable<Dictionary<string, object?>> rows);
        Task<ChartDrilldownResponseDto> GetChartDrilldownAsync(int tenantId, FinanceRightsDto rights, ChartDrilldownRequestDto request);
        Task<ChartDrilldownResponseDto> GetChartDrilldownExportAsync(int tenantId, FinanceRightsDto rights, ChartDrilldownRequestDto request);
        byte[] BuildDrilldownExcel(string title, string startDateLabel, string endDateLabel, string selectionLabel, ChartDrilldownResponseDto response);
        byte[] BuildDrilldownPdf(string title, string startDateLabel, string endDateLabel, string selectionLabel, ChartDrilldownResponseDto response);
    }
}
