using System.Collections.Generic;
using System.Threading.Tasks;
using IT4You.Application.FinanceAnalytics.DTOs;

namespace IT4You.Application.FinanceAnalytics.Interfaces
{
    public interface IFinanceAnalyticsRepository
    {
        Task<IEnumerable<FinanceCompanyOptionDto>> GetCompaniesAsync(int tenantId);
        Task<FinanceSummaryDto> GetSummaryAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null, IEnumerable<string>? companyIds = null);
        Task<IEnumerable<MonthlyFlowDto>> GetMonthlyFlowAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null, IEnumerable<string>? companyIds = null);
        Task<IEnumerable<TopDebtorDto>> GetTopDebtorsAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null, IEnumerable<string>? companyIds = null);
        Task<AiAnalysisDto> GetAiAnalysisDataAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null, IEnumerable<string>? companyIds = null);
        Task<AdvancedDashboardDto> GetAdvancedAnalyticsAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null, IEnumerable<string>? companyIds = null);
        Task<IEnumerable<ChartQueryDetailsItemDto>> GetChartQueryDetailsAsync(int tenantId, FinanceRightsDto rights, IEnumerable<string> chartIds, DateTime? startDate = null, DateTime? endDate = null, IEnumerable<string>? companyIds = null);

        Task<IEnumerable<Dictionary<string, object?>>> GetChartExportDatasetAsync(int tenantId, FinanceRightsDto rights, string chartId, DateTime? startDate = null, DateTime? endDate = null, string? entityValue = null, IEnumerable<string>? companyIds = null);
        Task<ChartDrilldownResponseDto> GetChartDrilldownAsync(int tenantId, FinanceRightsDto rights, ChartDrilldownRequestDto request);
    }
}
