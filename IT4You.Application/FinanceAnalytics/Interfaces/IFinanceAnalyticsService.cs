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
    }
}
