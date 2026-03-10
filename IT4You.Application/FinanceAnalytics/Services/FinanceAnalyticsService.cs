using System.Collections.Generic;
using System.Threading.Tasks;
using IT4You.Application.FinanceAnalytics.Interfaces;
using IT4You.Application.FinanceAnalytics.DTOs;

namespace IT4You.Application.FinanceAnalytics.Services
{
    public class FinanceAnalyticsService : IFinanceAnalyticsService
    {
        private readonly IFinanceAnalyticsRepository _repository;

        public FinanceAnalyticsService(IFinanceAnalyticsRepository repository)
        {
            _repository = repository;
        }

        public async Task<FinanceSummaryDto> GetSummaryAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null)
        {
            return await _repository.GetSummaryAsync(tenantId, startDate, endDate);
        }

        public async Task<IEnumerable<MonthlyFlowDto>> GetMonthlyFlowAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null)
        {
            return await _repository.GetMonthlyFlowAsync(tenantId, startDate, endDate);
        }

        public async Task<IEnumerable<TopDebtorDto>> GetTopDebtorsAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null)
        {
            return await _repository.GetTopDebtorsAsync(tenantId, startDate, endDate);
        }

        public async Task<AiAnalysisDto> GetAiAnalysisDataAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null)
        {
            return await _repository.GetAiAnalysisDataAsync(tenantId, startDate, endDate);
        }

        public async Task<AdvancedDashboardDto> GetAdvancedAnalyticsAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null)
        {
            return await _repository.GetAdvancedAnalyticsAsync(tenantId, startDate, endDate);
        }
    }
}
