using System;
using IT4You.Application.FinanceAnalytics.Interfaces;
using IT4You.Application.FinanceAnalytics.DTOs;
using IT4You.Application.Services;

namespace IT4You.Application.FinanceAnalytics.Services
{
    public class FinanceAnalyticsService : IFinanceAnalyticsService
    {
        private readonly IFinanceAnalyticsRepository _repository;
        private readonly RedisCacheService _cache;
        private readonly ICacheWarmingService _warmingService;

        public FinanceAnalyticsService(
            IFinanceAnalyticsRepository repository, 
            RedisCacheService cache,
            ICacheWarmingService warmingService)
        {
            _repository = repository;
            _cache = cache;
            _warmingService = warmingService;
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
            var start = startDate?.ToString("yyyyMMdd") ?? "null";
            var end = endDate?.ToString("yyyyMMdd") ?? "null";
            var cacheKey = $"finance:advanced:{tenantId}:{start}:{end}";

            // 1. Try to get from Redis
            var cachedResult = await _cache.GetAsync<AdvancedDashboardDto>(cacheKey);
            if (cachedResult != null)
            {
                return cachedResult;
            }

            // 2. If not in Redis, warm it up (this will call repository and save to Redis)
            await _warmingService.WarmUpAdvancedAnalyticsAsync(tenantId, rights, startDate, endDate);

            // 3. Return from Redis (now it's guaranteed to be there unless SQL failed)
            return await _cache.GetAsync<AdvancedDashboardDto>(cacheKey) 
                   ?? await _repository.GetAdvancedAnalyticsAsync(tenantId, rights, startDate, endDate);
        }
    }
}
