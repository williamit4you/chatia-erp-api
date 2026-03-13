using System;
using System.Threading.Tasks;
using IT4You.Application.FinanceAnalytics.Interfaces;
using IT4You.Application.FinanceAnalytics.DTOs;
using IT4You.Application.Services;

namespace IT4You.Application.FinanceAnalytics.Services
{
    public class CacheWarmingService : ICacheWarmingService
    {
        private readonly IFinanceAnalyticsRepository _repository;
        private readonly RedisCacheService _cache;

        public CacheWarmingService(IFinanceAnalyticsRepository repository, RedisCacheService cache)
        {
            _repository = repository;
            _cache = cache;
        }

        public async Task WarmUpAdvancedAnalyticsAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null)
        {
            var cacheKey = GetAdvancedAnalyticsCacheKey(tenantId, startDate, endDate);
            
            // Execute heavy SQL query
            var result = await _repository.GetAdvancedAnalyticsAsync(tenantId, rights, startDate, endDate);
            
            // Save to Redis (Expiration: 24 hours for dashboard data)
            await _cache.SetAsync(cacheKey, result, 1440); 
        }

        public async Task ClearAdvancedAnalyticsAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null)
        {
            var cacheKey = GetAdvancedAnalyticsCacheKey(tenantId, startDate, endDate);
            await _cache.RemoveAsync(cacheKey);
        }

        private string GetAdvancedAnalyticsCacheKey(int tenantId, DateTime? startDate, DateTime? endDate)
        {
            var start = startDate?.ToString("yyyyMMdd") ?? "null";
            var end = endDate?.ToString("yyyyMMdd") ?? "null";
            return $"finance:advanced:{tenantId}:{start}:{end}";
        }
    }
}
