using System;
using System.Threading.Tasks;
using IT4You.Application.FinanceAnalytics.DTOs;

namespace IT4You.Application.FinanceAnalytics.Interfaces
{
    public interface ICacheWarmingService
    {
        Task WarmUpAdvancedAnalyticsAsync(int tenantId, FinanceRightsDto rights, DateTime? startDate = null, DateTime? endDate = null);
        Task ClearAdvancedAnalyticsAsync(int tenantId, DateTime? startDate = null, DateTime? endDate = null);
    }
}
