using System.Collections.Generic;
using System.Threading.Tasks;
using IT4You.Application.SalesBudgetAnalytics.DTOs;

namespace IT4You.Application.SalesBudgetAnalytics.Interfaces;

public interface ISalesBudgetAnalyticsRepository
{
    Task<SalesBudgetKpiResponseDto> GetKpisAsync(SalesBudgetKpiRequestDto request);
    Task<SalesBudgetChartBatchResponseDto> GetChartsAsync(SalesBudgetChartBatchRequestDto request);
}
