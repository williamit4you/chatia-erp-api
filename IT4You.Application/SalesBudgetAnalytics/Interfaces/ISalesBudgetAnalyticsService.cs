using System.Threading.Tasks;
using IT4You.Application.SalesBudgetAnalytics.DTOs;

namespace IT4You.Application.SalesBudgetAnalytics.Interfaces;

public interface ISalesBudgetAnalyticsService
{
    Task<SalesBudgetCatalogResponseDto> GetCatalogAsync();
    Task<SalesBudgetKpiResponseDto> GetKpisAsync(SalesBudgetKpiRequestDto request);
    Task<SalesBudgetChartBatchResponseDto> GetChartsAsync(SalesBudgetChartBatchRequestDto request);
}
