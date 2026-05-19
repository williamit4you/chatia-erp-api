using System;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using IT4You.Application.SalesBudgetAnalytics.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace IT4You.API.Controllers
{
    [ApiController]
    [Route("api/sales-budget-analytics")]
    [Authorize]
    public class SalesBudgetAnalyticsController : ControllerBase
    {
        private readonly ISalesBudgetAnalyticsService _salesBudgetAnalyticsService;

        public SalesBudgetAnalyticsController(ISalesBudgetAnalyticsService salesBudgetAnalyticsService)
        {
            _salesBudgetAnalyticsService = salesBudgetAnalyticsService;
        }

        private bool HasAnyRole(params string[] roles)
        {
            var role = User.FindFirst(ClaimTypes.Role)?.Value
                ?? User.FindFirst("role")?.Value
                ?? string.Empty;

            role = role.Trim();
            return roles.Any(r => string.Equals(role, r, StringComparison.OrdinalIgnoreCase));
        }

        private bool HasSalesBudgetDashboardAccess()
        {
            if (User.IsInRole("TENANT_ADMIN") || User.IsInRole("SUPER_ADMIN") || HasAnyRole("TENANT_ADMIN", "SUPER_ADMIN"))
                return true;

            return User.FindFirst("hasBudgetDashboardAccess")?.Value == "true";
        }

        [HttpGet("catalog")]
        public async Task<IActionResult> GetCatalog()
        {
            if (!HasSalesBudgetDashboardAccess())
                return Forbid();

            var data = await _salesBudgetAnalyticsService.GetCatalogAsync();
            return Ok(data);
        }

        [HttpPost("kpis")]
        public async Task<IActionResult> GetKpis([FromBody] IT4You.Application.SalesBudgetAnalytics.DTOs.SalesBudgetKpiRequestDto request)
        {
            if (!HasSalesBudgetDashboardAccess())
                return Forbid();

            var data = await _salesBudgetAnalyticsService.GetKpisAsync(request);
            return Ok(data);
        }

        [HttpPost("charts/batch")]
        public async Task<IActionResult> GetCharts([FromBody] IT4You.Application.SalesBudgetAnalytics.DTOs.SalesBudgetChartBatchRequestDto request)
        {
            if (!HasSalesBudgetDashboardAccess())
                return Forbid();

            var data = await _salesBudgetAnalyticsService.GetChartsAsync(request);
            return Ok(data);
        }
    }
}
