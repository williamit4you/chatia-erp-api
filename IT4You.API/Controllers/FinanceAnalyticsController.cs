using IT4You.Application.FinanceAnalytics.Interfaces;
using IT4You.Application.FinanceAnalytics.DTOs;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;
using System.Security.Claims;

namespace IT4You.API.Controllers
{
    [ApiController]
    [Route("api/finance-analytics")]
    [Authorize] // Assuming all routes here require a logged-in user
    public class FinanceAnalyticsController : ControllerBase
    {
        private readonly IFinanceAnalyticsService _financeAnalyticsService;

        public FinanceAnalyticsController(IFinanceAnalyticsService financeAnalyticsService)
        {
            _financeAnalyticsService = financeAnalyticsService;
        }

        private int GetTenantId()
        {
            var tenantClaim = User.FindFirst("TenantId")?.Value;
            if (int.TryParse(tenantClaim, out int tenantId))
                return tenantId;
            return 0; // Default to 0, which likely won't return data or will be handled by Db
        }

        private FinanceRightsDto GetFinanceRights()
        {
            var user = User;
            bool isFullAdmin = user.IsInRole("TENANT_ADMIN") || user.IsInRole("SUPER_ADMIN");
            
            if (isFullAdmin) 
                return new FinanceRightsDto(true, true, true);

            return new FinanceRightsDto(
                HasPayableDashboardAccess: user.FindFirst("hasPayableDashboardAccess")?.Value == "true",
                HasReceivableDashboardAccess: user.FindFirst("hasReceivableDashboardAccess")?.Value == "true",
                HasBankingDashboardAccess: user.FindFirst("hasBankingDashboardAccess")?.Value == "true"
            );
        }

        [HttpGet("summary")]
        public async Task<IActionResult> GetSummary([FromQuery] DateTime? startDate = null, [FromQuery] DateTime? endDate = null)
        {
            var tenantId = GetTenantId();
            var rights = GetFinanceRights();
            var data = await _financeAnalyticsService.GetSummaryAsync(tenantId, rights, startDate, endDate);
            return Ok(data);
        }

        [HttpGet("monthly-flow")]
        public async Task<IActionResult> GetMonthlyFlow([FromQuery] DateTime? startDate = null, [FromQuery] DateTime? endDate = null)
        {
            var tenantId = GetTenantId();
            var rights = GetFinanceRights();
            var data = await _financeAnalyticsService.GetMonthlyFlowAsync(tenantId, rights, startDate, endDate);
            return Ok(data);
        }

        [HttpGet("top-debtors")]
        public async Task<IActionResult> GetTopDebtors([FromQuery] DateTime? startDate = null, [FromQuery] DateTime? endDate = null)
        {
            var tenantId = GetTenantId();
            var rights = GetFinanceRights();
            var data = await _financeAnalyticsService.GetTopDebtorsAsync(tenantId, rights, startDate, endDate);
            return Ok(data);
        }

        [HttpGet("ai-analysis")]
        public async Task<IActionResult> GetAiAnalysis([FromQuery] DateTime? startDate = null, [FromQuery] DateTime? endDate = null)
        {
            var tenantId = GetTenantId();
            var rights = GetFinanceRights();
            var data = await _financeAnalyticsService.GetAiAnalysisDataAsync(tenantId, rights, startDate, endDate);
            return Ok(data);
        }

        [HttpGet("advanced-analytics")]
        public async Task<IActionResult> GetAdvancedAnalytics([FromQuery] DateTime? startDate = null, [FromQuery] DateTime? endDate = null)
        {
            var tenantId = GetTenantId();
            var rights = GetFinanceRights();
            var data = await _financeAnalyticsService.GetAdvancedAnalyticsAsync(tenantId, rights, startDate, endDate);
            return Ok(data);
        }
    }
}
