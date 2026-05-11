using IT4You.Application.FinanceAnalytics.Interfaces;
using IT4You.Application.FinanceAnalytics.DTOs;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Threading.Tasks;
using System.Security.Claims;
using System.Linq;

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
            var tenantClaim = User.FindFirst("tenantId")?.Value ?? User.FindFirst("TenantId")?.Value;
            if (int.TryParse(tenantClaim, out int tenantId))
                return tenantId;
            return 0; // Default to 0, which likely won't return data or will be handled by Db
        }

        private bool HasAnyRole(params string[] roles)
        {
            var role = User.FindFirst(ClaimTypes.Role)?.Value
                ?? User.FindFirst("role")?.Value
                ?? string.Empty;

            role = role.Trim();
            return roles.Any(r => string.Equals(role, r, StringComparison.OrdinalIgnoreCase));
        }

        private FinanceRightsDto GetFinanceRights()
        {
            var user = User;
            bool isFullAdmin = user.IsInRole("TENANT_ADMIN") || user.IsInRole("SUPER_ADMIN") || HasAnyRole("TENANT_ADMIN", "SUPER_ADMIN");
            
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

        [HttpPost("chart-query-details")]
        public async Task<IActionResult> GetChartQueryDetails([FromBody] ChartQueryDetailsRequestDto request)
        {
            var canSeeSql = User.IsInRole("TENANT_ADMIN") || User.IsInRole("SUPER_ADMIN") || HasAnyRole("TENANT_ADMIN", "SUPER_ADMIN");

            var tenantId = GetTenantId();
            var rights = GetFinanceRights();
            var chartIds = request?.ChartIds?.Where(id => !string.IsNullOrWhiteSpace(id)).Distinct().ToList() ?? new();

            var items = (await _financeAnalyticsService.GetChartQueryDetailsAsync(tenantId, rights, chartIds, request?.StartDate, request?.EndDate)).ToList();

            // "Detalhes dos gráficos" é visível para todos os usuários autenticados,
            // porém as consultas SQL (SELECT/query) ficam restritas a SUPER_ADMIN e TENANT_ADMIN.
            if (!canSeeSql)
            {
                foreach (var item in items)
                {
                    var hadSql = item.SqlQueries?.Count > 0;
                    item.SqlQueries = new();
                    if (hadSql)
                    {
                        item.Rules.Add("SQL oculto: disponível apenas para TENANT_ADMIN e SUPER_ADMIN.");
                    }
                }
            }

            return Ok(new ChartQueryDetailsResponseDto { Items = items });
        }

        [HttpPost("charts/metrics")]
        public async Task<IActionResult> GetChartMetrics([FromBody] ChartMetricsRequestDto request)
        {
            var tenantId = GetTenantId();
            var rights = GetFinanceRights();

            var chartIds = request?.ChartIds?.Where(id => !string.IsNullOrWhiteSpace(id)).Distinct().ToList() ?? new();
            if (chartIds.Count == 0)
                return Ok(new ChartMetricsResponseDto { Items = new() });

            var response = await _financeAnalyticsService.GetChartMetricsAsync(tenantId, rights, request);
            return Ok(response);
        }

        [HttpPost("charts/export")]
        public async Task<IActionResult> ExportChart([FromBody] ChartExportRequestDto request)
        {
            var tenantId = GetTenantId();
            var rights = GetFinanceRights();

            if (request == null || string.IsNullOrWhiteSpace(request.ChartId))
                return BadRequest(new { message = "ChartId é obrigatório." });

            if (!string.Equals(request.Format, "csv", StringComparison.OrdinalIgnoreCase))
                return BadRequest(new { message = "Formato não suportado. Use 'csv'." });

            var rows = await _financeAnalyticsService.GetChartExportDatasetAsync(
                tenantId,
                rights,
                request.ChartId.Trim(),
                request.StartDate,
                request.EndDate,
                request.EntityValue
            );

            var csvBytes = _financeAnalyticsService.BuildCsv(rows);
            var fileName = $"finance-{request.ChartId}-{DateTime.UtcNow:yyyyMMddHHmmss}.csv";
            return File(csvBytes, "text/csv; charset=utf-8", fileName);
        }

        [HttpPost("charts/drilldown")]
        public async Task<IActionResult> Drilldown([FromBody] ChartDrilldownRequestDto request)
        {
            var tenantId = GetTenantId();
            var rights = GetFinanceRights();

            if (request == null || string.IsNullOrWhiteSpace(request.ChartId))
                return BadRequest(new { message = "ChartId é obrigatório." });

            if (request.Selection == null || string.IsNullOrWhiteSpace(request.Selection.Kind))
                return BadRequest(new { message = "Selection.kind é obrigatório." });

            request.Page = request.Page <= 0 ? 1 : request.Page;
            request.PageSize = request.PageSize <= 0 ? 50 : Math.Min(request.PageSize, 200);

            var response = await _financeAnalyticsService.GetChartDrilldownAsync(tenantId, rights, request);
            return Ok(response);
        }

        [HttpPost("charts/drilldown/export")]
        public async Task<IActionResult> ExportDrilldown([FromBody] ChartDrilldownExportRequestDto request)
        {
            var tenantId = GetTenantId();
            var rights = GetFinanceRights();

            if (request == null || string.IsNullOrWhiteSpace(request.ChartId))
                return BadRequest(new { message = "ChartId e obrigatorio." });

            if (request.Selection == null || string.IsNullOrWhiteSpace(request.Selection.Kind))
                return BadRequest(new { message = "Selection.kind e obrigatorio." });

            var format = (request.Format ?? "xlsx").Trim().ToLowerInvariant();
            if (format != "xlsx" && format != "pdf")
                return BadRequest(new { message = "Formato nao suportado. Use 'xlsx' ou 'pdf'." });

            var response = await _financeAnalyticsService.GetChartDrilldownExportAsync(tenantId, rights, request);

            string selectionLabel = request.Selection.Label
                ?? request.Selection.Key
                ?? request.Selection.Value
                ?? request.Selection.Uf
                ?? "recorte";

            var title = $"Drilldown - {request.ChartId}";
            var startDateLabel = request.StartDate?.ToString("yyyy-MM-dd") ?? "-";
            var endDateLabel = request.EndDate?.ToString("yyyy-MM-dd") ?? "-";
            var safeChartId = request.ChartId.Trim().Replace(" ", "-").ToLowerInvariant();
            var timestamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss");

            if (format == "pdf")
            {
                var pdfBytes = _financeAnalyticsService.BuildDrilldownPdf(title, startDateLabel, endDateLabel, selectionLabel, response);
                return File(pdfBytes, "application/pdf", $"drilldown-{safeChartId}-{timestamp}.pdf");
            }

            var xlsxBytes = _financeAnalyticsService.BuildDrilldownExcel(title, startDateLabel, endDateLabel, selectionLabel, response);
            return File(
                xlsxBytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                $"drilldown-{safeChartId}-{timestamp}.xlsx");
        }
    }
}
