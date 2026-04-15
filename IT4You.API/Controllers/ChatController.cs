using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using System.Security.Claims;

namespace IT4You.API.Controllers;

[Authorize]
[ApiController]
[Route("api/[controller]")]
public class ChatController : ControllerBase
{
    private readonly IChatService _chatService;
    private readonly IMemoryCache _cache;

    public ChatController(IChatService chatService, IMemoryCache cache)
    {
        _chatService = chatService;
        _cache = cache;
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] ChatRequest request)
    {
        try
        {
            // Get data from JWT claims
            var claims = User.Claims.Select(c => $"{c.Type}: {c.Value}").ToList();
            Console.WriteLine("Incoming Claims: " + string.Join(", ", claims));

            var userId = User.FindFirst("userId")?.Value;
            var tenantId = User.FindFirst("tenantId")?.Value;

            if (string.IsNullOrEmpty(userId) || string.IsNullOrEmpty(tenantId))
            {
                Console.WriteLine($"Unauthorized: Missing claims. UserId: {userId}, TenantId: {tenantId}");
                return Unauthorized(new { message = "Invalid session claims" });
            }

            var response = await _chatService.ProcessMessageAsync(request, userId, tenantId);
            return Ok(response);
        }
        catch (Exception ex)
        {
            Console.WriteLine("!!! EXCEPTION IN CHATCONTROLLER.POST: " + ex.Message);
            if (ex.InnerException != null) Console.WriteLine("Inner: " + ex.InnerException.Message);
            return BadRequest(new { message = ex.Message });
        }
    }

    [HttpPost("analyze-chart")]
    public async Task<IActionResult> AnalyzeChart([FromBody] ChartAnalysisRequest request)
    {
        try
        {
            var userId = User.FindFirst("userId")?.Value;
            var tenantId = User.FindFirst("tenantId")?.Value;

            if (string.IsNullOrEmpty(userId) || string.IsNullOrEmpty(tenantId))
                return Unauthorized(new { message = "Invalid session claims" });

            var response = await _chatService.ProcessChartAnalysisAsync(request, userId, tenantId);
            return Ok(response);
        }
        catch (Exception ex)
        {
            Console.WriteLine("!!! EXCEPTION IN CHATCONTROLLER.ANALYZECHART: " + ex.Message);
            return BadRequest(new { message = ex.Message });
        }
    }

    [HttpGet("sessions")]
    public async Task<IActionResult> GetSessions()
    {
        var userId = User.FindFirst("userId")?.Value;
        var tenantId = User.FindFirst("tenantId")?.Value;

        if (string.IsNullOrEmpty(userId) || string.IsNullOrEmpty(tenantId))
            return Unauthorized();

        var sessions = await _chatService.GetSessionsAsync(userId, tenantId);
        return Ok(sessions);
    }

    [HttpGet("messages/{sessionId}")]
    public async Task<IActionResult> GetMessages(string sessionId)
    {
        var messages = await _chatService.GetMessagesAsync(sessionId);
        return Ok(messages);
    }

    [HttpGet("sql-logs")]
    public async Task<IActionResult> GetSqlLogs([FromQuery] string? userId = null, [FromQuery] DateTime? startDate = null, [FromQuery] DateTime? endDate = null)
    {
        var currentUserId = User.FindFirst("userId")?.Value;
        var tenantId = User.FindFirst("tenantId")?.Value;

        if (string.IsNullOrEmpty(currentUserId) || string.IsNullOrEmpty(tenantId))
            return Unauthorized();

        // Check admin role
        var role = User.FindFirst(System.Security.Claims.ClaimTypes.Role)?.Value;
        var isAdmin = role == "TENANT_ADMIN" || role == "SUPER_ADMIN" || role == "ADMIN";
        if (!isAdmin)
            return Forbid();

        var logs = await _chatService.GetSqlLogsAsync(tenantId, userId, startDate, endDate);
        return Ok(logs);
    }

    [HttpDelete("sessions/{sessionId}")]
    public async Task<IActionResult> DeleteSession(string sessionId)
    {
        var tenantId = User.FindFirst("tenantId")?.Value;
        if (string.IsNullOrEmpty(tenantId))
            return Unauthorized();

        await _chatService.DeleteSessionAsync(sessionId, tenantId);
        return NoContent();
    }

    /// <summary>
    /// Download de relatório Excel gerado pelo ErpPlugin (export bypass).
    /// Requer JWT válido. O arquivo fica disponível por 30 minutos no cache.
    /// </summary>
    [HttpGet("export/{exportId}")]
    public IActionResult DownloadExport(string exportId)
    {
        if (string.IsNullOrWhiteSpace(exportId))
            return BadRequest(new { message = "exportId inválido." });

        if (!_cache.TryGetValue($"export:{exportId}", out byte[]? excelBytes) || excelBytes == null)
            return NotFound(new { message = "Relatório expirado ou não encontrado. Solicite a listagem novamente." });

        var fileName = $"relatorio_{exportId[..8]}_{DateTime.Now:yyyyMMdd_HHmmss}.xlsx";
        return File(
            excelBytes,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            fileName);
    }

    /// <summary>
    /// Download de relatório PDF gerado on-demand a partir dos dados brutos em cache.
    /// Requer JWT válido. Os dados ficam disponíveis por 30 minutos no cache.
    /// </summary>
    [HttpGet("export/{exportId}/pdf")]
    public IActionResult DownloadExportPdf(string exportId)
    {
        if (string.IsNullOrWhiteSpace(exportId))
            return BadRequest(new { message = "exportId inválido." });

        if (!_cache.TryGetValue($"export-data:{exportId}", out string? rawDataJson) || rawDataJson == null)
            return NotFound(new { message = "Dados do relatório expirados ou não encontrados. Solicite a listagem novamente." });

        // Desserializa os dados brutos e gera o PDF on-demand
        var rows = System.Text.Json.JsonSerializer.Deserialize<List<Dictionary<string, object>>>(rawDataJson)
                   ?? new List<Dictionary<string, object>>();

        // Recupera metadados de total/valor do cache de export de Excel (mesma chave base)
        // Como não temos metadados separados, recalculamos a partir dos dados
        int total = rows.Count;
        decimal valorTotal = 0;
        foreach (var row in rows)
        {
            if (row.TryGetValue("VALORORIG", out var v) && v is System.Text.Json.JsonElement je)
            {
                if (je.ValueKind == System.Text.Json.JsonValueKind.Number)
                    valorTotal += je.GetDecimal();
            }
        }

        var pdfBytes = IT4You.Application.Plugins.ErpPlugin.GerarPdf(rows, total, valorTotal);

        var fileName = $"relatorio_{exportId[..8]}_{DateTime.Now:yyyyMMdd_HHmmss}.pdf";
        return File(pdfBytes, "application/pdf", fileName);
    }
}

