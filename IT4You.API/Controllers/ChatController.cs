using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Security.Claims;

namespace IT4You.API.Controllers;

[Authorize]
[ApiController]
[Route("api/[controller]")]
public class ChatController : ControllerBase
{
    private readonly IChatService _chatService;

    public ChatController(IChatService chatService)
    {
        _chatService = chatService;
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
}
