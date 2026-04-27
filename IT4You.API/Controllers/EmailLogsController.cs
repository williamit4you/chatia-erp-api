using IT4You.Application.Interfaces;
using IT4You.Domain.Entities;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace IT4You.API.Controllers;

[Authorize(Roles = "SUPER_ADMIN")]
[ApiController]
[Route("api/email-logs")]
public class EmailLogsController : ControllerBase
{
    private readonly IEmailLogService _logs;

    public EmailLogsController(IEmailLogService logs)
    {
        _logs = logs;
    }

    [HttpGet]
    public async Task<IActionResult> Get(
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 20,
        [FromQuery] EmailLogStatus? status = null,
        [FromQuery] string? templateKey = null,
        [FromQuery] string? toEmail = null,
        [FromQuery] string? tenantId = null,
        [FromQuery] string? requestedByUserId = null,
        [FromQuery] DateTime? from = null,
        [FromQuery] DateTime? to = null,
        [FromQuery] string? sortBy = null,
        [FromQuery] string? sortDirection = "desc")
    {
        return Ok(await _logs.SearchAsync(page, pageSize, status, templateKey, toEmail, tenantId, requestedByUserId, from, to, sortBy, sortDirection));
    }
}

