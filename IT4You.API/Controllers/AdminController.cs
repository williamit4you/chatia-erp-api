using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace IT4You.API.Controllers;

[Authorize(Roles = "TENANT_ADMIN,SUPER_ADMIN")]
[ApiController]
[Route("api/[controller]")]
public class AdminController : ControllerBase
{
    private readonly ITenantService _tenantService;

    public AdminController(ITenantService tenantService)
    {
        _tenantService = tenantService;
    }

    [HttpPut("settings")]
    public async Task<IActionResult> UpdateSettings([FromBody] UpdateSettingsRequest request)
    {
        var tenantId = User.FindFirst("tenantId")?.Value;
        if (string.IsNullOrEmpty(tenantId)) return Unauthorized();

        try
        {
            await _tenantService.UpdateSettingsAsync(tenantId, request);
            return Ok(new { message = "Settings updated successfully" });
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }
}
