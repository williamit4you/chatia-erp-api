using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.Application.Data;
using IT4You.Domain.Entities;
using Pgvector;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace IT4You.API.Controllers;

[Authorize(Roles = "TENANT_ADMIN,SUPER_ADMIN")]
[ApiController]
[Route("api/[controller]")]
public class AdminController : ControllerBase
{
    private readonly ITenantService _tenantService;
    private readonly AppDbContext _context;

    public AdminController(ITenantService tenantService, AppDbContext context)
    {
        _tenantService = tenantService;
        _context = context;
    }

    [HttpGet("settings")]
    public async Task<IActionResult> GetSettings()
    {
        var tenantId = User.FindFirst("tenantId")?.Value;
        if (string.IsNullOrEmpty(tenantId) && !User.IsInRole("SUPER_ADMIN")) return Unauthorized();

        var tenant = await _tenantService.GetTenantAsync(tenantId);
        if (tenant == null) return NotFound();

        return Ok(new { tenant.IaToken, tenant.ErpToken });
    }

    [HttpPut("settings")]
    public async Task<IActionResult> UpdateSettings([FromBody] UpdateSettingsRequest request)
    {
        var tenantId = User.FindFirst("tenantId")?.Value;
        if (string.IsNullOrEmpty(tenantId) && !User.IsInRole("SUPER_ADMIN")) return Unauthorized();

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

    [HttpGet("users")]
    public async Task<IActionResult> GetUsers()
    {
        var tenantId = User.FindFirst("tenantId")?.Value;
        if (string.IsNullOrEmpty(tenantId) && !User.IsInRole("SUPER_ADMIN")) return Unauthorized();

        if (User.IsInRole("SUPER_ADMIN") && string.IsNullOrEmpty(tenantId))
        {
            // SuperAdmin wanting to see all users in the admin screen?
            // Actually, usually they see this per tenant, but if they are here, let's show all.
            var allTenants = await _tenantService.GetAllTenantsAsync();
            var allUsers = allTenants.SelectMany(t => t.Users ?? new List<UserDto>());
            return Ok(allUsers);
        }

        var tenant = await _tenantService.GetTenantAsync(tenantId);
        if (tenant == null) return NotFound();

        return Ok(tenant.Users);
    }

    [HttpPost("users")]
    public async Task<IActionResult> CreateUser([FromBody] CreateUserRequest request)
    {
        var tenantId = User.FindFirst("tenantId")?.Value;
        if (string.IsNullOrEmpty(tenantId) && !User.IsInRole("SUPER_ADMIN")) return Unauthorized();

        try
        {
            await _tenantService.CreateUserAsync(tenantId, request);
            return Ok(new { success = true });
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }

    [HttpPut("users/{userId}")]
    public async Task<IActionResult> UpdateUser(string userId, [FromBody] UpdateUserRequest request)
    {
        var tenantId = User.FindFirst("tenantId")?.Value;
        if (string.IsNullOrEmpty(tenantId) && !User.IsInRole("SUPER_ADMIN")) return Unauthorized();

        try
        {
            await _tenantService.UpdateUserAsync(tenantId, userId, request);
            return Ok(new { success = true });
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }
}
