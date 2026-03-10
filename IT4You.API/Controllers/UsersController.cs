using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace IT4You.API.Controllers;

[Authorize]
[ApiController]
[Route("api/[controller]")]
public class UsersController : ControllerBase
{
    private readonly IUserService _userService;

    public UsersController(IUserService userService)
    {
        _userService = userService;
    }

    private string GetTenantId() => User.FindFirst("tenantId")?.Value ?? string.Empty;
    private string GetUserId() => User.FindFirst("userId")?.Value ?? string.Empty;
    private string GetRole() => User.FindFirst("role")?.Value ?? string.Empty;

    [HttpGet("me")]
    public async Task<IActionResult> GetMe()
    {
        var userId = GetUserId();
        var user = await _userService.GetUserByIdAsync(userId);
        if (user == null) return NotFound();
        return Ok(user);
    }

    [HttpGet]
    [Authorize(Roles = "TENANT_ADMIN,SUPER_ADMIN")]
    public async Task<IActionResult> GetTenantUsers()
    {
        var tenantId = GetTenantId();
        
        // If SUPER_ADMIN with no tenantId, they should see all users? 
        // Based on logic, SuperAdmin manages all.
        if (string.IsNullOrEmpty(tenantId) && User.IsInRole("SUPER_ADMIN"))
        {
            // SuperAdmin should probably see all tenants/users in the SuperAdmin screen.
            // If they are here, we could return all users, but let's just return empty for now
            // and avoid the logic failure.
            return Ok(new List<UserResponse>());
        }

        var users = await _userService.GetUsersByTenantAsync(tenantId);
        return Ok(users);
    }

    [HttpPost]
    [Authorize(Roles = "TENANT_ADMIN,SUPER_ADMIN")]
    public async Task<IActionResult> CreateUser([FromBody] CreateTenantUserRequest request)
    {
        var tenantId = GetTenantId();
        try
        {
            var user = await _userService.CreateUserAsync(tenantId, request);
            return Ok(user);
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }

    [HttpPut("{id}/status")]
    [Authorize(Roles = "TENANT_ADMIN,SUPER_ADMIN")]
    public async Task<IActionResult> UpdateUserStatus(string id, [FromBody] UpdateUserStatusRequest request)
    {
        var tenantId = GetTenantId();
        
        // Prevent users from deactivating themselves
        if (id == GetUserId() && !request.IsActive)
        {
            return BadRequest(new { message = "You cannot deactivate your own account." });
        }

        var success = await _userService.UpdateUserStatusAsync(tenantId, id, request.IsActive);
        if (!success) return NotFound(new { message = "User not found or access denied." });

        return NoContent();
    }
}
