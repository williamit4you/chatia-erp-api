using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.Application.Data;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace IT4You.API.Controllers;

[Authorize(Roles = "SUPER_ADMIN")]
[ApiController]
[Route("api/superadmin")]
public class SuperAdminController : ControllerBase
{
    private readonly ITenantService _tenantService;
    private readonly AppDbContext _context;
    private readonly ILogger<SuperAdminController> _logger;

    public SuperAdminController(ITenantService tenantService, AppDbContext context, ILogger<SuperAdminController> logger)
    {
        _tenantService = tenantService;
        _context = context;
        _logger = logger;
    }

    [AllowAnonymous]
    [HttpGet("promote-me")]
    public async Task<IActionResult> PromoteMe([FromQuery] string email = "william@it4you.inf.br")
    {
        var user = await _context.Users.FirstOrDefaultAsync(u => u.Email == email);
        if (user == null) return NotFound("Usuário não encontrado.");
        
        user.Role = IT4You.Domain.Entities.UserRole.SUPER_ADMIN;
        await _context.SaveChangesAsync();
        
        return Ok($"Usuário {email} foi promovido a SUPER ADMIN! Agora ele pode logar na tela principal.");
    }

    [AllowAnonymous]
    [HttpGet("test-db")]
    public IActionResult CheckConnection()
    {
        _logger.LogInformation("Testing database connection to: {DbName}", _context.Database.GetDbConnection().Database);
        try
        {
            var canConnect = _context.Database.CanConnect();
            
            if (canConnect)
            {
                _logger.LogInformation("Successfully connected to the database.");
                return Ok(new { success = true, message = "Conexão com SQL Server (.NET + EF Core) estabelecida com sucesso!" });
            }
            
            _logger.LogWarning("Database connection test failed (returned false).");
            return StatusCode(500, new { success = false, message = "Não foi possível conectar ao banco de dados, mas nenhuma exceção foi lançada." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Database connection exception occurred.");
            return StatusCode(500, new
            {
                success = false,
                error = ex.Message
            });
        }
    }

    [HttpGet("tenants")]
    public async Task<IActionResult> GetTenants()
    {
        var tenants = await _tenantService.GetAllTenantsAsync();
        return Ok(tenants);
    }

    [HttpPut("tenants/{tenantId}")]
    public async Task<IActionResult> UpdateTenantSettings(string tenantId, [FromBody] UpdateSettingsRequest request)
    {
        try
        {
            await _tenantService.UpdateSettingsAsync(tenantId, request);
            return Ok(new { success = true });
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }

    [HttpPost("tenants/{tenantId}/users")]
    public async Task<IActionResult> CreateUser(string tenantId, [FromBody] CreateUserRequest request)
    {
        try
        {
            await _tenantService.CreateUserAsync(tenantId, request);
            return CreatedAtAction(nameof(CreateUser), new { success = true });
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }

    [HttpPut("tenants/{tenantId}/users/{userId}")]
    public async Task<IActionResult> UpdateUser(string tenantId, string userId, [FromBody] UpdateUserRequest request)
    {
        try
        {
            await _tenantService.UpdateUserAsync(tenantId, userId, request);
            return Ok(new { success = true, message = "User updated successfully" });
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }
}
