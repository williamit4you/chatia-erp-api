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
}
