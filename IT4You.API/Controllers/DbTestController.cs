using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Data.SqlClient;
using Dapper;

namespace IT4You.API.Controllers;

[AllowAnonymous]
[ApiController]
[Route("api/[controller]")]
public class DbTestController : ControllerBase
{
    private readonly IConfiguration _configuration;

    public DbTestController(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    [HttpGet("check")]
    public async Task<IActionResult> CheckDb()
    {
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            
            if (string.IsNullOrEmpty(connectionString))
            {
                return BadRequest(new { status = "error", message = "Connection string 'DefaultConnection' not found." });
            }

            // Garante que TrustServerCertificate está ativo e desabilita Integrated Security/Kerberos
            // se o ambiente for Linux (comum em Docker/Produção)
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                TrustServerCertificate = true,
                IntegratedSecurity = false // Força SQL Authentication (Usuario/Senha)
            };

            using var connection = new SqlConnection(builder.ConnectionString);
            
            var result = await connection.QueryFirstOrDefaultAsync<string>("SELECT TOP 1 descricao FROM testeia");
            
            return Ok(new 
            { 
                status = "success", 
                message = "SQL Server connection verified with SQL Authentication", 
                data = result ?? "No record found" 
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new 
            { 
                status = "error", 
                message = ex.Message, 
                details = ex.InnerException?.Message 
            });
        }
    }
}
