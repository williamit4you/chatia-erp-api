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
            // Pegando a mesma connection string que o ErpPlugin usa
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            
            if (string.IsNullOrEmpty(connectionString))
            {
                return BadRequest(new { status = "error", message = "Connection string 'DefaultConnection' not found." });
            }

            using var connection = new SqlConnection(connectionString);
            
            // Usando a sintaxe SQL Server (TOP 1) conforme solicitado
            var result = await connection.QueryFirstOrDefaultAsync<string>("SELECT TOP 1 descricao FROM testeia");
            
            return Ok(new 
            { 
                status = "success", 
                message = "SQL Server connection verified (using DefaultConnection)", 
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
