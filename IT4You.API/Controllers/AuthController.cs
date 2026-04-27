using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace IT4You.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AuthController : ControllerBase
{
    private readonly IAuthService _authService;
    private readonly IConfiguration _configuration;

    public AuthController(IAuthService authService, IConfiguration configuration)
    {
        _authService = authService;
        _configuration = configuration;
    }

    [HttpPost("login")]
    public async Task<IActionResult> Login([FromBody] LoginRequest request)
    {
        try
        {
            var response = await _authService.LoginAsync(request);
            if (response == null)
                return Unauthorized(new { message = "E-mail ou senha inválidos." });

            // Generate JWT
            var token = GenerateJwtToken(response);
            
            return Ok(response with { Token = token });
        }
        catch (UnauthorizedAccessException ex)
        {
            return StatusCode(403, new { error = ex.Message, message = ex.Message });
        }
    }

    [HttpPost("/api/register")]
    public async Task<IActionResult> Register([FromBody] RegisterRequest request)
    {
        try
        {
            var response = await _authService.RegisterAsync(request);
            return CreatedAtAction(nameof(Register), response);
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }

    private string GenerateJwtToken(LoginResponse user)
    {
        var jwtSettings = _configuration.GetSection("Jwt");
        var key = Encoding.ASCII.GetBytes(jwtSettings["Key"] ?? "a_very_long_secret_key_that_should_be_in_settings");

        var tokenDescriptor = new SecurityTokenDescriptor
        {
            Subject = new ClaimsIdentity(new[]
            {
                new Claim(ClaimTypes.NameIdentifier, user.Email),
                new Claim("userId", user.Id),
                new Claim("id", user.Email),
                new Claim("tenantId", user.TenantId),
                new Claim("tenantName", user.TenantName ?? string.Empty),
                new Claim("sessionId", user.CurrentSessionId),
                new Claim(ClaimTypes.Role, user.Role),
                new Claim("hasPayableChatAccess", user.HasPayableChatAccess.ToString().ToLower()),
                new Claim("hasPayableDashboardAccess", user.HasPayableDashboardAccess.ToString().ToLower()),
                new Claim("hasReceivableChatAccess", user.HasReceivableChatAccess.ToString().ToLower()),
                new Claim("hasReceivableDashboardAccess", user.HasReceivableDashboardAccess.ToString().ToLower()),
                new Claim("hasBankingChatAccess", user.HasBankingChatAccess.ToString().ToLower()),
                new Claim("hasBankingDashboardAccess", user.HasBankingDashboardAccess.ToString().ToLower())
            }),
            Expires = DateTime.UtcNow.AddDays(7),
            SigningCredentials = new SigningCredentials(new SymmetricSecurityKey(key), SecurityAlgorithms.HmacSha256Signature),
            Issuer = jwtSettings["Issuer"],
            Audience = jwtSettings["Audience"]
        };

        var tokenHandler = new JwtSecurityTokenHandler();
        var token = tokenHandler.CreateToken(tokenDescriptor);
        return tokenHandler.WriteToken(token);
    }
}
