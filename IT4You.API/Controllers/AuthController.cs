using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.API.Infrastructure.RateLimiting;
using Microsoft.AspNetCore.Authorization;
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
    private readonly IPasswordResetService _passwordResetService;
    private readonly IConfiguration _configuration;
    private readonly SimpleRateLimiter _rateLimiter;

    public AuthController(IAuthService authService, IPasswordResetService passwordResetService, IConfiguration configuration, SimpleRateLimiter rateLimiter)
    {
        _authService = authService;
        _passwordResetService = passwordResetService;
        _configuration = configuration;
        _rateLimiter = rateLimiter;
    }

    private string GetClientIp() => HttpContext.Connection.RemoteIpAddress?.ToString() ?? "unknown";

    private IActionResult TooManyRequestsResponse(int retryAfterSeconds)
    {
        Response.Headers["Retry-After"] = retryAfterSeconds.ToString();
        return StatusCode(429, new
        {
            message = $"Muitas tentativas. Tente novamente em {retryAfterSeconds} segundos."
        });
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

    [HttpPost("superadmin/login")]
    public async Task<IActionResult> SuperAdminLogin([FromBody] LoginRequest request)
    {
        try
        {
            var response = await _authService.LoginAsync(request);
            if (response == null)
                return Unauthorized(new { message = "E-mail ou senha inválidos." });

            if (!string.Equals(response.Role, "SUPER_ADMIN", StringComparison.OrdinalIgnoreCase))
                return Unauthorized(new { message = "E-mail ou senha inválidos." });

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

    [AllowAnonymous]
    [HttpPost("forgot-password")]
    public async Task<IActionResult> ForgotPassword([FromBody] ForgotPasswordRequest request)
    {
        var ip = GetClientIp();
        var email = (request.Email ?? string.Empty).Trim().ToLowerInvariant();
        if (!_rateLimiter.TryConsume($"forgot-password:{ip}:{email}", limit: 5, window: TimeSpan.FromMinutes(15), out var retryAfterSeconds))
            return TooManyRequestsResponse(retryAfterSeconds);

        if (string.IsNullOrWhiteSpace(email))
        {
            return Ok(new
            {
                success = true,
                message = "Se o e-mail estiver cadastrado, enviaremos instruÃ§Ãµes para redefinir sua senha."
            });
        }

        var resetBaseUrl = _configuration["Frontend:BaseUrl"]
            ?? Request.Headers.Origin.FirstOrDefault()
            ?? "http://localhost:3010";

        await _passwordResetService.RequestResetAsync(
            email,
            resetBaseUrl,
            HttpContext.Connection.RemoteIpAddress?.ToString(),
            Request.Headers.UserAgent.ToString());

        return Ok(new
        {
            success = true,
            message = "Se o e-mail estiver cadastrado, enviaremos instruções para redefinir sua senha."
        });
    }

    [AllowAnonymous]
    [HttpGet("reset-password/validate")]
    public async Task<IActionResult> ValidateResetToken([FromQuery] string token)
    {
        var ip = GetClientIp();
        if (!_rateLimiter.TryConsume($"reset-validate:{ip}", limit: 30, window: TimeSpan.FromMinutes(5), out var retryAfterSeconds))
            return TooManyRequestsResponse(retryAfterSeconds);

        return Ok(await _passwordResetService.ValidateAsync(token));
    }

    [AllowAnonymous]
    [HttpPost("reset-password")]
    public async Task<IActionResult> ResetPassword([FromBody] ResetPasswordRequest request)
    {
        var ip = GetClientIp();
        if (!_rateLimiter.TryConsume($"reset-password:{ip}", limit: 10, window: TimeSpan.FromMinutes(15), out var retryAfterSeconds))
            return TooManyRequestsResponse(retryAfterSeconds);

        try
        {
            await _passwordResetService.ResetPasswordAsync(request);
            return Ok(new { success = true });
        }
        catch (Exception ex)
        {
            return BadRequest(new { success = false, message = ex.Message });
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
