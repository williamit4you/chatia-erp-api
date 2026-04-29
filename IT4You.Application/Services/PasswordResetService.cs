using System.Security.Cryptography;
using System.Text;
using IT4You.Application.Data;
using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace IT4You.Application.Services;

public class PasswordResetService : IPasswordResetService
{
    private const int ResetTokenLifetimeMinutes = 30;
    private readonly AppDbContext _context;
    private readonly IEmailSenderService _emailSender;
    private readonly ILogger<PasswordResetService> _logger;

    public PasswordResetService(AppDbContext context, IEmailSenderService emailSender, ILogger<PasswordResetService> logger)
    {
        _context = context;
        _emailSender = emailSender;
        _logger = logger;
    }

    public async Task RequestResetAsync(string email, string resetBaseUrl, string? ip, string? userAgent)
    {
        var normalizedEmail = email.Trim().ToLowerInvariant();
        var user = await _context.Users
            .Include(u => u.Tenant)
            .FirstOrDefaultAsync(u => u.Email != null && u.Email.ToLower() == normalizedEmail && u.IsActive && !u.IsInactive);

        if (user == null) return;

        var previous = await _context.PasswordResetTokens
            .Where(t => t.UserId == user.Id && t.UsedAt == null)
            .ToListAsync();
        foreach (var token in previous) token.UsedAt = DateTime.UtcNow;

        var rawToken = Convert.ToBase64String(RandomNumberGenerator.GetBytes(48))
            .Replace("+", "-")
            .Replace("/", "_")
            .TrimEnd('=');

        var resetToken = new PasswordResetToken
        {
            UserId = user.Id,
            TenantId = user.TenantId,
            TokenHash = HashToken(rawToken),
            ExpiresAt = DateTime.UtcNow.AddMinutes(ResetTokenLifetimeMinutes),
            RequestedByIp = ip,
            RequestedByUserAgent = userAgent
        };
        _context.PasswordResetTokens.Add(resetToken);
        await _context.SaveChangesAsync();

        var resetUrl = $"{resetBaseUrl.TrimEnd('/')}/reset-password?token={Uri.EscapeDataString(rawToken)}";
        var variables = new Dictionary<string, string?>
        {
            ["userName"] = user.Name ?? user.Email,
            ["userEmail"] = user.Email,
            ["tenantName"] = user.Tenant?.Name,
            ["resetUrl"] = resetUrl,
            ["expiresAt"] = resetToken.ExpiresAt.ToLocalTime().ToString("dd/MM/yyyy HH:mm"),
            ["requestedAt"] = DateTime.UtcNow.ToLocalTime().ToString("dd/MM/yyyy HH:mm"),
            ["applicationName"] = "IT4You AI ERP"
        };

        await _emailSender.SendTemplateEmailAsync("password_reset", user.Email!, user.Name, variables, null, user.Id, user.TenantId);
    }

    public async Task<ValidateResetTokenResponse> ValidateAsync(string token)
    {
        token = NormalizeToken(token);
        if (!IsTokenFormatValid(token))
        {
            _logger.LogWarning("Password reset token rejected due to invalid format during validation.");
            return new ValidateResetTokenResponse(false, null, "TOKEN_INVALID");
        }

        var resetToken = await FindTokenAsync(token);
        if (resetToken == null) return new ValidateResetTokenResponse(false, null, "TOKEN_INVALID");
        if (resetToken.UsedAt != null) return new ValidateResetTokenResponse(false, resetToken.ExpiresAt, "TOKEN_USED");
        if (resetToken.ExpiresAt < DateTime.UtcNow) return new ValidateResetTokenResponse(false, resetToken.ExpiresAt, "TOKEN_EXPIRED");

        return new ValidateResetTokenResponse(true, resetToken.ExpiresAt, null);
    }

    public async Task ResetPasswordAsync(ResetPasswordRequest request)
    {
        if (request.NewPassword != request.ConfirmPassword) throw new Exception("As senhas nao conferem");
        if (request.NewPassword.Length < 8) throw new Exception("A senha deve ter pelo menos 8 caracteres");

        var normalizedToken = NormalizeToken(request.Token);
        if (!IsTokenFormatValid(normalizedToken))
        {
            _logger.LogWarning("Password reset rejected due to invalid token format.");
            throw new Exception("Token invalido");
        }

        var validation = await ValidateAsync(normalizedToken);
        if (!validation.Valid)
        {
            _logger.LogWarning("Password reset rejected. Reason: {Reason}", validation.Reason);
            throw new Exception(validation.Reason switch
            {
                "TOKEN_USED" => "Token ja utilizado",
                "TOKEN_EXPIRED" => "Token expirado",
                _ => "Token invalido"
            });
        }

        var resetToken = await FindTokenAsync(normalizedToken);
        if (resetToken == null || resetToken.User == null)
        {
            _logger.LogWarning("Password reset rejected because token was not found after validation.");
            throw new Exception("Token invalido");
        }

        resetToken.User.Password = BCrypt.Net.BCrypt.HashPassword(request.NewPassword);
        resetToken.User.UpdatedAt = DateTime.UtcNow;
        resetToken.UsedAt = DateTime.UtcNow;

        var otherActiveTokens = await _context.PasswordResetTokens
            .Where(t => t.UserId == resetToken.UserId && t.Id != resetToken.Id && t.UsedAt == null)
            .ToListAsync();

        foreach (var otherToken in otherActiveTokens)
        {
            otherToken.UsedAt = DateTime.UtcNow;
        }

        await _context.SaveChangesAsync();

        var variables = new Dictionary<string, string?>
        {
            ["userName"] = resetToken.User.Name ?? resetToken.User.Email,
            ["userEmail"] = resetToken.User.Email,
            ["tenantName"] = resetToken.User.Tenant?.Name,
            ["requestedAt"] = DateTime.UtcNow.ToLocalTime().ToString("dd/MM/yyyy HH:mm"),
            ["applicationName"] = "IT4You AI ERP"
        };

        await _emailSender.SendTemplateEmailAsync("password_changed", resetToken.User.Email!, resetToken.User.Name, variables, null, resetToken.UserId, resetToken.User.TenantId);
    }

    private async Task<PasswordResetToken?> FindTokenAsync(string token)
    {
        if (!IsTokenFormatValid(token))
            return null;

        var tokenHash = HashToken(token);
        return await _context.PasswordResetTokens
            .Include(t => t.User)
            .ThenInclude(u => u!.Tenant)
            .FirstOrDefaultAsync(t => t.TokenHash == tokenHash);
    }

    private static string NormalizeToken(string token)
    {
        return (token ?? string.Empty).Trim();
    }

    private static bool IsTokenFormatValid(string token)
    {
        if (string.IsNullOrWhiteSpace(token) || token.Length != 64)
            return false;

        return token.All(ch => char.IsLetterOrDigit(ch) || ch is '-' or '_');
    }

    private static string HashToken(string token)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(token));
        return Convert.ToHexString(bytes);
    }
}

