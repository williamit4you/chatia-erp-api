using IT4You.Application.DTOs;

namespace IT4You.Application.Interfaces;

public interface IPasswordResetService
{
    Task RequestResetAsync(string email, string resetBaseUrl, string? ip, string? userAgent);
    Task<ValidateResetTokenResponse> ValidateAsync(string token);
    Task ResetPasswordAsync(ResetPasswordRequest request);
}

