using IT4You.Domain.Entities;

namespace IT4You.Application.DTOs;

public record EmailConfigurationRequest(
    string Name,
    string SenderName,
    string SenderEmail,
    string SmtpHost,
    int SmtpPort,
    string? SmtpUser,
    string? SmtpPassword,
    bool SmtpUseSsl,
    bool SmtpUseStartTls,
    EmailReceiveProtocol ReceiveProtocol,
    string? ReceiveHost,
    int? ReceivePort,
    string? ReceiveUser,
    string? ReceivePassword,
    bool ReceiveUseSsl,
    int TimeoutSeconds,
    bool IsActive = true);

public record EmailConfigurationResponse(
    string Id,
    string Name,
    string SenderName,
    string SenderEmail,
    string SmtpHost,
    int SmtpPort,
    string? SmtpUser,
    bool HasSmtpPassword,
    bool SmtpUseSsl,
    bool SmtpUseStartTls,
    EmailReceiveProtocol ReceiveProtocol,
    string? ReceiveHost,
    int? ReceivePort,
    string? ReceiveUser,
    bool HasReceivePassword,
    bool ReceiveUseSsl,
    int TimeoutSeconds,
    bool IsActive,
    DateTime CreatedAt,
    DateTime UpdatedAt);

public record TestEmailRequest(string ToEmail);

public record EmailTemplateRequest(
    string Key,
    string Name,
    string Subject,
    string HtmlBody,
    string? TextBody,
    string? AllowedVariables,
    bool IsActive = true);

public record EmailTemplateResponse(
    string Id,
    string Key,
    string Name,
    string Subject,
    string HtmlBody,
    string? TextBody,
    string? AllowedVariables,
    bool IsActive,
    DateTime CreatedAt,
    DateTime UpdatedAt);

public record EmailLogResponse(
    string Id,
    string? TemplateKey,
    string ToEmail,
    string? ToName,
    string Subject,
    string? RequestedByUserName,
    string? TargetUserName,
    string? TenantName,
    EmailLogStatus Status,
    string? ErrorMessage,
    DateTime? SentAt,
    DateTime CreatedAt);

public record PagedResponse<T>(IEnumerable<T> Items, int Page, int PageSize, int Total);

public record ForgotPasswordRequest(string Email);

public record ResetPasswordRequest(string Token, string NewPassword, string ConfirmPassword);

public record ValidateResetTokenResponse(bool Valid, DateTime? ExpiresAt, string? Reason);

