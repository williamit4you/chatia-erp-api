using IT4You.Application.DTOs;

namespace IT4You.Application.Interfaces;

public interface IEmailSenderService
{
    Task<IEnumerable<EmailConfigurationResponse>> GetConfigurationsAsync();
    Task<EmailConfigurationResponse> SaveConfigurationAsync(EmailConfigurationRequest request, string? userId, string? id = null);
    Task<string> SendTemplateEmailAsync(string templateKey, string toEmail, string? toName, IDictionary<string, string?> variables, string? requestedByUserId = null, string? targetUserId = null, string? tenantId = null);
    Task<string> SendTestEmailAsync(string configurationId, string toEmail, string? requestedByUserId);
}

