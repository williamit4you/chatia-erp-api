using IT4You.Application.DTOs;
using IT4You.Domain.Entities;

namespace IT4You.Application.Interfaces;

public interface IEmailTemplateService
{
    Task<IEnumerable<EmailTemplateResponse>> GetTemplatesAsync();
    Task<EmailTemplateResponse?> GetTemplateAsync(string id);
    Task<EmailTemplateResponse> UpsertTemplateAsync(EmailTemplateRequest request, string? userId);
    Task<EmailTemplateResponse> UpdateTemplateAsync(string id, EmailTemplateRequest request, string? userId);
    Task<EmailTemplate?> GetActiveTemplateByKeyAsync(string key);
    string Render(string template, IDictionary<string, string?> variables);
}

