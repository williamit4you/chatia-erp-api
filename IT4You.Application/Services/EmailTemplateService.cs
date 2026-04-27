using IT4You.Application.Data;
using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace IT4You.Application.Services;

public class EmailTemplateService : IEmailTemplateService
{
    private readonly AppDbContext _context;

    public EmailTemplateService(AppDbContext context)
    {
        _context = context;
    }

    public async Task<IEnumerable<EmailTemplateResponse>> GetTemplatesAsync()
    {
        var templates = await _context.EmailTemplates
            .OrderBy(t => t.Key)
            .ToListAsync();

        return templates.Select(ToResponse);
    }

    public async Task<EmailTemplateResponse?> GetTemplateAsync(string id)
    {
        var template = await _context.EmailTemplates.FindAsync(id);
        return template == null ? null : ToResponse(template);
    }

    public async Task<EmailTemplateResponse> UpsertTemplateAsync(EmailTemplateRequest request, string? userId)
    {
        var template = await _context.EmailTemplates.FirstOrDefaultAsync(t => t.Key == request.Key);
        if (template == null)
        {
            template = new EmailTemplate
            {
                CreatedByUserId = userId
            };
            _context.EmailTemplates.Add(template);
        }

        Apply(template, request, userId);
        await _context.SaveChangesAsync();
        return ToResponse(template);
    }

    public async Task<EmailTemplateResponse> UpdateTemplateAsync(string id, EmailTemplateRequest request, string? userId)
    {
        var template = await _context.EmailTemplates.FindAsync(id);
        if (template == null) throw new Exception("Template nao encontrado");

        var keyExists = await _context.EmailTemplates.AnyAsync(t => t.Id != id && t.Key == request.Key);
        if (keyExists) throw new Exception("Ja existe um template com esta chave");

        Apply(template, request, userId);
        await _context.SaveChangesAsync();
        return ToResponse(template);
    }

    public Task<EmailTemplate?> GetActiveTemplateByKeyAsync(string key)
    {
        return _context.EmailTemplates.FirstOrDefaultAsync(t => t.Key == key && t.IsActive);
    }

    public string Render(string template, IDictionary<string, string?> variables)
    {
        var rendered = template;
        foreach (var variable in variables)
        {
            rendered = rendered.Replace("{{" + variable.Key + "}}", variable.Value ?? string.Empty);
        }
        return rendered;
    }

    private static void Apply(EmailTemplate template, EmailTemplateRequest request, string? userId)
    {
        template.Key = request.Key.Trim();
        template.Name = request.Name.Trim();
        template.Subject = request.Subject.Trim();
        template.HtmlBody = request.HtmlBody;
        template.TextBody = request.TextBody;
        template.AllowedVariables = request.AllowedVariables;
        template.IsActive = request.IsActive;
        template.UpdatedByUserId = userId;
        template.UpdatedAt = DateTime.UtcNow;
    }

    private static EmailTemplateResponse ToResponse(EmailTemplate template)
    {
        return new EmailTemplateResponse(
            template.Id,
            template.Key,
            template.Name,
            template.Subject,
            template.HtmlBody,
            template.TextBody,
            template.AllowedVariables,
            template.IsActive,
            template.CreatedAt,
            template.UpdatedAt);
    }
}

