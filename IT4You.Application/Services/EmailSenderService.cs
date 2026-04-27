using System.Net;
using System.Net.Mail;
using IT4You.Application.Data;
using IT4You.Application.DTOs;
using IT4You.Application.Helpers;
using IT4You.Application.Interfaces;
using IT4You.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace IT4You.Application.Services;

public class EmailSenderService : IEmailSenderService
{
    private readonly AppDbContext _context;
    private readonly IEmailTemplateService _templateService;

    public EmailSenderService(AppDbContext context, IEmailTemplateService templateService)
    {
        _context = context;
        _templateService = templateService;
    }

    public async Task<IEnumerable<EmailConfigurationResponse>> GetConfigurationsAsync()
    {
        var configs = await _context.EmailConfigurations
            .OrderByDescending(c => c.IsActive)
            .ThenBy(c => c.Name)
            .ToListAsync();

        return configs.Select(ToResponse);
    }

    public async Task<EmailConfigurationResponse> SaveConfigurationAsync(EmailConfigurationRequest request, string? userId, string? id = null)
    {
        var config = string.IsNullOrWhiteSpace(id)
            ? null
            : await _context.EmailConfigurations.FindAsync(id);

        if (config == null)
        {
            config = new EmailConfiguration { CreatedByUserId = userId };
            _context.EmailConfigurations.Add(config);
        }

        config.Name = request.Name.Trim();
        config.SenderName = request.SenderName.Trim();
        config.SenderEmail = request.SenderEmail.Trim();
        config.SmtpHost = request.SmtpHost.Trim();
        config.SmtpPort = request.SmtpPort;
        config.SmtpUser = request.SmtpUser;
        if (!string.IsNullOrWhiteSpace(request.SmtpPassword)) config.SmtpPasswordEncrypted = EncryptionHelper.Encrypt(request.SmtpPassword);
        config.SmtpUseSsl = request.SmtpUseSsl;
        config.SmtpUseStartTls = request.SmtpUseStartTls;
        config.ReceiveProtocol = request.ReceiveProtocol;
        config.ReceiveHost = request.ReceiveHost;
        config.ReceivePort = request.ReceivePort;
        config.ReceiveUser = request.ReceiveUser;
        if (!string.IsNullOrWhiteSpace(request.ReceivePassword)) config.ReceivePasswordEncrypted = EncryptionHelper.Encrypt(request.ReceivePassword);
        config.ReceiveUseSsl = request.ReceiveUseSsl;
        config.TimeoutSeconds = request.TimeoutSeconds <= 0 ? 30 : request.TimeoutSeconds;
        config.IsActive = request.IsActive;
        config.UpdatedByUserId = userId;
        config.UpdatedAt = DateTime.UtcNow;

        if (config.IsActive)
        {
            var others = await _context.EmailConfigurations.Where(c => c.Id != config.Id && c.IsActive).ToListAsync();
            foreach (var other in others) other.IsActive = false;
        }

        await _context.SaveChangesAsync();
        return ToResponse(config);
    }

    public async Task<string> SendTemplateEmailAsync(string templateKey, string toEmail, string? toName, IDictionary<string, string?> variables, string? requestedByUserId = null, string? targetUserId = null, string? tenantId = null)
    {
        var template = await _templateService.GetActiveTemplateByKeyAsync(templateKey)
            ?? throw new Exception($"Template ativo nao encontrado: {templateKey}");
        var subject = _templateService.Render(template.Subject, variables);
        var html = _templateService.Render(template.HtmlBody, variables);
        var text = template.TextBody == null ? null : _templateService.Render(template.TextBody, variables);

        return await SendAsync(templateKey, toEmail, toName, subject, html, text, requestedByUserId, targetUserId, tenantId);
    }

    public async Task<string> SendTestEmailAsync(string configurationId, string toEmail, string? requestedByUserId)
    {
        var variables = new Dictionary<string, string?>
        {
            ["applicationName"] = "IT4You AI ERP",
            ["requestedAt"] = DateTime.UtcNow.ToString("dd/MM/yyyy HH:mm")
        };

        var template = await _templateService.GetActiveTemplateByKeyAsync("email_test");
        if (template != null)
        {
            return await SendTemplateEmailAsync("email_test", toEmail, null, variables, requestedByUserId);
        }

        return await SendAsync("email_test", toEmail, null, "Teste de envio - IT4You AI ERP", "<p>Teste de envio realizado com sucesso.</p>", "Teste de envio realizado com sucesso.", requestedByUserId, null, null, configurationId);
    }

    private async Task<string> SendAsync(string? templateKey, string toEmail, string? toName, string subject, string html, string? text, string? requestedByUserId, string? targetUserId, string? tenantId, string? configurationId = null)
    {
        var config = !string.IsNullOrWhiteSpace(configurationId)
            ? await _context.EmailConfigurations.FirstOrDefaultAsync(c => c.Id == configurationId)
            : await _context.EmailConfigurations.FirstOrDefaultAsync(c => c.IsActive);

        if (config == null) throw new Exception("Configuracao de e-mail ativa nao encontrada");

        var log = new EmailLog
        {
            TemplateKey = templateKey,
            EmailConfigurationId = config.Id,
            ToEmail = toEmail,
            ToName = toName,
            Subject = subject,
            RequestedByUserId = requestedByUserId,
            TargetUserId = targetUserId,
            TenantId = tenantId,
            Status = EmailLogStatus.PENDING
        };
        _context.EmailLogs.Add(log);
        await _context.SaveChangesAsync();

        try
        {
            using var client = new SmtpClient(config.SmtpHost, config.SmtpPort)
            {
                EnableSsl = config.SmtpUseSsl || config.SmtpUseStartTls,
                Timeout = config.TimeoutSeconds * 1000
            };

            if (!string.IsNullOrWhiteSpace(config.SmtpUser))
            {
                var password = EncryptionHelper.Decrypt(config.SmtpPasswordEncrypted ?? string.Empty);
                client.Credentials = new NetworkCredential(config.SmtpUser, password);
            }

            using var message = new MailMessage
            {
                From = new MailAddress(config.SenderEmail, config.SenderName),
                Subject = subject,
                Body = string.IsNullOrWhiteSpace(html) ? (text ?? string.Empty) : html,
                IsBodyHtml = !string.IsNullOrWhiteSpace(html)
            };
            message.To.Add(new MailAddress(toEmail, toName ?? toEmail));
            await client.SendMailAsync(message);

            log.Status = EmailLogStatus.SUCCESS;
            log.SentAt = DateTime.UtcNow;
            await _context.SaveChangesAsync();
            return log.Id;
        }
        catch (Exception ex)
        {
            log.Status = EmailLogStatus.ERROR;
            log.ErrorMessage = ex.Message.Length > 500 ? ex.Message[..500] : ex.Message;
            await _context.SaveChangesAsync();
            throw;
        }
    }

    private static EmailConfigurationResponse ToResponse(EmailConfiguration config)
    {
        return new EmailConfigurationResponse(
            config.Id,
            config.Name,
            config.SenderName,
            config.SenderEmail,
            config.SmtpHost,
            config.SmtpPort,
            config.SmtpUser,
            !string.IsNullOrWhiteSpace(config.SmtpPasswordEncrypted),
            config.SmtpUseSsl,
            config.SmtpUseStartTls,
            config.ReceiveProtocol,
            config.ReceiveHost,
            config.ReceivePort,
            config.ReceiveUser,
            !string.IsNullOrWhiteSpace(config.ReceivePasswordEncrypted),
            config.ReceiveUseSsl,
            config.TimeoutSeconds,
            config.IsActive,
            config.CreatedAt,
            config.UpdatedAt);
    }
}

