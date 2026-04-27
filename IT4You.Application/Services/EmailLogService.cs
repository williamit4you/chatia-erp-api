using IT4You.Application.Data;
using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace IT4You.Application.Services;

public class EmailLogService : IEmailLogService
{
    private readonly AppDbContext _context;

    public EmailLogService(AppDbContext context)
    {
        _context = context;
    }

    public async Task<PagedResponse<EmailLogResponse>> SearchAsync(int page, int pageSize, EmailLogStatus? status, string? templateKey, string? toEmail, string? tenantId, string? requestedByUserId, DateTime? from, DateTime? to, string? sortBy, string? sortDirection)
    {
        page = Math.Max(1, page);
        pageSize = Math.Clamp(pageSize, 1, 100);

        var query = _context.EmailLogs.AsQueryable();

        if (status.HasValue) query = query.Where(l => l.Status == status.Value);
        if (!string.IsNullOrWhiteSpace(templateKey)) query = query.Where(l => l.TemplateKey == templateKey);
        if (!string.IsNullOrWhiteSpace(toEmail)) query = query.Where(l => l.ToEmail.ToLower().Contains(toEmail.ToLower()));
        if (!string.IsNullOrWhiteSpace(tenantId)) query = query.Where(l => l.TenantId == tenantId);
        if (!string.IsNullOrWhiteSpace(requestedByUserId)) query = query.Where(l => l.RequestedByUserId == requestedByUserId);
        if (from.HasValue) query = query.Where(l => l.CreatedAt >= from.Value);
        if (to.HasValue) query = query.Where(l => l.CreatedAt <= to.Value);

        var descending = string.Equals(sortDirection, "desc", StringComparison.OrdinalIgnoreCase);
        query = (sortBy?.ToLowerInvariant()) switch
        {
            "status" => descending ? query.OrderByDescending(l => l.Status) : query.OrderBy(l => l.Status),
            "toemail" => descending ? query.OrderByDescending(l => l.ToEmail) : query.OrderBy(l => l.ToEmail),
            "templatekey" => descending ? query.OrderByDescending(l => l.TemplateKey) : query.OrderBy(l => l.TemplateKey),
            _ => descending ? query.OrderByDescending(l => l.CreatedAt) : query.OrderBy(l => l.CreatedAt)
        };

        var total = await query.CountAsync();
        var logs = await query.Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
        var userIds = logs.SelectMany(l => new[] { l.RequestedByUserId, l.TargetUserId }).Where(id => id != null).Distinct().ToList();
        var tenantIds = logs.Select(l => l.TenantId).Where(id => id != null).Distinct().ToList();

        var users = await _context.Users.Where(u => userIds.Contains(u.Id)).ToDictionaryAsync(u => u.Id, u => u.Name ?? u.Email ?? u.Id);
        var tenants = await _context.Tenants.Where(t => tenantIds.Contains(t.Id)).ToDictionaryAsync(t => t.Id, t => t.Name);

        var items = logs.Select(l => new EmailLogResponse(
            l.Id,
            l.TemplateKey,
            l.ToEmail,
            l.ToName,
            l.Subject,
            l.RequestedByUserId != null && users.TryGetValue(l.RequestedByUserId, out var requestedBy) ? requestedBy : null,
            l.TargetUserId != null && users.TryGetValue(l.TargetUserId, out var targetUser) ? targetUser : null,
            l.TenantId != null && tenants.TryGetValue(l.TenantId, out var tenant) ? tenant : null,
            l.Status,
            l.ErrorMessage,
            l.SentAt,
            l.CreatedAt));

        return new PagedResponse<EmailLogResponse>(items, page, pageSize, total);
    }
}

