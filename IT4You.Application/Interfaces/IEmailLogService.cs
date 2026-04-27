using IT4You.Application.DTOs;
using IT4You.Domain.Entities;

namespace IT4You.Application.Interfaces;

public interface IEmailLogService
{
    Task<PagedResponse<EmailLogResponse>> SearchAsync(int page, int pageSize, EmailLogStatus? status, string? templateKey, string? toEmail, string? tenantId, string? requestedByUserId, DateTime? from, DateTime? to, string? sortBy, string? sortDirection);
}

