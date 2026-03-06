using IT4You.Domain.Entities;

namespace IT4You.Application.DTOs;

public record UserResponse(string Id, string Name, string Email, UserRole Role, int QueryCount, bool IsActive, DateTime CreatedAt);

public record CreateTenantUserRequest(string Name, string Email, string Password, UserRole Role = UserRole.TENANT_USER);

public record UpdateUserStatusRequest(bool IsActive);
