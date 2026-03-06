using IT4You.Application.DTOs;

namespace IT4You.Application.Interfaces;

public interface IUserService
{
    Task<List<UserResponse>> GetUsersByTenantAsync(string tenantId);
    Task<UserResponse> CreateUserAsync(string tenantId, CreateTenantUserRequest request);
    Task<bool> UpdateUserStatusAsync(string tenantId, string userId, bool isActive);
    Task<UserResponse?> GetUserByIdAsync(string userId);
}
