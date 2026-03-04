using IT4You.Application.DTOs;

namespace IT4You.Application.Interfaces;

public interface ITenantService
{
    Task UpdateSettingsAsync(string tenantId, UpdateSettingsRequest request);
    Task<IEnumerable<TenantDto>> GetAllTenantsAsync();
    Task CreateUserAsync(string tenantId, CreateUserRequest request);
}
