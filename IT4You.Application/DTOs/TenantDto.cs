namespace IT4You.Application.DTOs;

public record UpdateSettingsRequest(string? IaToken, string? ErpToken);

public record CreateUserRequest(string Email, string Password, string? Name, string? Role);

public record UserDto(string Id, string? Name, string? Email, string Role, int QueryCount, DateTime CreatedAt);
public record TenantDto(string Id, string Name, string Cnpj, string? IaToken, string? ErpToken, DateTime CreatedAt, IEnumerable<UserDto> Users);
