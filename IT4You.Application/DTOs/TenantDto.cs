namespace IT4You.Application.DTOs;

public record UpdateSettingsRequest(string? IaToken, string? ErpToken);

public record CreateUserRequest(string Email, string Password, string? Name, string? Role);

public record TenantDto(string Id, string Name, string Cnpj, string? IaToken, string? ErpToken);
