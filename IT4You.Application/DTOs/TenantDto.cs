namespace IT4You.Application.DTOs;

public record UpdateSettingsRequest(string? IaToken, string? ErpToken, string? ChatAiToken, string? DbIp, string? DbName, string? DbType, string? DbUser, string? DbPassword, bool? ShowChartDetails = null);

public record CreateUserRequest(string Email, string Password, string? Name, string? Role, 
    bool HasPayableChatAccess = false, bool HasPayableDashboardAccess = false,
    bool HasReceivableChatAccess = false, bool HasReceivableDashboardAccess = false,
    bool HasBankingChatAccess = false, bool HasBankingDashboardAccess = false,
    bool HasBudgetChatAccess = false, bool HasBudgetDashboardAccess = false);
public record UpdateUserRequest(string? Email = null, string? Name = null, string? Password = null, string? Role = null, bool? IsActive = null, 
    bool? HasPayableChatAccess = null, bool? HasPayableDashboardAccess = null,
    bool? HasReceivableChatAccess = null, bool? HasReceivableDashboardAccess = null,
    bool? HasBankingChatAccess = null, bool? HasBankingDashboardAccess = null,
    bool? HasBudgetChatAccess = null, bool? HasBudgetDashboardAccess = null,
    bool? IsInactive = null, DateTime? BlockedUntil = null, bool? Unblock = null);

public record UserDto(string Id, string? Name, string? Email, string Role, int QueryCount, DateTime CreatedAt, bool IsActive, 
    bool HasPayableChatAccess, bool HasPayableDashboardAccess,
    bool HasReceivableChatAccess, bool HasReceivableDashboardAccess,
    bool HasBankingChatAccess, bool HasBankingDashboardAccess,
    bool HasBudgetChatAccess, bool HasBudgetDashboardAccess,
    bool IsInactive, DateTime? BlockedUntil);
public record TenantDto(string Id, string Name, string Cnpj, string? IaToken, string? ErpToken, string? ChatAiToken, 
    string? DbIp, string? DbName, string? DbType, string? DbUser, string? DbPassword, bool ShowChartDetails,
    DateTime CreatedAt, IEnumerable<UserDto> Users);
