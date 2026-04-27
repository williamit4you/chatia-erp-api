namespace IT4You.Application.DTOs;

public record LoginRequest(string Email, string Password);

public record LoginResponse(string Id, string Token, string Name, string Email, string TenantId, string TenantName, string Role, 
    bool HasPayableChatAccess, bool HasPayableDashboardAccess,
    bool HasReceivableChatAccess, bool HasReceivableDashboardAccess,
    bool HasBankingChatAccess, bool HasBankingDashboardAccess,
    string CurrentSessionId = "");

public record RegisterRequest(string Cnpj, string CompanyName, string Email, string Password, string Name);

public record RegisterResponse(string Message, string TenantId);
