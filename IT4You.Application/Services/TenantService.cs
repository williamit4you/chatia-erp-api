using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.Domain.Entities;
using IT4You.Application.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using IT4You.Application.Helpers;

namespace IT4You.Application.Services;

public class TenantService : ITenantService
{
    private readonly AppDbContext _context;
    private readonly ILogger<TenantService> _logger;

    public TenantService(AppDbContext context, ILogger<TenantService> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task UpdateSettingsAsync(string tenantId, UpdateSettingsRequest request)
    {
        _logger.LogInformation("Updating settings for tenant: {TenantId}", tenantId);
        var tenant = await _context.Tenants.FindAsync(tenantId);
        if (tenant == null) throw new Exception("Tenant not found");

        tenant.IaToken = request.IaToken;
        tenant.ChatAiToken = request.ChatAiToken;
        tenant.ErpToken = request.ErpToken;
        
        tenant.DbIp = request.DbIp;
        tenant.DbName = request.DbName;
        tenant.DbType = request.DbType;
        tenant.DbUser = request.DbUser;
        if (!string.IsNullOrEmpty(request.DbPassword))
        {
            tenant.DbPassword = EncryptionHelper.Encrypt(request.DbPassword);
        }
        
        tenant.UpdatedAt = DateTime.UtcNow;

        await _context.SaveChangesAsync();
    }

    public async Task<IEnumerable<TenantDto>> GetAllTenantsAsync()
    {
        _logger.LogInformation("Fetching all tenants and users...");
        var tenants = await _context.Tenants
            .Include(t => t.Users)
            .ToListAsync();

        _logger.LogInformation("Found {Count} tenants", tenants.Count);
        foreach (var t in tenants)
        {
            _logger.LogInformation("Tenant {Name} ({Id}) has {UserCount} users", t.Name, t.Id, t.Users.Count);
        }

        return tenants.Select(t => new TenantDto(
            t.Id, 
            t.Name, 
            t.Cnpj, 
            t.IaToken, 
            t.ErpToken, 
            t.ChatAiToken,
            t.DbIp,
            t.DbName,
            t.DbType,
            t.DbUser,
            EncryptionHelper.Decrypt(t.DbPassword ?? ""),
            t.CreatedAt,
            t.Users.Select(u => new UserDto(
                u.Id, 
                u.Name, 
                u.Email, 
                u.Role.ToString(), 
                u.QueryCount, 
                u.CreatedAt, 
                u.IsActive, 
                u.HasPayableChatAccess,
                u.HasPayableDashboardAccess,
                u.HasReceivableChatAccess,
                u.HasReceivableDashboardAccess,
                u.HasBankingChatAccess,
                u.HasBankingDashboardAccess,
                u.IsInactive,
                u.BlockedUntil
            ))
        ));
    }

    public async Task<TenantDto?> GetTenantAsync(string tenantId)
    {
        _logger.LogInformation("Fetching single tenant: {TenantId}", tenantId);
        var tenant = await _context.Tenants
            .Include(t => t.Users)
            .FirstOrDefaultAsync(t => t.Id == tenantId);

        if (tenant == null) 
        {
            _logger.LogWarning("Tenant not found: {TenantId}", tenantId);
            return null;
        }

        _logger.LogInformation("Tenant {Name} has {UserCount} users", tenant.Name, tenant.Users.Count);

        return new TenantDto(
            tenant.Id, 
            tenant.Name, 
            tenant.Cnpj, 
            tenant.IaToken, 
            tenant.ErpToken, 
            tenant.ChatAiToken,
            tenant.DbIp,
            tenant.DbName,
            tenant.DbType,
            tenant.DbUser,
            EncryptionHelper.Decrypt(tenant.DbPassword ?? ""),
            tenant.CreatedAt,
            tenant.Users.Select(u => new UserDto(u.Id, u.Name, u.Email, u.Role.ToString(), u.QueryCount, u.CreatedAt, u.IsActive, u.HasPayableChatAccess, u.HasPayableDashboardAccess, u.HasReceivableChatAccess, u.HasReceivableDashboardAccess, u.HasBankingChatAccess, u.HasBankingDashboardAccess, u.IsInactive, u.BlockedUntil))
        );
    }

    public async Task CreateUserAsync(string tenantId, CreateUserRequest request)
    {
        var existingUser = await _context.Users.AnyAsync(u => u.Email == request.Email);
        if (existingUser) throw new Exception("Email already in use");

        var hashedPassword = BCrypt.Net.BCrypt.HashPassword(request.Password);
        var role = Enum.TryParse<UserRole>(request.Role, out var parsedRole) ? parsedRole : UserRole.TENANT_USER;
        if (role == UserRole.SUPER_ADMIN) throw new Exception("Não é permitido criar SUPER_ADMIN por este fluxo");
        var isAdmin = role == UserRole.TENANT_ADMIN || role == UserRole.ADMIN;

        var user = new User
        {
            Email = request.Email,
            Password = hashedPassword,
            Name = request.Name ?? request.Email.Split('@')[0],
            Role = role,
            TenantId = tenantId,
            HasPayableChatAccess = isAdmin || request.HasPayableChatAccess,
            HasPayableDashboardAccess = isAdmin || request.HasPayableDashboardAccess,
            HasReceivableChatAccess = isAdmin || request.HasReceivableChatAccess,
            HasReceivableDashboardAccess = isAdmin || request.HasReceivableDashboardAccess,
            HasBankingChatAccess = isAdmin || request.HasBankingChatAccess,
            HasBankingDashboardAccess = isAdmin || request.HasBankingDashboardAccess
        };

        _context.Users.Add(user);
        await _context.SaveChangesAsync();
    }

    public async Task UpdateUserAsync(string tenantId, string userId, UpdateUserRequest request)
    {
        var user = await _context.Users.FirstOrDefaultAsync(u => u.Id == userId && u.TenantId == tenantId);
        if (user == null) throw new Exception("User not found or does not belong to this tenant");

        var requestedRole = user.Role;
        if (Enum.TryParse<UserRole>(request.Role, out var parsedRole))
        {
            if (parsedRole == UserRole.SUPER_ADMIN) throw new Exception("Não é permitido promover usuários para SUPER_ADMIN por este fluxo");
            requestedRole = parsedRole;
            user.Role = parsedRole;
        }

        var isAdmin = requestedRole == UserRole.TENANT_ADMIN || requestedRole == UserRole.ADMIN;

        if (!string.IsNullOrWhiteSpace(request.Email) && user.Email != request.Email)
        {
            var emailExists = await _context.Users.AnyAsync(u => u.Email == request.Email);
            if (emailExists) throw new Exception("Email already in use by another user");
            user.Email = request.Email;
        }

        if (request.IsActive.HasValue)
        {
            user.IsActive = request.IsActive.Value;
        }

        if (request.HasPayableChatAccess.HasValue) user.HasPayableChatAccess = isAdmin || request.HasPayableChatAccess.Value;
        if (request.HasPayableDashboardAccess.HasValue) user.HasPayableDashboardAccess = isAdmin || request.HasPayableDashboardAccess.Value;
        if (request.HasReceivableChatAccess.HasValue) user.HasReceivableChatAccess = isAdmin || request.HasReceivableChatAccess.Value;
        if (request.HasReceivableDashboardAccess.HasValue) user.HasReceivableDashboardAccess = isAdmin || request.HasReceivableDashboardAccess.Value;
        if (request.HasBankingChatAccess.HasValue) user.HasBankingChatAccess = isAdmin || request.HasBankingChatAccess.Value;
        if (request.HasBankingDashboardAccess.HasValue) user.HasBankingDashboardAccess = isAdmin || request.HasBankingDashboardAccess.Value;

        if (request.IsInactive.HasValue) user.IsInactive = request.IsInactive.Value;
        
        if (request.Unblock == true)
        {
            user.BlockedUntil = null;
        }
        else if (request.BlockedUntil.HasValue)
        {
            user.BlockedUntil = request.BlockedUntil.Value;
        }

        if (!string.IsNullOrWhiteSpace(request.Name))
        {
            user.Name = request.Name;
        }

        if (!string.IsNullOrWhiteSpace(request.Password))
        {
            user.Password = BCrypt.Net.BCrypt.HashPassword(request.Password);
        }

        user.UpdatedAt = DateTime.UtcNow;
        await _context.SaveChangesAsync();
    }
}
