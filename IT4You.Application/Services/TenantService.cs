using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.Domain.Entities;
using IT4You.Application.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

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
        tenant.ErpToken = request.ErpToken;
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
                u.HasBankingDashboardAccess
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
            tenant.CreatedAt,
            tenant.Users.Select(u => new UserDto(u.Id, u.Name, u.Email, u.Role.ToString(), u.QueryCount, u.CreatedAt, u.IsActive, u.HasPayableChatAccess, u.HasPayableDashboardAccess, u.HasReceivableChatAccess, u.HasReceivableDashboardAccess, u.HasBankingChatAccess, u.HasBankingDashboardAccess))
        );
    }

    public async Task CreateUserAsync(string tenantId, CreateUserRequest request)
    {
        var existingUser = await _context.Users.AnyAsync(u => u.Email == request.Email);
        if (existingUser) throw new Exception("Email already in use");

        var hashedPassword = BCrypt.Net.BCrypt.HashPassword(request.Password);
        var isAdmin = Enum.TryParse<UserRole>(request.Role, out var parsedRole) && (parsedRole == UserRole.TENANT_ADMIN || parsedRole == UserRole.SUPER_ADMIN || parsedRole == UserRole.ADMIN);

        var user = new User
        {
            Email = request.Email,
            Password = hashedPassword,
            Name = request.Name ?? request.Email.Split('@')[0],
            Role = Enum.TryParse<UserRole>(request.Role, out var role) ? role : UserRole.TENANT_USER,
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

        var isAdmin = user.Role == UserRole.TENANT_ADMIN || user.Role == UserRole.SUPER_ADMIN || user.Role == UserRole.ADMIN;

        if (user.Email != request.Email)
        {
            var emailExists = await _context.Users.AnyAsync(u => u.Email == request.Email);
            if (emailExists) throw new Exception("Email already in use by another user");
            user.Email = request.Email;
        }

        if (Enum.TryParse<UserRole>(request.Role, out var role))
        {
            user.Role = role;
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
