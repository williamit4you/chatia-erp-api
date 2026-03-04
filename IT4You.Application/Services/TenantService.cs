using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.Domain.Entities;
using IT4You.Application.Data;
using Microsoft.EntityFrameworkCore;

namespace IT4You.Application.Services;

public class TenantService : ITenantService
{
    private readonly AppDbContext _context;

    public TenantService(AppDbContext context)
    {
        _context = context;
    }

    public async Task UpdateSettingsAsync(string tenantId, UpdateSettingsRequest request)
    {
        var tenant = await _context.Tenants.FindAsync(tenantId);
        if (tenant == null) throw new Exception("Tenant not found");

        tenant.IaToken = request.IaToken;
        tenant.ErpToken = request.ErpToken;
        tenant.UpdatedAt = DateTime.UtcNow;

        await _context.SaveChangesAsync();
    }

    public async Task<IEnumerable<TenantDto>> GetAllTenantsAsync()
    {
        return await _context.Tenants
            .Include(t => t.Users)
            .Select(t => new TenantDto(
                t.Id, 
                t.Name, 
                t.Cnpj, 
                t.IaToken, 
                t.ErpToken, 
                t.CreatedAt,
                t.Users.Select(u => new UserDto(u.Id, u.Name, u.Email, u.Role.ToString(), u.QueryCount, u.CreatedAt))
            ))
            .ToListAsync();
    }

    public async Task CreateUserAsync(string tenantId, CreateUserRequest request)
    {
        var existingUser = await _context.Users.AnyAsync(u => u.Email == request.Email);
        if (existingUser) throw new Exception("Email already in use");

        var hashedPassword = BCrypt.Net.BCrypt.HashPassword(request.Password);

        var user = new User
        {
            Email = request.Email,
            Password = hashedPassword,
            Name = request.Name ?? request.Email.Split('@')[0],
            Role = Enum.TryParse<UserRole>(request.Role, out var role) ? role : UserRole.TENANT_USER,
            TenantId = tenantId
        };

        _context.Users.Add(user);
        await _context.SaveChangesAsync();
    }
}
