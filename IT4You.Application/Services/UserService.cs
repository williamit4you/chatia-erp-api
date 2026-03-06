using IT4You.Application.Data;
using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace IT4You.Application.Services;

public class UserService : IUserService
{
    private readonly AppDbContext _context;
    private readonly ILogger<UserService> _logger;

    public UserService(AppDbContext context, ILogger<UserService> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<List<UserResponse>> GetUsersByTenantAsync(string tenantId)
    {
        _logger.LogInformation("Getting users for Tenant: {TenantId}", tenantId);
        return await _context.Users
            .Where(u => u.TenantId == tenantId)
            .Select(u => new UserResponse(u.Id, u.Name ?? "", u.Email ?? "", u.Role, u.QueryCount, u.IsActive, u.CreatedAt))
            .ToListAsync();
    }

    public async Task<UserResponse> CreateUserAsync(string tenantId, CreateTenantUserRequest request)
    {
        _logger.LogInformation("Creating new user {Email} for Tenant: {TenantId}", request.Email, tenantId);

        var existingUser = await _context.Users.FirstOrDefaultAsync(u => u.Email == request.Email);
        if (existingUser != null) throw new Exception("Email already registered");

        var user = new User
        {
            TenantId = tenantId,
            Name = request.Name,
            Email = request.Email,
            Password = BCrypt.Net.BCrypt.HashPassword(request.Password),
            Role = request.Role
        };

        _context.Users.Add(user);
        await _context.SaveChangesAsync();

        return new UserResponse(user.Id, user.Name ?? "", user.Email ?? "", user.Role, user.QueryCount, user.IsActive, user.CreatedAt);
    }

    public async Task<bool> UpdateUserStatusAsync(string tenantId, string userId, bool isActive)
    {
        _logger.LogInformation("Updating active status for User: {UserId} to {IsActive}", userId, isActive);

        var user = await _context.Users.FirstOrDefaultAsync(u => u.Id == userId && u.TenantId == tenantId);
        if (user == null) return false;

        user.IsActive = isActive;
        
        // If inactivating, clear the session so the user is immediately kicked out on next request
        if (!isActive)
        {
            user.CurrentSessionId = null;
        }

        _context.Users.Update(user);
        await _context.SaveChangesAsync();
        return true;
    }

    public async Task<UserResponse?> GetUserByIdAsync(string userId)
    {
        var user = await _context.Users.FirstOrDefaultAsync(u => u.Id == userId);
        if (user == null) return null;
        return new UserResponse(user.Id, user.Name ?? "", user.Email ?? "", user.Role, user.QueryCount, user.IsActive, user.CreatedAt);
    }
}
