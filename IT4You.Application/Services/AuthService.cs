using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.Application.Data;
using IT4You.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using BCrypt.Net;

namespace IT4You.Application.Services;

public class AuthService : IAuthService
{
    private readonly AppDbContext _context;
    private readonly ILogger<AuthService> _logger;

    public AuthService(AppDbContext context, ILogger<AuthService> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<LoginResponse?> LoginAsync(LoginRequest request)
    {
        _logger.LogInformation("Attempting login for user: {Email}", request.Email);
        try
        {
            var user = await _context.Users
                .Include(u => u.Tenant)
                .FirstOrDefaultAsync(u => u.Email == request.Email);

            if (user == null)
            {
                _logger.LogWarning("User not found: {Email}", request.Email);
                return null;
            }

            if (!user.IsActive)
            {
                _logger.LogWarning("User is inactive: {Email}", request.Email);
                throw new UnauthorizedAccessException("FORBIDDEN_INACTIVE_USER");
            }

            if (user.Tenant != null && !user.Tenant.IsActive)
            {
                _logger.LogWarning("Tenant is inactive for user: {Email}", request.Email);
                throw new UnauthorizedAccessException("FORBIDDEN_INACTIVE_TENANT");
            }
            
            _logger.LogInformation("User found. Checking password for: {Email}", request.Email);

            bool isPasswordValid = BCrypt.Net.BCrypt.Verify(request.Password, user.Password);
            
            if (!isPasswordValid && request.Password == user.Password)
            {
                _logger.LogInformation("Fallback plain-text password match for: {Email}", request.Email);
                isPasswordValid = true;
            }

            if (!isPasswordValid)
            {
                _logger.LogWarning("Invalid password for: {Email}", request.Email);
                return null;
            }

            user.CurrentSessionId = Guid.NewGuid().ToString();
            await _context.SaveChangesAsync();

            _logger.LogInformation("Login successful for: {Email}", request.Email);
            return new LoginResponse(
                Id: user.Id,
                Token: "", 
                Name: user.Name ?? string.Empty,
                Email: user.Email ?? string.Empty,
                TenantId: user.TenantId ?? string.Empty,
                Role: user.Role.ToString(),
                HasDashboardAccess: user.HasDashboardAccess,
                HasPayableAccess: user.HasPayableAccess,
                HasReceivableAccess: user.HasReceivableAccess,
                CurrentSessionId: user.CurrentSessionId
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during login for user: {Email}", request.Email);
            throw;
        }
    }

    public async Task<RegisterResponse> RegisterAsync(RegisterRequest request)
    {
        _logger.LogInformation("Registering new user/company: {Email}, {CompanyName}", request.Email, request.CompanyName);
        var existingTenant = await _context.Tenants.FirstOrDefaultAsync(t => t.Cnpj == request.Cnpj);
        if (existingTenant != null) throw new Exception("Já existe uma empresa cadastrada com este CNPJ.");

        var existingUser = await _context.Users.FirstOrDefaultAsync(u => u.Email == request.Email);
        if (existingUser != null) throw new Exception("Este e-mail já está em uso.");

        var hashedPassword = BCrypt.Net.BCrypt.HashPassword(request.Password);

        using var transaction = await _context.Database.BeginTransactionAsync();
        try
        {
            var tenant = new Tenant
            {
                Cnpj = request.Cnpj,
                Name = request.CompanyName
            };

            _context.Tenants.Add(tenant);
            await _context.SaveChangesAsync();

            var user = new User
            {
                Email = request.Email,
                Name = request.Name,
                Password = hashedPassword,
                Role = UserRole.TENANT_ADMIN,
                TenantId = tenant.Id
            };

            _context.Users.Add(user);
            await _context.SaveChangesAsync();
            await transaction.CommitAsync();

            _logger.LogInformation("Registration successful for: {Email}", request.Email);
            return new RegisterResponse("Company registered successfully", tenant.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during registration for: {Email}", request.Email);
            await transaction.RollbackAsync();
            throw;
        }
    }
}
