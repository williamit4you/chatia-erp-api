using IT4You.Application.Data;
using IT4You.Application.Interfaces;
using IT4You.Application.Services;
using IT4You.Application.FinanceAnalytics.Interfaces;
using IT4You.Application.FinanceAnalytics.Services;
using IT4You.Infrastructure.Repositories;
using IT4You.Infrastructure.Services;
using IT4You.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using System.Security.Claims;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

// Fix PostgreSQL DateTime kind issue
AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);

// Ensure logging is configured
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddDebug();

// Add services to the container.
builder.Services.AddControllers(options => 
{
    options.Filters.Add<IT4You.API.Filters.ConcurrentSessionFilter>();
});

builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowNextJS",
        policy => policy.WithOrigins(
                            "http://localhost:3010", 
                            "https://localhost:3010",
                            "http://localhost:8081",
                            "http://192.168.0.113:8081",
                            "https://desenvolvimento-chat-erp-ia-web-dev.ykzlki.easypanel.host",
                            "https://desenvolvimento-chat-erp-ia-web.ykzlki.easypanel.host",
                            "https://swia.it4you.inf.br",
                            "https://teste-swia.it4you.inf.br"
                            )
                        .AllowAnyMethod()
                        .AllowAnyHeader()
                        .AllowCredentials());
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddMemoryCache();

var connString = builder.Configuration.GetConnectionString("AppConnection");
var dataSourceBuilder = new NpgsqlDataSourceBuilder(connString);
dataSourceBuilder.UseVector(); // RE-ADDING WITH USING NPGSQL
dataSourceBuilder.MapEnum<UserRole>("RoleName", nameTranslator: new Npgsql.NameTranslation.NpgsqlNullNameTranslator());
var dataSource = dataSourceBuilder.Build();

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(dataSource, o => o.UseVector()));

// Register Services
builder.Services.AddScoped<IAuthService, AuthService>();
builder.Services.AddScoped<IChatService, ChatService>();
builder.Services.AddScoped<ITenantService, TenantService>();
builder.Services.AddScoped<IFavoriteService, FavoriteService>();
builder.Services.AddScoped<IUserService, UserService>();
builder.Services.AddScoped<IFinanceAnalyticsRepository, FinanceAnalyticsRepository>();
builder.Services.AddScoped<IFinanceAnalyticsService, FinanceAnalyticsService>();

builder.Services.AddScoped<IT4You.Application.Plugins.ErpPlugin>();
builder.Services.AddScoped<IFinancialAgentFactory, FinancialAgentFactory>();

builder.Services.AddHostedService<MonitoringWorker>();

// Configure JWT Authentication
System.IdentityModel.Tokens.Jwt.JwtSecurityTokenHandler.DefaultInboundClaimTypeMap.Clear();

var jwtSettings = builder.Configuration.GetSection("Jwt");
var key = System.Text.Encoding.ASCII.GetBytes(jwtSettings["Key"] ?? "a_very_long_secret_key_that_should_be_in_settings");

builder.Services.AddAuthentication(x =>
{
    x.DefaultAuthenticateScheme = Microsoft.AspNetCore.Authentication.JwtBearer.JwtBearerDefaults.AuthenticationScheme;
    x.DefaultChallengeScheme = Microsoft.AspNetCore.Authentication.JwtBearer.JwtBearerDefaults.AuthenticationScheme;
})
.AddJwtBearer(x =>
{
    x.RequireHttpsMetadata = false;
    x.SaveToken = true;
    x.TokenValidationParameters = new Microsoft.IdentityModel.Tokens.TokenValidationParameters
    {
        ValidateIssuerSigningKey = true,
        IssuerSigningKey = new Microsoft.IdentityModel.Tokens.SymmetricSecurityKey(key),
        ValidateIssuer = false, // Relaxed for local dev
        ValidateAudience = false, // Relaxed for local dev
        RoleClaimType = System.Security.Claims.ClaimTypes.Role,
        ValidateLifetime = true
    };
});

var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseCors("AllowNextJS");

app.UseSwagger();
app.UseSwaggerUI();

app.UseAuthentication();

app.UseAuthorization();

using (var scope = app.Services.CreateScope())
{
    var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    
    // Seed Super Admin if none exists
    if (!context.Users.Any(u => u.Role == IT4You.Domain.Entities.UserRole.SUPER_ADMIN))
    {
        var superAdmin = new IT4You.Domain.Entities.User
        {
            Email = "admin@it4you.com",
            Name = "Super Admin IT4You",
            Password = BCrypt.Net.BCrypt.HashPassword("admin"),
            Role = IT4You.Domain.Entities.UserRole.SUPER_ADMIN,
            IsActive = true
        };
        context.Users.Add(superAdmin);
        context.SaveChanges();
    }
}

app.MapControllers();

app.Run();
