using IT4You.Application.Data;
using IT4You.Application.Interfaces;
using IT4You.Application.Services;
using IT4You.Application.FinanceAnalytics.Interfaces;
using IT4You.Application.FinanceAnalytics.Services;
using IT4You.Infrastructure.Repositories;
using IT4You.Domain.Entities;
using IT4You.Application.FinanceAnalytics.Interfaces;
using IT4You.Application.FinanceAnalytics.Services;
using IT4You.Application.Services;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;
using Microsoft.Extensions.Configuration;
using System.Security.Claims;

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
                            "https://desenvolvimento-chat-erp-ia-web-dev.ykzlki.easypanel.host"
                            "https://desenvolvimento-chat-erp-ia-web.ykzlki.easypanel.host")
                        .AllowAnyMethod()
                        .AllowAnyHeader()
                        .AllowCredentials());
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var connString = builder.Configuration.GetConnectionString("AppConnection");
var dataSourceBuilder = new Npgsql.NpgsqlDataSourceBuilder(connString);
dataSourceBuilder.MapEnum<UserRole>("RoleName", nameTranslator: new Npgsql.NameTranslation.NpgsqlNullNameTranslator());
var dataSource = dataSourceBuilder.Build();

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(dataSource));

// Register Services
builder.Services.AddScoped<IAuthService, AuthService>();
builder.Services.AddScoped<IChatService, ChatService>();
builder.Services.AddScoped<ITenantService, TenantService>();
builder.Services.AddScoped<IFavoriteService, FavoriteService>();
builder.Services.AddScoped<IUserService, UserService>();
builder.Services.AddScoped<IFinanceAnalyticsRepository, FinanceAnalyticsRepository>();
builder.Services.AddScoped<IFinanceAnalyticsService, FinanceAnalyticsService>();
builder.Services.AddScoped<ICacheWarmingService, CacheWarmingService>();

// Redis Configuration
var redisConn = builder.Configuration.GetSection("Redis")["ConnectionString"] ?? "localhost";
builder.Services.AddSingleton<IConnectionMultiplexer>(ConnectionMultiplexer.Connect(redisConn));
builder.Services.AddSingleton<IT4You.Application.Services.RedisCacheService>();

builder.Services.AddScoped<IT4You.Application.Plugins.ErpPlugin>();
builder.Services.AddScoped<IFinancialAgentFactory, FinancialAgentFactory>();

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

app.MapControllers();

app.Run();
