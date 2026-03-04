using IT4You.Application.Data;
using IT4You.Application.Interfaces;
using IT4You.Application.Services;
using IT4You.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

var builder = WebApplication.CreateBuilder(args);

// Ensure logging is configured
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddDebug();

// Add services to the container.
builder.Services.AddControllers();

builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowNextJS",
        policy => policy.WithOrigins("http://localhost:3010", "https://localhost:3010")
                        .AllowAnyMethod()
                        .AllowAnyHeader()
                        .AllowCredentials());
});
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("AppConnection")));

// Register Services
builder.Services.AddScoped<IAuthService, AuthService>();
builder.Services.AddScoped<IChatService, ChatService>();
builder.Services.AddScoped<ITenantService, TenantService>();

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
        ValidateLifetime = true
    };
});

var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseCors("AllowNextJS");

app.UseSwagger();
app.UseSwaggerUI();


// app.UseHttpsRedirection(); // Commented out to avoid local CORS issues with redirects

app.UseAuthentication();
app.UseAuthorization();

app.MapControllers();

app.Run();
