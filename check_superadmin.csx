using System;
using System.Linq;
using IT4You.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using BCrypt.Net;

var connString = "Host=localhost;Database=chaterpia;Username=postgres;Password=postgres"; // Adjust if needed
var dataSourceBuilder = new NpgsqlDataSourceBuilder(connString);
dataSourceBuilder.MapEnum<IT4You.Domain.Entities.UserRole>("RoleName", nameTranslator: new Npgsql.NameTranslation.NpgsqlNullNameTranslator());
var dataSource = dataSourceBuilder.Build();

var optionsBuilder = new DbContextOptionsBuilder<IT4You.Application.Data.AppDbContext>();
optionsBuilder.UseNpgsql(dataSource);

using var context = new IT4You.Application.Data.AppDbContext(optionsBuilder.Options);

var users = context.Users.Where(u => u.Role == IT4You.Domain.Entities.UserRole.SUPER_ADMIN).ToList();

Console.WriteLine($"Found {users.Count} superadmins.");
foreach (var u in users) {
    Console.WriteLine($"Email: {u.Email}, Role: {u.Role}, HasTenant: {u.TenantId != null}, Active: {u.IsActive}");
}
