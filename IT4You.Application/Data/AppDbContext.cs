using IT4You.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace IT4You.Application.Data;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<Tenant> Tenants { get; set; }
    public DbSet<User> Users { get; set; }
    public DbSet<ChatSession> ChatSessions { get; set; }
    public DbSet<ChatMessage> ChatMessages { get; set; }
    public DbSet<FavoriteQuestion> FavoriteQuestions { get; set; }

    public override int SaveChanges()
    {
        UpdateTimestamps();
        return base.SaveChanges();
    }

    public override Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        UpdateTimestamps();
        return base.SaveChangesAsync(cancellationToken);
    }

    private void UpdateTimestamps()
    {
        var entries = ChangeTracker.Entries()
            .Where(e => e.State == EntityState.Added || e.State == EntityState.Modified);

        foreach (var entry in entries)
        {
            foreach (var property in entry.Properties)
            {
                if (property.Metadata.ClrType == typeof(DateTime) && property.CurrentValue is DateTime dt)
                {
                    if (dt.Kind == DateTimeKind.Unspecified)
                    {
                        property.CurrentValue = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                    }
                    else if (dt.Kind == DateTimeKind.Local)
                    {
                        property.CurrentValue = dt.ToUniversalTime();
                    }
                }
                else if (property.Metadata.ClrType == typeof(DateTime?) && property.CurrentValue is DateTime dtNullable)
                {
                    if (dtNullable.Kind == DateTimeKind.Unspecified)
                    {
                        property.CurrentValue = DateTime.SpecifyKind(dtNullable, DateTimeKind.Utc);
                    }
                    else if (dtNullable.Kind == DateTimeKind.Local)
                    {
                        property.CurrentValue = dtNullable.ToUniversalTime();
                    }
                }
            }
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        
        modelBuilder.HasPostgresEnum<UserRole>(name: "RoleName", nameTranslator: new Npgsql.NameTranslation.NpgsqlNullNameTranslator());

        // --- MAP TABLE NAMES (Singular like Prisma) ---
        modelBuilder.Entity<Tenant>().ToTable("Tenant");
        modelBuilder.Entity<User>().ToTable("User");
        modelBuilder.Entity<ChatSession>().ToTable("ChatSession");
        modelBuilder.Entity<ChatMessage>().ToTable("ChatMessage");
        modelBuilder.Entity<FavoriteQuestion>().ToTable("FavoriteQuestion");

        // --- COMPREHENSIVE COLUMN MAPPING (Match Prisma camelCase) ---

        // User
        modelBuilder.Entity<User>(entity => {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Email).HasColumnName("email");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Password).HasColumnName("password");
            entity.Property(e => e.Role).HasColumnName("role");
            entity.Property(e => e.TenantId).HasColumnName("tenantId");
            entity.Property(e => e.QueryCount).HasColumnName("queryCount");
            entity.Property(e => e.IsActive).HasColumnName("isActive");
            entity.Property(e => e.HasPayableChatAccess).HasColumnName("hasPayableChatAccess");
            entity.Property(e => e.HasPayableDashboardAccess).HasColumnName("hasPayableDashboardAccess");
            entity.Property(e => e.HasReceivableChatAccess).HasColumnName("hasReceivableChatAccess");
            entity.Property(e => e.HasReceivableDashboardAccess).HasColumnName("hasReceivableDashboardAccess");
            entity.Property(e => e.HasBankingChatAccess).HasColumnName("hasBankingChatAccess");
            entity.Property(e => e.HasBankingDashboardAccess).HasColumnName("hasBankingDashboardAccess");
            entity.Property(e => e.CurrentSessionId).HasColumnName("currentSessionId");
            entity.Property(e => e.CreatedAt).HasColumnName("createdAt");
            entity.Property(e => e.UpdatedAt).HasColumnName("updatedAt");
            
            entity.HasOne(d => d.Tenant)
                .WithMany(p => p.Users)
                .HasForeignKey(d => d.TenantId)
                .OnDelete(DeleteBehavior.SetNull);
        });

        // Tenant
        modelBuilder.Entity<Tenant>(entity => {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Cnpj).HasColumnName("cnpj");
            entity.Property(e => e.IaToken).HasColumnName("iaToken");
            entity.Property(e => e.ErpToken).HasColumnName("erpToken");
            entity.Property(e => e.IsActive).HasColumnName("isActive");
            entity.Property(e => e.CreatedAt).HasColumnName("createdAt");
            entity.Property(e => e.UpdatedAt).HasColumnName("updatedAt");
        });

        // ChatSession
        modelBuilder.Entity<ChatSession>(entity => {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Title).HasColumnName("title");
            entity.Property(e => e.TenantId).HasColumnName("tenantId");
            entity.Property(e => e.UserId).HasColumnName("userId");
            entity.Property(e => e.CreatedAt).HasColumnName("createdAt");
            entity.Property(e => e.UpdatedAt).HasColumnName("updatedAt");
            entity.Property(e => e.IsVisible).HasColumnName("isVisible").HasDefaultValue(true);

            entity.HasOne(d => d.Tenant)
                .WithMany(p => p.ChatSessions)
                .HasForeignKey(d => d.TenantId)
                .OnDelete(DeleteBehavior.Cascade);

            entity.HasOne(d => d.User)
                .WithMany(p => p.ChatSessions)
                .HasForeignKey(d => d.UserId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        // ChatMessage
        modelBuilder.Entity<ChatMessage>(entity => {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.SessionId).HasColumnName("sessionId");
            entity.Property(e => e.Role).HasColumnName("role");
            entity.Property(e => e.Content).HasColumnName("content");
            entity.Property(e => e.SqlQueries).HasColumnName("sqlQueries");
            entity.Property(e => e.CreatedAt).HasColumnName("createdAt");

            entity.HasOne(d => d.Session)
                .WithMany(p => p.Messages)
                .HasForeignKey(d => d.SessionId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        // FavoriteQuestion
        modelBuilder.Entity<FavoriteQuestion>(entity => {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.QuestionText).HasColumnName("questionText");
            entity.Property(e => e.UserId).HasColumnName("userId");
            entity.Property(e => e.CreatedAt).HasColumnName("createdAt");

            entity.HasOne(d => d.User)
                .WithMany(p => p.FavoriteQuestions)
                .HasForeignKey(d => d.UserId)
                .OnDelete(DeleteBehavior.Cascade);
        });
    }
}
