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
    public DbSet<AgentMemory> AgentMemories { get; set; }
    public DbSet<EmailConfiguration> EmailConfigurations { get; set; }
    public DbSet<EmailTemplate> EmailTemplates { get; set; }
    public DbSet<PasswordResetToken> PasswordResetTokens { get; set; }
    public DbSet<EmailLog> EmailLogs { get; set; }

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
        
        modelBuilder.HasPostgresExtension("vector");
        modelBuilder.HasPostgresEnum<UserRole>(name: "RoleName", nameTranslator: new Npgsql.NameTranslation.NpgsqlNullNameTranslator());

        // --- MAP TABLE NAMES (Singular like Prisma) ---
        modelBuilder.Entity<Tenant>().ToTable("Tenant");
        modelBuilder.Entity<User>().ToTable("User");
        modelBuilder.Entity<ChatSession>().ToTable("ChatSession");
        modelBuilder.Entity<ChatMessage>().ToTable("ChatMessage");
        modelBuilder.Entity<ChatMessage>().ToTable("ChatMessage");
        modelBuilder.Entity<FavoriteQuestion>().ToTable("FavoriteQuestion");
        modelBuilder.Entity<AgentMemory>().ToTable("AgentMemory");
        modelBuilder.Entity<EmailConfiguration>().ToTable("EmailConfiguration");
        modelBuilder.Entity<EmailTemplate>().ToTable("EmailTemplate");
        modelBuilder.Entity<PasswordResetToken>().ToTable("PasswordResetToken");
        modelBuilder.Entity<EmailLog>().ToTable("EmailLog");

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
            entity.Property(e => e.IsInactive).HasColumnName("isInactive");
            entity.Property(e => e.BlockedUntil).HasColumnName("blockedUntil");
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
            entity.Property(e => e.ChatAiToken).HasColumnName("chatAiToken");
            entity.Property(e => e.ErpToken).HasColumnName("erpToken");
            entity.Property(e => e.DbIp).HasColumnName("dbIp");
            entity.Property(e => e.DbName).HasColumnName("dbName");
            entity.Property(e => e.DbType).HasColumnName("dbType");
            entity.Property(e => e.DbUser).HasColumnName("dbUser");
            entity.Property(e => e.DbPassword).HasColumnName("dbPassword");
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
            entity.Property(e => e.Module).HasColumnName("module");
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

        // AgentMemory
        modelBuilder.Entity<AgentMemory>(entity => {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.UserId).HasColumnName("userId");
            entity.Property(e => e.Content).HasColumnName("content");
            entity.Property(e => e.Embedding).HasColumnName("embedding").HasColumnType("vector(1536)");
            entity.Property(e => e.IsActive).HasColumnName("isActive");
            entity.Property(e => e.CreatedAt).HasColumnName("createdAt");

            entity.HasOne(d => d.User)
                .WithMany() // Ajustado para ser unilateral sem Navigation Property no lado de User
                .HasForeignKey(d => d.UserId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<EmailConfiguration>(entity => {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.SenderName).HasColumnName("senderName");
            entity.Property(e => e.SenderEmail).HasColumnName("senderEmail");
            entity.Property(e => e.SmtpHost).HasColumnName("smtpHost");
            entity.Property(e => e.SmtpPort).HasColumnName("smtpPort");
            entity.Property(e => e.SmtpUser).HasColumnName("smtpUser");
            entity.Property(e => e.SmtpPasswordEncrypted).HasColumnName("smtpPasswordEncrypted");
            entity.Property(e => e.SmtpUseSsl).HasColumnName("smtpUseSsl");
            entity.Property(e => e.SmtpUseStartTls).HasColumnName("smtpUseStartTls");
            entity.Property(e => e.ReceiveProtocol).HasColumnName("receiveProtocol").HasConversion<string>();
            entity.Property(e => e.ReceiveHost).HasColumnName("receiveHost");
            entity.Property(e => e.ReceivePort).HasColumnName("receivePort");
            entity.Property(e => e.ReceiveUser).HasColumnName("receiveUser");
            entity.Property(e => e.ReceivePasswordEncrypted).HasColumnName("receivePasswordEncrypted");
            entity.Property(e => e.ReceiveUseSsl).HasColumnName("receiveUseSsl");
            entity.Property(e => e.TimeoutSeconds).HasColumnName("timeoutSeconds");
            entity.Property(e => e.IsActive).HasColumnName("isActive");
            entity.Property(e => e.CreatedByUserId).HasColumnName("createdByUserId");
            entity.Property(e => e.UpdatedByUserId).HasColumnName("updatedByUserId");
            entity.Property(e => e.CreatedAt).HasColumnName("createdAt");
            entity.Property(e => e.UpdatedAt).HasColumnName("updatedAt");
        });

        modelBuilder.Entity<EmailTemplate>(entity => {
            entity.HasKey(e => e.Id);
            entity.HasIndex(e => e.Key).IsUnique();
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Key).HasColumnName("key");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Subject).HasColumnName("subject");
            entity.Property(e => e.HtmlBody).HasColumnName("htmlBody");
            entity.Property(e => e.TextBody).HasColumnName("textBody");
            entity.Property(e => e.AllowedVariables).HasColumnName("allowedVariables");
            entity.Property(e => e.IsActive).HasColumnName("isActive");
            entity.Property(e => e.CreatedByUserId).HasColumnName("createdByUserId");
            entity.Property(e => e.UpdatedByUserId).HasColumnName("updatedByUserId");
            entity.Property(e => e.CreatedAt).HasColumnName("createdAt");
            entity.Property(e => e.UpdatedAt).HasColumnName("updatedAt");
        });

        modelBuilder.Entity<PasswordResetToken>(entity => {
            entity.HasKey(e => e.Id);
            entity.HasIndex(e => e.TokenHash).IsUnique();
            entity.HasIndex(e => new { e.UserId, e.CreatedAt });
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.UserId).HasColumnName("userId");
            entity.Property(e => e.TenantId).HasColumnName("tenantId");
            entity.Property(e => e.TokenHash).HasColumnName("tokenHash");
            entity.Property(e => e.ExpiresAt).HasColumnName("expiresAt");
            entity.Property(e => e.UsedAt).HasColumnName("usedAt");
            entity.Property(e => e.RequestedByIp).HasColumnName("requestedByIp");
            entity.Property(e => e.RequestedByUserAgent).HasColumnName("requestedByUserAgent");
            entity.Property(e => e.CreatedAt).HasColumnName("createdAt");

            entity.HasOne(d => d.User)
                .WithMany()
                .HasForeignKey(d => d.UserId)
                .OnDelete(DeleteBehavior.Cascade);

            entity.HasOne(d => d.Tenant)
                .WithMany()
                .HasForeignKey(d => d.TenantId)
                .OnDelete(DeleteBehavior.SetNull);
        });

        modelBuilder.Entity<EmailLog>(entity => {
            entity.HasKey(e => e.Id);
            entity.HasIndex(e => e.CreatedAt);
            entity.HasIndex(e => new { e.Status, e.CreatedAt });
            entity.HasIndex(e => e.ToEmail);
            entity.HasIndex(e => e.TemplateKey);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.TemplateKey).HasColumnName("templateKey");
            entity.Property(e => e.EmailConfigurationId).HasColumnName("emailConfigurationId");
            entity.Property(e => e.ToEmail).HasColumnName("toEmail");
            entity.Property(e => e.ToName).HasColumnName("toName");
            entity.Property(e => e.Subject).HasColumnName("subject");
            entity.Property(e => e.RequestedByUserId).HasColumnName("requestedByUserId");
            entity.Property(e => e.TargetUserId).HasColumnName("targetUserId");
            entity.Property(e => e.TenantId).HasColumnName("tenantId");
            entity.Property(e => e.Status).HasColumnName("status").HasConversion<string>();
            entity.Property(e => e.ProviderMessageId).HasColumnName("providerMessageId");
            entity.Property(e => e.ErrorMessage).HasColumnName("errorMessage");
            entity.Property(e => e.SentAt).HasColumnName("sentAt");
            entity.Property(e => e.CreatedAt).HasColumnName("createdAt");
        });
    }
}
