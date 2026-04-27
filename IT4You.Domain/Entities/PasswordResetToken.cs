namespace IT4You.Domain.Entities;

public class PasswordResetToken
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string UserId { get; set; } = string.Empty;
    public User? User { get; set; }
    public string? TenantId { get; set; }
    public Tenant? Tenant { get; set; }
    public string TokenHash { get; set; } = string.Empty;
    public DateTime ExpiresAt { get; set; }
    public DateTime? UsedAt { get; set; }
    public string? RequestedByIp { get; set; }
    public string? RequestedByUserAgent { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

