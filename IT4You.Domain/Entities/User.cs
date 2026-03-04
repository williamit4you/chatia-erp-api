namespace IT4You.Domain.Entities;

public enum UserRole
{
    SUPER_ADMIN,
    TENANT_ADMIN,
    TENANT_USER
}

public class User
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string? Name { get; set; }
    public string? Email { get; set; }
    public string? Password { get; set; }
    public UserRole Role { get; set; } = UserRole.TENANT_USER;
    public int QueryCount { get; set; } = 0;
    
    public string? TenantId { get; set; }
    public Tenant? Tenant { get; set; }
    
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    
    public ICollection<ChatSession> ChatSessions { get; set; } = new List<ChatSession>();
}
