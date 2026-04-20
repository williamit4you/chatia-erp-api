namespace IT4You.Domain.Entities;

public class Tenant
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string Name { get; set; } = string.Empty;
    public string Cnpj { get; set; } = string.Empty;
    public string? IaToken { get; set; }
    public string? ChatAiToken { get; set; }
    public string? ErpToken { get; set; }
    
    // Database Configuration
    public string? DbIp { get; set; }
    public string? DbName { get; set; }
    public string? DbType { get; set; } // SQL Server / Oracle
    public string? DbUser { get; set; }
    public string? DbPassword { get; set; } // Stored Encrypted

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    public bool IsActive { get; set; } = true;

    public ICollection<User> Users { get; set; } = new List<User>();
    public ICollection<ChatSession> ChatSessions { get; set; } = new List<ChatSession>();
}
