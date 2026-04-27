namespace IT4You.Domain.Entities;

public enum EmailLogStatus
{
    PENDING,
    SUCCESS,
    ERROR
}

public class EmailLog
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string? TemplateKey { get; set; }
    public string? EmailConfigurationId { get; set; }
    public string ToEmail { get; set; } = string.Empty;
    public string? ToName { get; set; }
    public string Subject { get; set; } = string.Empty;
    public string? RequestedByUserId { get; set; }
    public string? TargetUserId { get; set; }
    public string? TenantId { get; set; }
    public EmailLogStatus Status { get; set; } = EmailLogStatus.PENDING;
    public string? ProviderMessageId { get; set; }
    public string? ErrorMessage { get; set; }
    public DateTime? SentAt { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

