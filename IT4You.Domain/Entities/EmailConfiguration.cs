namespace IT4You.Domain.Entities;

public enum EmailReceiveProtocol
{
    NONE,
    POP3,
    IMAP
}

public class EmailConfiguration
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string Name { get; set; } = string.Empty;
    public string SenderName { get; set; } = string.Empty;
    public string SenderEmail { get; set; } = string.Empty;
    public string SmtpHost { get; set; } = string.Empty;
    public int SmtpPort { get; set; } = 587;
    public string? SmtpUser { get; set; }
    public string? SmtpPasswordEncrypted { get; set; }
    public bool SmtpUseSsl { get; set; } = true;
    public bool SmtpUseStartTls { get; set; } = true;
    public EmailReceiveProtocol ReceiveProtocol { get; set; } = EmailReceiveProtocol.NONE;
    public string? ReceiveHost { get; set; }
    public int? ReceivePort { get; set; }
    public string? ReceiveUser { get; set; }
    public string? ReceivePasswordEncrypted { get; set; }
    public bool ReceiveUseSsl { get; set; } = true;
    public int TimeoutSeconds { get; set; } = 30;
    public bool IsActive { get; set; } = true;
    public string? CreatedByUserId { get; set; }
    public string? UpdatedByUserId { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}

