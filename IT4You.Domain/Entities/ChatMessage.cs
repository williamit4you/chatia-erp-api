namespace IT4You.Domain.Entities;

public class ChatMessage
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string Role { get; set; } = string.Empty; // "user" ou "model"
    public string Content { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    public string? SqlQueries { get; set; } // JSON array of queries executed by the AI

    public string SessionId { get; set; } = string.Empty;
    public ChatSession? Session { get; set; }
}
