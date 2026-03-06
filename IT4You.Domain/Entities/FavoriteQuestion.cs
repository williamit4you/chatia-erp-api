namespace IT4You.Domain.Entities;

public class FavoriteQuestion
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string QuestionText { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    
    public string? UserId { get; set; }
    public User? User { get; set; }
}
