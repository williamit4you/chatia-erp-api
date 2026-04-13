using Pgvector;
using System.ComponentModel.DataAnnotations.Schema;

namespace IT4You.Domain.Entities;

public class AgentMemory
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    
    // Pode ser Nulo. Se for null = RAG Global (ex: Dicionário). Se tiver ID = Inteligência do Usuário.
    public string? UserId { get; set; } 
    public virtual User? User { get; set; }

    public string Content { get; set; } = string.Empty;

    // Coluna vetorial para 1536 dimensões (Padrão OpenAI text-embedding-3-small)
    [Column("embedding", TypeName = "vector(1536)")]
    public Vector? Embedding { get; set; }
    public bool IsActive { get; set; } = true;
    
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
