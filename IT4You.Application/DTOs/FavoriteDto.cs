namespace IT4You.Application.DTOs;

public record FavoriteQuestionResponse(string Id, string QuestionText, DateTime CreatedAt);

public record CreateFavoriteRequest(string QuestionText);
