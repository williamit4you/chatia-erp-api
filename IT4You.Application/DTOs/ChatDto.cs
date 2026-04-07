namespace IT4You.Application.DTOs;

public record ChatRequest(string Message, List<ChatMessageDto>? History, string? SessionId);

public record ChartAnalysisRequest(string Message, List<ChatMessageDto>? History, string ChartId, string ChartTitle, string ChartDescription, object ChartData, string? StartDate = null, string? EndDate = null);

public record ChatMessageDto(string Role, string Content);

public record ChatResponse(string Reply, string SessionId, string? SqlQueries = null);

public record SqlLogDto(string MessageId, DateTime Date, string UserName, string UserEmail, string UserQuestion, string AiReply, string SqlQueries);
