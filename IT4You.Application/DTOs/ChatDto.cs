namespace IT4You.Application.DTOs;

public record ChatRequest(string Message, List<ChatMessageDto>? History, string? SessionId);

public record ChartAnalysisRequest(string Message, List<ChatMessageDto>? History, string ChartId, string ChartTitle, string ChartDescription, object ChartData);

public record ChatMessageDto(string Role, string Content);

public record ChatResponse(string Reply, string SessionId);
