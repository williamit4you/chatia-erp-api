namespace IT4You.Application.DTOs;

public record ChatRequest(string Message, List<ChatMessageDto>? History, string? SessionId);

public record ChartAnalysisRequest(string Message, List<ChatMessageDto>? History, string ChartId, string ChartTitle, string ChartDescription, object ChartData, string? StartDate = null, string? EndDate = null, string? SessionId = null);

public record ChatMessageDto(string Role, string Content);

public record ChatRightRailActionItem(
    string Label,
    string? Action = null,
    string? Metadata = null);

public record ChatRightRailInsightItem(
    string Title,
    string Description,
    string? CtaLabel = null,
    string? CtaAction = null,
    string? Tone = null);

public record ChatRightRail(
    List<ChatRightRailActionItem> Suggestions,
    List<ChatRightRailInsightItem> Insights,
    List<ChatRightRailActionItem> FavoriteQuestions);

public record ChatResponse(
    string Reply,
    string SessionId,
    string? SqlQueries = null,
    int? ContextUsageScore = null,
    string? ExportId = null,
    int ExportTotalLinhas = 0,
    decimal ExportValorTotal = 0,
    int MetricsTotalLinhas = 0,
    decimal MetricsValorTotal = 0,
    ChatRightRail? RightRail = null);

public record SqlLogDto(string MessageId, DateTime Date, string UserName, string UserEmail, string UserQuestion, string AiReply, string SqlQueries);
