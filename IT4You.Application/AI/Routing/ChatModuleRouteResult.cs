namespace IT4You.Application.AI.Routing;

public record ChatModuleRouteResult(ChatModule Module, bool NeedsClarification, string? ClarificationPrompt = null);

