using IT4You.Application.DTOs;
using IT4You.Domain.Entities;
using System.Collections.Generic;

namespace IT4You.Application.Interfaces;

public interface IChatService
{
    Task<ChatResponse> ProcessMessageAsync(ChatRequest request, string userId, string tenantId);
    Task<ChatResponse> ProcessChartAnalysisAsync(ChartAnalysisRequest request, string userId, string tenantId);
    Task<List<ChatSession>> GetSessionsAsync(string userId, string tenantId);
    Task<List<ChatMessage>> GetMessagesAsync(string sessionId);
}
