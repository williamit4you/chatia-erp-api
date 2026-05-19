using Microsoft.Agents.AI;

namespace IT4You.Application.Interfaces;

public interface IBudgetAgentFactory
{
    Task<AIAgent> CreateAgentAsync(string chatAiToken, string ragAiToken, bool hasBudgetChatAccess, string userInput = null, string userId = null);
}

