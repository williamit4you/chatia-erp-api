using IT4You.Application.Data;
using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using IT4You.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace IT4You.Application.Services;

public class FavoriteService : IFavoriteService
{
    private readonly AppDbContext _context;
    private readonly ILogger<FavoriteService> _logger;

    public FavoriteService(AppDbContext context, ILogger<FavoriteService> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<List<FavoriteQuestionResponse>> GetFavoritesAsync(string userId)
    {
        _logger.LogInformation("Getting favorites for User: {UserId}", userId);
        return await _context.FavoriteQuestions
            .Where(f => f.UserId == userId)
            .OrderByDescending(f => f.CreatedAt)
            .Select(f => new FavoriteQuestionResponse(f.Id, f.QuestionText, f.CreatedAt))
            .ToListAsync();
    }

    public async Task<FavoriteQuestionResponse> AddFavoriteAsync(string userId, CreateFavoriteRequest request)
    {
        _logger.LogInformation("Adding new favorite for User: {UserId}", userId);
        var favorite = new FavoriteQuestion
        {
            UserId = userId,
            QuestionText = request.QuestionText
        };

        _context.FavoriteQuestions.Add(favorite);
        await _context.SaveChangesAsync();

        return new FavoriteQuestionResponse(favorite.Id, favorite.QuestionText, favorite.CreatedAt);
    }

    public async Task<bool> RemoveFavoriteAsync(string userId, string favoriteId)
    {
        _logger.LogInformation("Removing favorite {FavoriteId} for User: {UserId}", favoriteId, userId);
        var favorite = await _context.FavoriteQuestions
            .FirstOrDefaultAsync(f => f.Id == favoriteId && f.UserId == userId);

        if (favorite == null) return false;

        _context.FavoriteQuestions.Remove(favorite);
        await _context.SaveChangesAsync();
        return true;
    }
}
