using IT4You.Application.DTOs;

namespace IT4You.Application.Interfaces;

public interface IFavoriteService
{
    Task<List<FavoriteQuestionResponse>> GetFavoritesAsync(string userId);
    Task<FavoriteQuestionResponse> AddFavoriteAsync(string userId, CreateFavoriteRequest request);
    Task<bool> RemoveFavoriteAsync(string userId, string favoriteId);
}
