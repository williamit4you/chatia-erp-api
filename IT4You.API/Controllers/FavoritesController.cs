using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace IT4You.API.Controllers;

[Authorize]
[ApiController]
[Route("api/[controller]")]
public class FavoritesController : ControllerBase
{
    private readonly IFavoriteService _favoriteService;

    public FavoritesController(IFavoriteService favoriteService)
    {
        _favoriteService = favoriteService;
    }

    private string GetUserId() => User.FindFirst("userId")?.Value ?? string.Empty;

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        var userId = GetUserId();
        if (string.IsNullOrEmpty(userId)) return Unauthorized();

        var favorites = await _favoriteService.GetFavoritesAsync(userId);
        return Ok(favorites);
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] CreateFavoriteRequest request)
    {
        var userId = GetUserId();
        if (string.IsNullOrEmpty(userId)) return Unauthorized();
        if (string.IsNullOrWhiteSpace(request.QuestionText)) return BadRequest(new { message = "Question text cannot be empty" });

        var favorite = await _favoriteService.AddFavoriteAsync(userId, request);
        return Ok(favorite);
    }

    [HttpDelete("{id}")]
    public async Task<IActionResult> Delete(string id)
    {
        var userId = GetUserId();
        if (string.IsNullOrEmpty(userId)) return Unauthorized();

        var success = await _favoriteService.RemoveFavoriteAsync(userId, id);
        if (!success) return NotFound();

        return NoContent();
    }
}
