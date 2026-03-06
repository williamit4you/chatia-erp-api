using System.Security.Claims;
using IT4You.Application.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.EntityFrameworkCore;

namespace IT4You.API.Filters;

public class ConcurrentSessionFilter : IAsyncActionFilter
{
    private readonly AppDbContext _context;

    public ConcurrentSessionFilter(AppDbContext context)
    {
        _context = context;
    }

    public async Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next)
    {
        var user = context.HttpContext.User;
        if (user.Identity != null && user.Identity.IsAuthenticated)
        {
            var userId = user.FindFirst("userId")?.Value;
            var sessionId = user.FindFirst("sessionId")?.Value;

            if (!string.IsNullOrEmpty(userId))
            {
                var dbUser = await _context.Users
                    .AsNoTracking()
                    .FirstOrDefaultAsync(u => u.Id == userId);
                    
                if (dbUser == null)
                {
                    context.Result = new UnauthorizedObjectResult(new { error = "USER_NOT_FOUND", message = "Usuário não encontrado." });
                    return;
                }
                
                if (!dbUser.IsActive)
                {
                    context.Result = new ObjectResult(new { error = "FORBIDDEN_INACTIVE_USER", message = "favor consulte o administrador do sistema" })
                    {
                        StatusCode = 403
                    };
                    return;
                }

                if (!string.IsNullOrEmpty(sessionId) && dbUser.CurrentSessionId != sessionId)
                {
                    context.Result = new ObjectResult(new { error = "CONCURRENT_SESSION", message = "outro usuário logado com esse e-mail" })
                    {
                        StatusCode = 403
                    };
                    return;
                }
            }
        }

        await next();
    }
}
