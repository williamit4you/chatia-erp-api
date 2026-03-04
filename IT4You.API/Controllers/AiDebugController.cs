using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using System.Text.Json;
using System.Text;

namespace IT4You.API.Controllers;

[AllowAnonymous]
[ApiController]
[Route("api/ai-debug")]
public class AiDebugController : ControllerBase
{
    private readonly ILogger<AiDebugController> _logger;

    public AiDebugController(ILogger<AiDebugController> logger)
    {
        _logger = logger;
    }

    [HttpPost("test-raw-http")]
    public async Task<IActionResult> TestRawHttp([FromBody] AiDebugRequest request)
    {
        if (string.IsNullOrEmpty(request.ApiKey))
            return BadRequest(new { message = "API Key é obrigatória" });

        var message = request.Message ?? "Oi, você está me ouvindo?";
        _logger.LogInformation("Iniciando teste RAW HTTP para Gemini. Model: {Model}", request.ModelId);

        try
        {
            var apiKey = request.ApiKey.Trim();
            // Usando v1beta por ser a mais comum para falhas de 404
            var url = $"https://generativelanguage.googleapis.com/v1beta/models/{request.ModelId}:generateContent?key={apiKey}";
            
            using var httpClient = new HttpClient();
            var payload = new 
            { 
                contents = new[] { new { parts = new[] { new { text = message } } } } 
            };
            
            var jsonPayload = JsonSerializer.Serialize(payload);
            var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
            
            var response = await httpClient.PostAsync(url, content);
            var responseBody = await response.Content.ReadAsStringAsync();
            
            _logger.LogInformation("STATUS RAW HTTP: {StatusCode}", response.StatusCode);

            return Ok(new 
            { 
                statusCode = (int)response.StatusCode,
                isSuccess = response.IsSuccessStatusCode,
                urlEvaluated = url.Replace(apiKey, "AIza...TRIMMED"),
                responseBody = responseBody.Length > 2000 ? responseBody.Substring(0, 2000) + "..." : responseBody
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Erro no teste RAW HTTP");
            return StatusCode(500, new { message = ex.Message, detail = ex.ToString() });
        }
    }
}

public class AiDebugRequest
{
    public string ApiKey { get; set; } = string.Empty;
    public string Message { get; set; } = "Oi, você está me ouvindo?";
    public string ModelId { get; set; } = "gemini-1.5-flash";
}
