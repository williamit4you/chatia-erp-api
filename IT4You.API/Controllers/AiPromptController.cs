using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using System.Text.Json;
using System.Text;
using System.Net.Http.Headers;

namespace IT4You.API.Controllers;

/// <summary>
/// Controller isolada para testes diretos com a API do Gemini.
/// Não utiliza Semantic Kernel nem Banco de Dados.
/// </summary>
[AllowAnonymous]
[ApiController]
[Route("api/ai-prompt")]
public class AiPromptController : ControllerBase
{
    private readonly ILogger<AiPromptController> _logger;

    public AiPromptController(ILogger<AiPromptController> logger)
    {
        _logger = logger;
    }

    [HttpPost("test")]
    public async Task<IActionResult> TestPrompt([FromBody] AiPromptRequest request)
    {
        if (string.IsNullOrEmpty(request.ApiKey))
            return BadRequest(new { message = "API Key é obrigatória no corpo da requisição." });

        _logger.LogInformation("Iniciando teste Gemini com Model: {Model}", request.ModelId);

        try
        {
            // O erro 400 (API_KEY_INVALID) pode ser causado por espaços ou caracteres invisíveis
            var apiKey = request.ApiKey.Trim();
            
            // Tentamos a v1beta que é a mais flexível para modelos flash
            var url = $"https://generativelanguage.googleapis.com/v1beta/models/{request.ModelId}:generateContent?key={apiKey}";
            
            using var httpClient = new HttpClient();
            
            var payload = new 
            { 
                contents = new[] 
                { 
                    new { 
                        parts = new[] { new { text = request.Prompt } } 
                    } 
                } 
            };
            
            var jsonPayload = JsonSerializer.Serialize(payload);
            var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
            
            // Adicionamos um User-Agent por precaução
            httpClient.DefaultRequestHeaders.Add("User-Agent", "IT4You-AI-Assistant/.NET");

            var response = await httpClient.PostAsync(url, content);
            var responseBody = await response.Content.ReadAsStringAsync();
            
            _logger.LogInformation("Gemini API Response Status: {StatusCode}", response.StatusCode);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Gemini API Error: {Body}", responseBody);
                return StatusCode((int)response.StatusCode, new 
                { 
                    success = false,
                    message = "A Google retornou um erro.",
                    googleError = responseBody,
                    urlUsed = url.Replace(apiKey, "AIza...REDACTED")
                });
            }

            return Ok(new 
            { 
                success = true,
                rawResponse = responseBody
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exceção ao chamar Gemini API");
            return StatusCode(500, new { message = ex.Message });
        }
    }
}

public class AiPromptRequest
{
    public string ApiKey { get; set; } = string.Empty;
    public string Prompt { get; set; } = "Diga: Olá, o teste funcionou!";
    public string ModelId { get; set; } = "gemini-1.5-flash";
}
