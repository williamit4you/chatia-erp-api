using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using System.Text.Json;
using System.Text;
using System.Net.Http.Headers;

namespace IT4You.API.Controllers;

[AllowAnonymous]
[ApiController]
[Route("api/ai-test")]
public class AiTestController : ControllerBase
{
    private readonly ILogger<AiTestController> _logger;

    public AiTestController(ILogger<AiTestController> logger)
    {
        _logger = logger;
    }

    [HttpPost("check-gemini")]
    public async Task<IActionResult> CheckGemini([FromBody] GeminiTestRequest request)
    {
        if (string.IsNullOrEmpty(request.ApiKey))
            return BadRequest(new { message = "ApiKey is required" });

        var results = new List<object>();
        var versions = new[] { "v1beta", "v1" };
        var models = new[] { request.ModelId, "gemini-1.5-flash", "gemini-1.5-pro", "gemini-2.0-flash-exp", "gemini-2.0-flash" };

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Add("User-Agent", "IT4You-AI-Debug");

        foreach (var v in versions)
        {
            foreach (var m in models.Where(x => !string.IsNullOrEmpty(x)).Distinct())
            {
                var apiKey = request.ApiKey.Trim();
                var url = $"https://generativelanguage.googleapis.com/{v}/models/{m}:generateContent?key={apiKey}";
                
                try
                {
                    var payload = new { contents = new[] { new { parts = new[] { new { text = "Hello" } } } } };
                    var json = JsonSerializer.Serialize(payload);
                    var content = new StringContent(json, Encoding.UTF8, "application/json");

                    var response = await httpClient.PostAsync(url, content);
                    var body = await response.Content.ReadAsStringAsync();

                    results.Add(new 
                    {
                        version = v,
                        model = m,
                        success = response.IsSuccessStatusCode,
                        status = (int)response.StatusCode,
                        googleMsg = body.Length > 500 ? body.Substring(0, 500) : body
                    });

                    if (response.IsSuccessStatusCode) 
                    {
                        return Ok(new { message = "Sucesso encontrado!", bestUrl = url.Replace(apiKey, "HIDDEN"), response = body });
                    }
                }
                catch (Exception ex)
                {
                    results.Add(new { version = v, model = m, error = ex.Message });
                }
            }
        }

        return BadRequest(new { message = "Nenhuma combinação funcionou", attempts = results });
    }

    [HttpPost("list-models")]
    public async Task<IActionResult> ListModels([FromBody] GeminiTestRequest request)
    {
        if (string.IsNullOrEmpty(request.ApiKey))
            return BadRequest(new { message = "ApiKey is required" });

        var apiKey = request.ApiKey.Trim();
        var url = $"https://generativelanguage.googleapis.com/v1beta/models?key={apiKey}";

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Add("User-Agent", "IT4You-AI-Debug");

        try
        {
            var response = await httpClient.GetAsync(url);
            var body = await response.Content.ReadAsStringAsync();
            return Ok(new { status = (int)response.StatusCode, response = JsonSerializer.Deserialize<JsonElement>(body) });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }
}

public class GeminiTestRequest
{
    public string ApiKey { get; set; } = string.Empty;
    public string ModelId { get; set; } = "gemini-1.5-flash";
}
