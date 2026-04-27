using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace IT4You.API.Controllers;

[Authorize(Roles = "SUPER_ADMIN")]
[ApiController]
[Route("api/email-templates")]
public class EmailTemplatesController : ControllerBase
{
    private readonly IEmailTemplateService _templates;

    public EmailTemplatesController(IEmailTemplateService templates)
    {
        _templates = templates;
    }

    private string? GetUserId() => User.FindFirst("userId")?.Value;

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        return Ok(await _templates.GetTemplatesAsync());
    }

    [HttpGet("{id}")]
    public async Task<IActionResult> GetById(string id)
    {
        var template = await _templates.GetTemplateAsync(id);
        return template == null ? NotFound() : Ok(template);
    }

    [HttpPost]
    public async Task<IActionResult> Create([FromBody] EmailTemplateRequest request)
    {
        return Ok(await _templates.UpsertTemplateAsync(request, GetUserId()));
    }

    [HttpPut("{id}")]
    public async Task<IActionResult> Update(string id, [FromBody] EmailTemplateRequest request)
    {
        try
        {
            return Ok(await _templates.UpdateTemplateAsync(id, request, GetUserId()));
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }
}

