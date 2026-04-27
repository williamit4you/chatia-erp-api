using IT4You.Application.DTOs;
using IT4You.Application.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace IT4You.API.Controllers;

[Authorize(Roles = "SUPER_ADMIN")]
[ApiController]
[Route("api/email-settings")]
public class EmailSettingsController : ControllerBase
{
    private readonly IEmailSenderService _emailSender;

    public EmailSettingsController(IEmailSenderService emailSender)
    {
        _emailSender = emailSender;
    }

    private string? GetUserId() => User.FindFirst("userId")?.Value;

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        return Ok(await _emailSender.GetConfigurationsAsync());
    }

    [HttpPost]
    public async Task<IActionResult> Create([FromBody] EmailConfigurationRequest request)
    {
        return Ok(await _emailSender.SaveConfigurationAsync(request, GetUserId()));
    }

    [HttpPut("{id}")]
    public async Task<IActionResult> Update(string id, [FromBody] EmailConfigurationRequest request)
    {
        return Ok(await _emailSender.SaveConfigurationAsync(request, GetUserId(), id));
    }

    [HttpPost("{id}/test")]
    public async Task<IActionResult> Test(string id, [FromBody] TestEmailRequest request)
    {
        try
        {
            var logId = await _emailSender.SendTestEmailAsync(id, request.ToEmail, GetUserId());
            return Ok(new { success = true, emailLogId = logId });
        }
        catch (Exception ex)
        {
            return BadRequest(new { success = false, message = ex.Message });
        }
    }
}

