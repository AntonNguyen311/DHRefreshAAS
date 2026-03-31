using System.Net;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Models;
using DHRefreshAAS.Services;

namespace DHRefreshAAS.Controllers;

public class AdfOrchestratorController
{
    private readonly AdfOrchestratorGateService _gateService;
    private readonly ResponseService _responseService;
    private readonly ErrorHandlingService _errorHandling;
    private readonly ILogger<AdfOrchestratorController> _logger;

    public AdfOrchestratorController(
        AdfOrchestratorGateService gateService,
        ResponseService responseService,
        ErrorHandlingService errorHandling,
        ILogger<AdfOrchestratorController> logger)
    {
        ArgumentNullException.ThrowIfNull(gateService);
        ArgumentNullException.ThrowIfNull(responseService);
        ArgumentNullException.ThrowIfNull(errorHandling);
        ArgumentNullException.ThrowIfNull(logger);

        _gateService = gateService;
        _responseService = responseService;
        _errorHandling = errorHandling;
        _logger = logger;
    }

    [Function("DHRefreshAAS_StartElJobIfIdle")]
    public async Task<HttpResponseData> StartElJobIfIdle(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req,
        FunctionContext context)
    {
        _logger.LogInformation("ADF orchestrator gate request received.");

        try
        {
            AdfOrchestratorGateRequest? request = null;
            using (var reader = new StreamReader(req.Body))
            {
                var body = await reader.ReadToEndAsync();
                if (!string.IsNullOrWhiteSpace(body))
                {
                    request = JsonSerializer.Deserialize<AdfOrchestratorGateRequest>(
                        body,
                        new JsonSerializerOptions
                        {
                            PropertyNameCaseInsensitive = true
                        });
                }
            }

            var result = await _gateService.TryStartElJobIfIdleAsync(request, context.CancellationToken);
            var statusCode = result.Started ? HttpStatusCode.Accepted : HttpStatusCode.OK;
            return await _responseService.CreateSuccessResponseAsync(req, result, statusCode);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "ADF orchestrator gate");
        }
    }
}
