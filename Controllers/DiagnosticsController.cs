using System;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Services;

namespace DHRefreshAAS.Controllers;

/// <summary>
/// Diagnostic endpoints for token and connection testing.
/// </summary>
public class DiagnosticsController
{
    private readonly IConnectionService _connectionService;
    private readonly IResponseService _responseService;
    private readonly IErrorHandlingService _errorHandling;
    private readonly ILogger<DiagnosticsController> _logger;

    public DiagnosticsController(
        IConnectionService connectionService,
        IResponseService responseService,
        IErrorHandlingService errorHandling,
        ILogger<DiagnosticsController> logger)
    {
        _connectionService = connectionService;
        _responseService = responseService;
        _errorHandling = errorHandling;
        _logger = logger;
    }

    [Function("DHRefreshAAS_TestToken")]
    public async Task<HttpResponseData> TestToken(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req,
        FunctionContext context)
    {
        _logger.LogInformation("Azure AD token acquisition test initiated.");

        try
        {
            var testResult = await _connectionService.TestTokenAcquisitionAsync(context.CancellationToken);
            return await _responseService.CreateSuccessResponseAsync(req, testResult);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "token acquisition test");
        }
    }

    [Function("DHRefreshAAS_TestConnection")]
    public async Task<HttpResponseData> TestConnection(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req,
        FunctionContext context)
    {
        _logger.LogInformation("AAS connection test initiated.");

        try
        {
            var testResult = await _connectionService.TestConnectionAsync(context.CancellationToken);
            return await _responseService.CreateSuccessResponseAsync(req, testResult);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "AAS connection test");
        }
    }
}
