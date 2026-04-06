using Microsoft.Azure.Functions.Worker.Http;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

public interface IStatusResponseBuilder
{
    Task<HttpResponseData> GetSpecificOperationStatusAsync(string operationId, HttpRequestData req, PortalUserContext? viewer = null, bool portalView = false);
    Task<HttpResponseData> GetGeneralStatusAsync(HttpRequestData req, PortalUserContext? viewer = null, bool portalView = false);
}
