using Microsoft.Azure.Functions.Worker.Http;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

public interface IPortalAuthService
{
    PortalUserContext? GetPortalUser(HttpRequestData request);
    bool CanReadMetadata(PortalUserContext user);
    bool CanSubmitRefresh(PortalUserContext user);
    bool IsAdmin(PortalUserContext user);
}
