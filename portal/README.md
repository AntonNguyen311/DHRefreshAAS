# Portal

This folder contains a lightweight static portal for Microsoft Entra SSO and self-service refresh submission.

## What it expects

- A separate web host such as Azure Static Web Apps, App Service, or Blob static website hosting
- Microsoft Entra app registration for the portal UI
- App Service Authentication or equivalent token validation in front of the Function App
- A backend API scope exposed by the Function App host or its Entra app registration

## Configuration

Update `portal/config.js` with:

- portal app `clientId`
- tenant `authority`
- backend Function App base URL
- backend API scope, for example `api://<backend-app-id>/Portal.Access`

## Backend endpoints used

- `GET /api/DHRefreshAAS_PortalModels`
- `GET /api/DHRefreshAAS_PortalTables?databaseName=...`
- `GET /api/DHRefreshAAS_PortalPartitions?databaseName=...&tableName=...`
- `POST /api/DHRefreshAAS_PortalSubmitRefresh`
- `GET /api/DHRefreshAAS_PortalStatus`

## Notes

- Phase 1 allows refreshing existing tables or existing partitions only.
- The portal intentionally does not let users type arbitrary partition names.
- The backend expects authenticated traffic and reads user context from App Service Auth headers after token validation.
