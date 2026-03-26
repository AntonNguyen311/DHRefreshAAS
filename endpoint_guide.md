# Azure Function Endpoints Guide

## Base URL
```
https://vn-fa-sa-sdp-p-aas-bbckcyaxbnahd5cq.southeastasia-01.azurewebsites.net
```

## 1. 🔍 Status Check (NEW!)
**Endpoint:** `/api/DHRefreshAAS_Status`
**Method:** GET
**Purpose:** Check if function is running and get endpoint information

### Usage:
```bash
curl "https://YOUR-FUNCTION-APP.azurewebsites.net/api/DHRefreshAAS_Status?code=YOUR_FUNCTION_KEY"
```

### Response:
```json
{
  "timestamp": "2025-08-21T05:00:08.097278Z",
  "status": "Function is running",
  "message": "Check Azure Portal logs for detailed operation status",
  "endpoints": {
    "test_token": "/api/DHRefreshAAS_TestToken",
    "test_connection": "/api/DHRefreshAAS_TestConnection",
    "refresh": "/api/DHRefreshAAS_HttpStart",
    "status": "/api/DHRefreshAAS_Status"
  }
}
```

## 2. 🔐 Token Test
**Endpoint:** `/api/DHRefreshAAS_TestToken`
**Method:** GET
**Purpose:** Test Azure AD authentication

## 3. 🔗 Connection Test
**Endpoint:** `/api/DHRefreshAAS_TestConnection`
**Method:** GET  
**Purpose:** Test AAS server connection

## 4. 🔄 Refresh Operation (Main Function)
**Endpoint:** `/api/DHRefreshAAS_HttpStart`
**Method:** POST
**Purpose:** Refresh AAS tables/partitions

### Request Body:
```json
{
  "database_name": "VN_CubeModel",
  "refresh_objects": [
    {"table": "Exchange Rate", "partition": ""},
    {"table": "SalesNAV", "partition": "fSalesNAV_202508"}
  ]
}
```

### Response Types:
- **1-2 tables:** Synchronous (HTTP 200)
- **3+ tables:** Asynchronous (HTTP 202) with operation ID

## 📊 Operation Monitoring

### Your Recent Operations:
- **Operation ID:** `517ba9bc-061d-4f60-953e-2989e8989658` (3 tables)
- **Operation ID:** `b099103d-cfce-45d0-b9bd-93daa5e12133` (6 tables)

### Log Monitoring Options:
1. **Azure Portal:** Function App → Monitor → Logs
2. **Log Stream:** Function App → Monitoring → Log stream  
3. **Kudu Console:** https://vn-fa-sa-sdp-p-aas-bbckcyaxbnahd5cq.scm.azurewebsites.net
