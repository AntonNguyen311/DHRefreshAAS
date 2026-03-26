# GitHub Actions — Secrets & Variables

Workflow [`ci.yml`](../.github/workflows/ci.yml) chỉ **build + test**, không cần bất kỳ secret nào.

Workflow [`deploy.yml`](../.github/workflows/deploy.yml) deploy lên Azure Function App `vn-fa-sa-sdp-p-aas` bằng **OIDC (User-assigned managed identity)** — không cần store password hay publish profile.

---

## Variables cho deploy (không phải secret)

Tạo tại: **Settings → Secrets and variables → Actions → Variables**

| Variable | Lấy từ đâu |
|----------|------------|
| `AZURE_CLIENT_ID` | Azure Portal → Managed Identities → chọn identity → Properties → **Client ID** |
| `AZURE_TENANT_ID` | Azure Portal → Microsoft Entra ID → Overview → **Tenant ID** |
| `AZURE_SUBSCRIPTION_ID` | Azure Portal → Subscriptions → **Subscription ID** |

> **Lưu ý**: Nếu dùng Azure Deployment Center → Save, ba giá trị này sẽ được tạo tự động.

---

## PAT khi push workflow file từ máy

Để push `.github/workflows/*.yml` lên GitHub, PAT cần scope **`repo` + `workflow`**.

```
gh auth refresh -h github.com -s workflow
git push origin master
```

Hoặc tạo PAT mới tại: GitHub → Settings → Developer settings → Personal access tokens → bật **workflow**.

---

## Không commit

| File | Lý do |
|------|-------|
| `local.settings.json` | Chứa AAS credentials và storage key — đã có trong `.gitignore` |
| `*.pubxml.user` | Chứa deployment credentials cá nhân |

Dùng **Application Settings** trên Function App hoặc **Key Vault references** để lưu secrets trong môi trường Azure.
