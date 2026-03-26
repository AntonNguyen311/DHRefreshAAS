# GitHub Actions — secrets (chuẩn bị)

Workflow [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) chỉ **build + test** trên `ubuntu-latest` và **không cần** repository secret nào để chạy CI cơ bản.

## PAT khi push từ máy (quan trọng)

Để **đẩy file** `.github/workflows/*.yml` lên GitHub, Personal Access Token phải có scope **`workflow`** (cùng với `repo`). Nếu không, Git báo: *refusing to allow a Personal Access Token to create or update workflow without `workflow` scope*.

Cách sửa: GitHub → Settings → Developer settings → Fine-grained hoặc classic token → bật **Workflow** (hoặc dùng `gh auth refresh -h github.com -s workflow` nếu dùng GitHub CLI).

## CI hiện tại (build/test)

| Secret | Cần không? | Ghi chú |
|--------|------------|---------|
| (không) | Không | `dotnet restore` / `build` / `test` dùng NuGet public |

## Khi bạn thêm bước deploy lên Azure (tùy chọn sau)

Tạo **Service Principal** hoặc dùng **Publish Profile**, rồi thêm vào repo: **Settings → Secrets and variables → Actions**.

| Secret | Mục đích |
|--------|----------|
| `AZURE_CREDENTIALS` | JSON từ `az ad sp create-for-rbac` (dùng với `azure/login`) |
| `AZURE_FUNCTIONAPP_PUBLISH_PROFILE` | Nội dung file `.PublishSettings` (deploy bằng MSBuild/zip) |
| `AZURE_SUBSCRIPTION_ID` | Subscription ID (một số template deploy) |
| `AZURE_RESOURCE_GROUP` | Tên resource group |

**Không** đưa `local.settings.json`, storage keys, hay AAD client secret vào workflow dạng plain text — dùng **Application Settings** trên Function App hoặc **Key Vault references** trong Azure.

## Cách thêm secret trên GitHub

1. Repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**
2. Name: ví dụ `AZURE_CREDENTIALS`
3. Value: dán nội dung (một dòng JSON hoặc XML publish profile tùy công cụ)

## Biến môi trường (không nhạy cảm)

Dùng **Settings → Secrets and variables → Actions → Variables** cho giá trị không bí mật (tên app, region) nếu workflow cần.
