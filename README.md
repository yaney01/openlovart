# Infinite Canvas

Docker deployment for the Infinite Canvas image and LMM UI.

## Deploy

1. Copy the env template and fill in your keys:

```bash
cp API/.env.example API/.env
```

2. Build and run:

```bash
docker compose up -d --build
```

3. Open:

```text
http://SERVER_IP:3000/
```

## VPS 部署（不落盘 / 图床模式）

VPS 上默认不在本地保存图片：生成的图上传图床后即删除本地文件，返回图床直链。磁盘只保留配置与结构化数据。

1. 在 `API/.env` 中设置图床（当前支持兰空图床 lsky）：

```env
IMAGE_HOST_STRATEGY=remote
IMAGE_HOST_TYPE=lsky
IMAGE_HOST_BASE_URL=https://你的图床域名
IMAGE_HOST_USERNAME=你的账号邮箱
IMAGE_HOST_PASSWORD=你的密码
# 或直接填 token（二选一）
IMAGE_HOST_TOKEN=
```

2. 所有 API 端点/密钥无内置默认值，必须在 `API/.env` 本地填写（`IMAGE_API_*`、`CHAT_API_*`、`MODELSCOPE_*` 等），留空即为未配置。

3. `docker compose up -d --build` 后开放 `3000` 端口即可。

## Data

镜像不含运行时数据，构建体积很小。挂载持久化的只有：

- `API/.env`: API 密钥与服务器设置
- `data/`: 对话、画布(canvas.json)、资产库与提示词缓存

`output/`、`assets/` 不再挂载——remote 图床模式下图片不落盘，容器内仅作临时缓冲，随容器销毁，不占宿主机磁盘。若把 `IMAGE_HOST_STRATEGY` 改回 `local`，需自行在 compose 中加回这两个卷。

## Host Services

若 LM Studio 或 ComfyUI 与本服务在同一宿主机，`API/.env` 中用 `host.docker.internal`：

```env
CHAT_API_BASE_URL=http://host.docker.internal:1234
COMFYUI_INSTANCES=host.docker.internal:8188
```
