import json
import uuid
import base64
import io
import urllib.request
import urllib.parse
import urllib.error
import os
import re
import random
import time
import shutil
import asyncio
import requests
import tempfile
import hashlib
import html
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional
from threading import Lock
import httpx
from PIL import Image, ImageOps
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, UploadFile, File, Header, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def no_store_for_local_ui_assets(request: Request, call_next):
    # Local UI files are edited directly during development.
    # Do not cache them, otherwise the browser can keep running old UI code.
    response = await call_next(request)
    path = request.url.path
    if (
        path == "/static/index.html"
        or path == "/static/api-settings.html"
        or (path.startswith("/static/cowart/") and path.endswith((".html", ".js", ".css")))
    ):
        response.headers["Cache-Control"] = "no-store, must-revalidate"
    return response

# --- WebSocket 状态管理器 ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.user_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, client_id: str = None):
        await websocket.accept()
        self.active_connections.append(websocket)
        if client_id:
            self.user_connections[client_id] = websocket
        print(f"WS Connected. Total: {len(self.active_connections)}")
        await self.broadcast_count()

    async def disconnect(self, websocket: WebSocket, client_id: str = None):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if client_id and client_id in self.user_connections:
            del self.user_connections[client_id]
        print(f"WS Disconnected. Total: {len(self.active_connections)}")
        await self.broadcast_count()

    async def broadcast_count(self):
        count = len(self.active_connections)
        data = json.dumps({"type": "stats", "online_count": count})
        for connection in self.active_connections[:]:
            try:
                await connection.send_text(data)
            except Exception as e:
                print(f"Broadcast error: {e}")
                self.active_connections.remove(connection)

    async def broadcast_new_image(self, image_data: dict):
        data = json.dumps({"type": "new_image", "data": image_data})
        for connection in self.active_connections[:]:
            try:
                await connection.send_text(data)
            except Exception as e:
                print(f"Broadcast image error: {e}")
                self.active_connections.remove(connection)

    async def send_personal_message(self, message: dict, client_id: str):
        ws = self.user_connections.get(client_id)
        if ws:
            try:
                await ws.send_text(json.dumps(message))
            except Exception as e:
                print(f"Personal message error for {client_id}: {e}")

manager = ConnectionManager()
GLOBAL_LOOP = None

@app.on_event("startup")
async def startup_event():
    global GLOBAL_LOOP
    GLOBAL_LOOP = asyncio.get_running_loop()

@app.websocket("/ws/stats")
async def websocket_endpoint(websocket: WebSocket, client_id: str = None):
    await manager.connect(websocket, client_id)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        await manager.disconnect(websocket, client_id)
    except Exception as e:
        print(f"WS Error: {e}")
        await manager.disconnect(websocket, client_id)

# --- 配置区域 ---

CLIENT_ID = str(uuid.uuid4())
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORKFLOW_DIR = os.path.join(BASE_DIR, "workflows")
WORKFLOW_PATH = os.path.join(WORKFLOW_DIR, "Z-Image.json")
STATIC_DIR = os.path.join(BASE_DIR, "static")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
ASSET_LIBRARY_DIR = os.path.join(ASSETS_DIR, "library")
HISTORY_FILE = os.path.join(BASE_DIR, "history.json")
API_ENV_FILE = os.path.join(BASE_DIR, "API", ".env")
DATA_DIR = os.path.join(BASE_DIR, "data")
CONVERSATION_DIR = os.path.join(DATA_DIR, "conversations")
CANVAS_DIR = os.path.join(DATA_DIR, "canvases")
ASSET_LIBRARY_FILE = os.path.join(DATA_DIR, "asset_library.json")
PROMPT_LIBRARY_FILE = os.path.join(DATA_DIR, "prompt_libraries.json")
COWART_DIR = os.path.join(DATA_DIR, "cowart")
COWART_CANVAS_FILE = os.path.join(COWART_DIR, "canvas.json")
COWART_SELECTION_FILE = os.path.join(COWART_DIR, "selection.json")
COWART_VIEW_STATE_FILE = os.path.join(COWART_DIR, "view-state.json")
GLOBAL_CONFIG_FILE = os.path.join(BASE_DIR, "global_config.json")
CANVAS_TRASH_RETENTION_MS = 30 * 24 * 60 * 60 * 1000

QUEUE = []
QUEUE_LOCK = Lock()
HISTORY_LOCK = Lock()
GLOBAL_CONFIG_LOCK = Lock()
CONVERSATION_LOCK = Lock()
CANVAS_LOCK = Lock()
ASSET_LIBRARY_LOCK = Lock()
PROMPT_LIBRARY_LOCK = Lock()
COWART_STATE_LOCK = Lock()
LOAD_LOCK = Lock()
ONLINE_SEED_LOCK = Lock()
COMFY_ORG_AUTH_LOCK = Lock()
NEXT_TASK_ID = 1
ONLINE_USED_SEEDS: Dict[str, set] = {}
COMFY_ORG_AUTH_STATE = {
    "id_token": "",
    "refresh_token": "",
    "expires_at": 0.0,
}

def load_env_file():
    if not os.path.exists(API_ENV_FILE):
        return
    try:
        with open(API_ENV_FILE, 'r', encoding='utf-8-sig') as f:
            for raw_line in f.read().splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)
    except Exception as e:
        print(f"加载 API/.env 失败: {e}")

load_env_file()

def normalize_comfy_url(value: str) -> str:
    value = value.strip().rstrip("/")
    if not value:
        return ""
    parsed = urllib.parse.urlparse(value if "://" in value else f"http://{value}")
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return f"http://{value}"
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"

def comfy_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"

COMFYUI_INSTANCES = [normalize_comfy_url(s) for s in os.getenv("COMFYUI_INSTANCES", "").split(",") if s.strip()]
COMFYUI_ADDRESS = COMFYUI_INSTANCES[0] if COMFYUI_INSTANCES else ""

IMAGE_API_BASE_URL = os.getenv("IMAGE_API_BASE_URL") or os.getenv("COMFLY_BASE_URL", "")
IMAGE_API_BASE_URL = IMAGE_API_BASE_URL.rstrip("/")
IMAGE_API_KEY = os.getenv("IMAGE_API_KEY") or os.getenv("COMFLY_API_KEY", "")
CHAT_API_BASE_URL = os.getenv("CHAT_API_BASE_URL") or os.getenv("COMFLY_BASE_URL", "")
CHAT_API_BASE_URL = CHAT_API_BASE_URL.rstrip("/")
CHAT_API_KEY = os.getenv("CHAT_API_KEY") or os.getenv("COMFLY_API_KEY", "")
AI_BASE_URL = IMAGE_API_BASE_URL
AI_API_KEY = IMAGE_API_KEY
COMFY_ORG_API_KEY = os.getenv("COMFY_ORG_API_KEY") or os.getenv("COMFY_API_KEY") or os.getenv("COMFYUI_API_KEY", "")
COMFY_ORG_AUTH_TOKEN = os.getenv("COMFY_ORG_AUTH_TOKEN", "")
COMFY_ORG_REFRESH_TOKEN = os.getenv("COMFY_ORG_REFRESH_TOKEN", "")
COMFY_ORG_EMAIL = os.getenv("COMFY_ORG_EMAIL", "")
COMFY_ORG_PASSWORD = os.getenv("COMFY_ORG_PASSWORD", "")
COMFY_ORG_FIREBASE_API_KEY = os.getenv("COMFY_ORG_FIREBASE_API_KEY", "")
MODELSCOPE_API_KEY = os.getenv("MODELSCOPE_API_KEY", "")
MODELSCOPE_CHAT_BASE_URL = "https://api-inference.modelscope.cn/v1"
MODELSCOPE_CHAT_MODELS = [m.strip() for m in os.getenv("MODELSCOPE_CHAT_MODELS", "Qwen/Qwen3-235B-A22B,MiniMax/MiniMax-M2.7:MiniMax").split(",") if m.strip()]
CHAT_MODEL = os.getenv("CHAT_API_DEFAULT_MODEL") or os.getenv("CHAT_MODEL", "local-model")
IMAGE_MODEL = os.getenv("IMAGE_API_DEFAULT_MODEL") or os.getenv("IMAGE_MODEL", "gpt-image-2")
SYSTEM_PROMPT = os.getenv("SYSTEM_PROMPT", "You are a helpful assistant.")
MAX_HISTORY_MESSAGES = max(0, int(os.getenv("MAX_HISTORY_MESSAGES", "0")))
AI_REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "120"))
IMAGE_POLL_INTERVAL = float(os.getenv("IMAGE_POLL_INTERVAL", "2"))
IMAGE_HOST_TYPE = os.getenv("IMAGE_HOST_TYPE", "lsky")
IMAGE_HOST_BASE_URL = os.getenv("IMAGE_HOST_BASE_URL", "").rstrip("/")
IMAGE_HOST_USERNAME = os.getenv("IMAGE_HOST_USERNAME", "")
IMAGE_HOST_PASSWORD = os.getenv("IMAGE_HOST_PASSWORD", "")
IMAGE_HOST_TOKEN = os.getenv("IMAGE_HOST_TOKEN", "")
IMAGE_HOST_STRATEGY = os.getenv("IMAGE_HOST_STRATEGY", "local").strip().lower()

def model_list(env_name, primary, defaults):
    configured = os.getenv(env_name, "")
    configured_values = [item.strip() for item in configured.split(",") if item.strip()]
    values = configured_values or [primary, *defaults]
    deduped = []
    for value in values:
        if value and value not in deduped:
            deduped.append(value)
    return deduped

CHAT_MODELS = model_list("CHAT_API_MODELS", CHAT_MODEL, ["gpt-4o-mini"])
IMAGE_MODELS = model_list("IMAGE_API_MODELS", IMAGE_MODEL, ["gpt-image-2"])

def split_csv(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value or "").split(",") if item.strip()]

def chat_endpoint_id(name: str, base_url: str, index: int) -> str:
    seed = f"{name}|{base_url}|{index}"
    return "chat-" + uuid.uuid5(uuid.NAMESPACE_URL, seed).hex[:12]

def normalize_chat_endpoint(item: Dict[str, Any], index: int, existing: Optional[Dict[str, Dict[str, Any]]] = None) -> Optional[Dict[str, Any]]:
    base_url = str(item.get("base_url") or item.get("url") or "").strip().rstrip("/")
    if not base_url:
        return None
    name = str(item.get("name") or f"LMM API {index + 1}").strip() or f"LMM API {index + 1}"
    endpoint_id = str(item.get("id") or chat_endpoint_id(name, base_url, index)).strip()
    models = split_csv(item.get("models") or item.get("model_list"))
    default_model = str(item.get("default_model") or item.get("model") or (models[0] if models else CHAT_MODEL)).strip()
    if default_model and default_model not in models:
        models.insert(0, default_model)
    api_key = str(item.get("api_key") or "").strip()
    if not api_key and existing and endpoint_id in existing:
        api_key = str(existing[endpoint_id].get("api_key") or "")
    return {
        "id": endpoint_id,
        "name": name,
        "base_url": base_url,
        "api_key": api_key,
        "models": models or [default_model or CHAT_MODEL],
        "default_model": default_model or (models[0] if models else CHAT_MODEL),
    }

def parse_chat_api_endpoints() -> List[Dict[str, Any]]:
    configured = os.getenv("CHAT_API_ENDPOINTS", "").strip()
    endpoints: List[Dict[str, Any]] = []
    if configured:
        try:
            parsed = json.loads(configured)
            if isinstance(parsed, list):
                for index, item in enumerate(parsed):
                    if isinstance(item, dict):
                        endpoint = normalize_chat_endpoint(item, index)
                        if endpoint:
                            endpoints.append(endpoint)
        except json.JSONDecodeError:
            pass
    if endpoints:
        return endpoints
    return [{
        "id": "default",
        "name": "默认对话 API",
        "base_url": CHAT_API_BASE_URL,
        "api_key": CHAT_API_KEY,
        "models": CHAT_MODELS,
        "default_model": CHAT_MODEL,
    }]

def public_chat_endpoints() -> List[Dict[str, Any]]:
    return [{
        "id": item["id"],
        "name": item["name"],
        "base_url": item["base_url"],
        "models": item.get("models") or [],
        "default_model": item.get("default_model") or "",
        "has_api_key": bool(item.get("api_key")),
    } for item in CHAT_API_ENDPOINTS]

CHAT_API_ENDPOINTS = parse_chat_api_endpoints()

def env_quote(value):
    text = str(value or "")
    if re.search(r"\s|#", text):
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text

def update_env_values(updates: Dict[str, str]):
    os.makedirs(os.path.dirname(API_ENV_FILE), exist_ok=True)
    existing = []
    seen = set()
    if os.path.exists(API_ENV_FILE):
        with open(API_ENV_FILE, "r", encoding="utf-8-sig") as f:
            existing = f.read().splitlines()
    next_lines = []
    for line in existing:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            next_lines.append(line)
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in updates:
            next_lines.append(f"{key}={env_quote(updates[key])}")
            os.environ[key] = str(updates[key] or "")
            seen.add(key)
        else:
            next_lines.append(line)
    for key, value in updates.items():
        if key not in seen:
            next_lines.append(f"{key}={env_quote(value)}")
            os.environ[key] = str(value or "")
    with open(API_ENV_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(next_lines).rstrip() + "\n")

def reload_runtime_config():
    global COMFYUI_INSTANCES, COMFYUI_ADDRESS, BACKEND_LOCAL_LOAD
    global IMAGE_API_BASE_URL, IMAGE_API_KEY, CHAT_API_BASE_URL, CHAT_API_KEY, AI_BASE_URL, AI_API_KEY
    global COMFY_ORG_API_KEY, COMFY_ORG_AUTH_TOKEN, COMFY_ORG_REFRESH_TOKEN, COMFY_ORG_EMAIL, COMFY_ORG_PASSWORD
    global CHAT_MODEL, IMAGE_MODEL, CHAT_MODELS, IMAGE_MODELS, CHAT_API_ENDPOINTS
    global IMAGE_HOST_BASE_URL, IMAGE_HOST_USERNAME, IMAGE_HOST_PASSWORD, IMAGE_HOST_TOKEN, IMAGE_HOST_STRATEGY
    COMFYUI_INSTANCES = [normalize_comfy_url(s) for s in os.getenv("COMFYUI_INSTANCES", "").split(",") if s.strip()]
    COMFYUI_ADDRESS = COMFYUI_INSTANCES[0] if COMFYUI_INSTANCES else ""
    IMAGE_API_BASE_URL = (os.getenv("IMAGE_API_BASE_URL") or os.getenv("COMFLY_BASE_URL", "")).rstrip("/")
    IMAGE_API_KEY = os.getenv("IMAGE_API_KEY") or os.getenv("COMFLY_API_KEY", "")
    CHAT_API_BASE_URL = (os.getenv("CHAT_API_BASE_URL") or os.getenv("COMFLY_BASE_URL", "")).rstrip("/")
    CHAT_API_KEY = os.getenv("CHAT_API_KEY") or os.getenv("COMFLY_API_KEY", "")
    AI_BASE_URL = IMAGE_API_BASE_URL
    AI_API_KEY = IMAGE_API_KEY
    COMFY_ORG_API_KEY = os.getenv("COMFY_ORG_API_KEY") or os.getenv("COMFY_API_KEY") or os.getenv("COMFYUI_API_KEY", "")
    COMFY_ORG_AUTH_TOKEN = os.getenv("COMFY_ORG_AUTH_TOKEN", "")
    COMFY_ORG_REFRESH_TOKEN = os.getenv("COMFY_ORG_REFRESH_TOKEN", "")
    COMFY_ORG_EMAIL = os.getenv("COMFY_ORG_EMAIL", "")
    COMFY_ORG_PASSWORD = os.getenv("COMFY_ORG_PASSWORD", "")
    CHAT_MODEL = os.getenv("CHAT_API_DEFAULT_MODEL") or os.getenv("CHAT_MODEL", "local-model")
    IMAGE_MODEL = os.getenv("IMAGE_API_DEFAULT_MODEL") or os.getenv("IMAGE_MODEL", "gpt-image-2")
    CHAT_MODELS = model_list("CHAT_API_MODELS", CHAT_MODEL, ["gpt-4o-mini"])
    IMAGE_MODELS = model_list("IMAGE_API_MODELS", IMAGE_MODEL, ["gpt-image-2"])
    CHAT_API_ENDPOINTS = parse_chat_api_endpoints()
    IMAGE_HOST_BASE_URL = os.getenv("IMAGE_HOST_BASE_URL", "").rstrip("/")
    IMAGE_HOST_USERNAME = os.getenv("IMAGE_HOST_USERNAME", "")
    IMAGE_HOST_PASSWORD = os.getenv("IMAGE_HOST_PASSWORD", "")
    IMAGE_HOST_TOKEN = os.getenv("IMAGE_HOST_TOKEN", "")
    IMAGE_HOST_STRATEGY = os.getenv("IMAGE_HOST_STRATEGY", "local").strip().lower()
    BACKEND_LOCAL_LOAD = {addr: 0 for addr in COMFYUI_INSTANCES}

BACKEND_LOCAL_LOAD = {addr: 0 for addr in COMFYUI_INSTANCES}

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(ASSET_LIBRARY_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)
os.makedirs(WORKFLOW_DIR, exist_ok=True)
os.makedirs(CONVERSATION_DIR, exist_ok=True)
os.makedirs(CANVAS_DIR, exist_ok=True)
os.makedirs(COWART_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
app.mount("/output", StaticFiles(directory=OUTPUT_DIR), name="output")
app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="assets")

# --- Pydantic 模型 ---

class GenerateRequest(BaseModel):
    prompt: str = ""
    width: int = 1024
    height: int = 1024
    workflow_json: str = "Z-Image.json"
    params: Dict[str, Any] = {}
    type: str = "zimage"
    model: str = ""
    reference_images: List[str] = []
    client_id: str = ""
    convert_to_jpg: bool = False

class DeleteHistoryRequest(BaseModel):
    timestamp: float
    delete_files: bool = True

class TokenRequest(BaseModel):
    token: str

class SettingsRequest(BaseModel):
    comfyui_instances: str = ""
    comfy_org_email: str = ""
    comfy_org_password: str = ""
    comfy_org_api_key: str = ""
    image_api_base_url: str = ""
    image_api_key: str = ""
    image_api_models: str = ""
    image_api_default_model: str = ""
    chat_api_base_url: str = ""
    chat_api_key: str = ""
    chat_api_models: str = ""
    chat_api_default_model: str = ""
    chat_api_endpoints: List[Dict[str, Any]] = []
    image_host_base_url: str = ""
    image_host_username: str = ""
    image_host_password: str = ""
    image_host_token: str = ""
    image_host_strategy: str = "local"

class AssetCreateRequest(BaseModel):
    url: str
    name: str = ""

class AssetDeleteRequest(BaseModel):
    id: str

class AssetBulkDeleteRequest(BaseModel):
    ids: List[str] = []

class AssetFolderCreateRequest(BaseModel):
    name: str

class AssetMoveRequest(BaseModel):
    ids: List[str] = []
    folder_id: str = ""

class AssetToComfyRequest(BaseModel):
    url: str
    name: str = ""

class AssetDuplicateRequest(BaseModel):
    id: str
    folder_id: str = ""

class AssetRenameRequest(BaseModel):
    id: str
    name: str = ""

class AssetEnsureRequest(BaseModel):
    url: str
    name: str = ""

class AssetFolderRenameRequest(BaseModel):
    name: str = ""

class PromptDexterSyncRequest(BaseModel):
    limit: int = 0
    fetch_details: int = 12
    force: bool = False
    category_id: str = ""

class PromptDexterPromptUpdateRequest(BaseModel):
    prompt: str = ""

class PromptPresetCreateRequest(BaseModel):
    source_item_id: str = ""
    title: str = "我的提示词"
    description: str = ""
    prompt: str = ""
    categories: List[str] = []
    tags: List[str] = []
    image_url: str = ""

class PromptPresetUpdateRequest(BaseModel):
    title: str = ""
    description: str = ""
    prompt: str = ""
    categories: List[str] = []
    tags: List[str] = []
    image_url: str = ""

class CloudGenRequest(BaseModel):
    prompt: str
    api_key: str = ""
    resolution: str = "1024*1024"
    type: str = "zimage"
    image_urls: List[str] = []
    client_id: Optional[str] = None

class CloudPollRequest(BaseModel):
    task_id: str
    api_key: str = ""
    client_id: Optional[str] = None

class AIReference(BaseModel):
    url: str = ""
    name: str = ""

class OnlineImageRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=4000)
    model: str = ""
    provider: str = "gpt"
    size: str = "1024x1024"
    quality: str = "auto"
    aspect_ratio: str = "auto"
    resolution: str = "1K"
    thinking_level: str = "MINIMAL"
    reference_images: List[AIReference] = []

class ComfyLoginRequest(BaseModel):
    email: str = ""
    password: str = ""
    api_key: str = ""

class ChatRequest(BaseModel):
    conversation_id: str = ""
    message: str = Field(min_length=1, max_length=20000)
    model: str = ""
    image_model: str = ""
    mode: str = "chat"
    size: str = "1024x1024"
    quality: str = "auto"
    reference_images: List[AIReference] = []
    provider: str = "comfly"
    ms_model: str = ""
    chat_provider: str = ""
    reasoning_enabled: bool = False
    reasoning_effort: str = ""

class MsGenerateRequest(BaseModel):
    prompt: str
    model: str = "black-forest-labs/FLUX.2-klein-9B"
    image_urls: List[str] = []
    width: int = 0
    height: int = 0
    loras: Optional[Any] = None
    client_id: Optional[str] = None

class CanvasLLMRequest(BaseModel):
    message: str = Field(min_length=1, max_length=20000)
    system_prompt: str = "You are a helpful assistant."
    model: str = ""
    messages: List[Dict[str, str]] = []
    provider: str = "comfly"
    ms_model: str = ""

class ConversationCreateRequest(BaseModel):
    title: str = "新对话"

class CanvasCreateRequest(BaseModel):
    title: str = "未命名画布"
    icon: str = "🧩"

class CanvasSaveRequest(BaseModel):
    title: str = "未命名画布"
    icon: str = "🧩"
    nodes: List[Dict[str, Any]] = []
    connections: List[Dict[str, Any]] = []
    viewport: Dict[str, Any] = {}

# --- 负载均衡 ---

def check_images_exist(backend_addr, images):
    if not images: return True
    for img in images:
        try:
            url = comfy_url(backend_addr, f"/view?filename={urllib.parse.quote(img)}&type=input")
            r = requests.get(url, stream=True, timeout=0.5)
            r.close()
            if r.status_code != 200: return False
        except: return False
    return True

def get_best_backend(required_images: List[str] = None):
    best_backend = COMFYUI_INSTANCES[0]
    min_queue_size = float('inf')
    candidates_with_images = []
    candidates_others = []
    backend_stats = {}

    for addr in COMFYUI_INSTANCES:
        try:
            with urllib.request.urlopen(comfy_url(addr, "/queue"), timeout=1) as response:
                data = json.loads(response.read())
                remote_load = len(data.get('queue_running', [])) + len(data.get('queue_pending', []))
                with LOAD_LOCK:
                    local_load = BACKEND_LOCAL_LOAD.get(addr, 0)
                effective_load = max(remote_load, local_load)
                has_images = check_images_exist(addr, required_images)
                backend_stats[addr] = {"load": effective_load, "has_images": has_images}
                if has_images:
                    candidates_with_images.append(addr)
                else:
                    candidates_others.append(addr)
        except Exception as e:
            print(f"Backend {addr} unreachable: {e}")
            continue

    target_candidates = candidates_with_images if candidates_with_images else candidates_others
    if not target_candidates:
        if candidates_others:
            target_candidates = candidates_others
        else:
            return COMFYUI_INSTANCES[0]

    for addr in target_candidates:
        load = backend_stats[addr]["load"]
        if load < min_queue_size:
            min_queue_size = load
            best_backend = addr

    return best_backend

# --- 辅助工具 ---

def offload_local_file(path, fallback_url):
    """remote 图床模式：上传成功后删除本地文件并返回远程直链；否则返回本地 URL。"""
    if IMAGE_HOST_STRATEGY != "remote":
        return fallback_url
    try:
        remote = upload_to_lsky_sync(path)
    except Exception as e:
        print(f"图床上传失败，保留本地文件: {e}")
        return fallback_url
    if remote:
        try:
            os.remove(path)
        except OSError:
            pass
        return remote
    return fallback_url

async def offload_local_file_async(path, fallback_url):
    """remote 图床模式（异步）：上传成功后删除本地文件并返回远程直链。"""
    if IMAGE_HOST_STRATEGY != "remote":
        return fallback_url
    try:
        remote = await upload_to_lsky(path)
    except Exception as e:
        print(f"图床上传失败，保留本地文件: {e}")
        return fallback_url
    if remote:
        try:
            os.remove(path)
        except OSError:
            pass
        return remote
    return fallback_url

def download_image(comfy_address, comfy_url_path, prefix="studio_"):
    filename = f"{prefix}{uuid.uuid4().hex[:10]}.png"
    local_path = os.path.join(OUTPUT_DIR, filename)
    full_url = comfy_url(comfy_address, comfy_url_path)
    try:
        with urllib.request.urlopen(full_url) as response, open(local_path, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
        return offload_local_file(local_path, f"/output/{filename}")
    except Exception as e:
        print(f"下载图片失败: {e}")
        if comfy_url_path.startswith("/view"):
            return comfy_url_path.replace("/view", "/api/view", 1)
        return full_url

def save_to_history(record):
    for url in record.get("images", []) or []:
        if not isinstance(url, str):
            continue
        if url.startswith("/output/") or url.startswith("/assets/"):
            try:
                sync_generated_asset(url, record.get("prompt") or os.path.basename(url))
            except Exception as e:
                print(f"自动写入资产库失败: {e}")
        elif url.startswith("http://") or url.startswith("https://"):
            try:
                add_asset_record(url, record.get("prompt") or os.path.basename(url), remote_url=url, source_url=url)
            except Exception as e:
                print(f"自动写入资产库失败: {e}")
    with HISTORY_LOCK:
        history = []
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            except: pass
        if "timestamp" not in record:
            record["timestamp"] = time.time()
        history.insert(0, record)
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history[:5000], f, ensure_ascii=False, indent=4)

def replace_history_record(timestamp, record):
    with HISTORY_LOCK:
        if not os.path.exists(HISTORY_FILE):
            return
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except:
            return
        for index, item in enumerate(history):
            if item.get("timestamp") == timestamp:
                history[index] = record
                break
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history[:5000], f, ensure_ascii=False, indent=4)

def get_comfy_history(comfy_address, prompt_id):
    try:
        with urllib.request.urlopen(comfy_url(comfy_address, f"/history/{prompt_id}")) as response:
            return json.loads(response.read())
    except Exception as e:
        return {}

def safe_user_id(user_id, request: Request):
    candidate = (user_id or "").strip()
    if not candidate and request.client:
        candidate = f"ip-{request.client.host}"
    if not candidate:
        candidate = "anonymous"
    candidate = re.sub(r"[^a-zA-Z0-9_.-]", "-", candidate)[:80].strip(".-")
    return candidate or "anonymous"

def user_dir(user_id):
    path = os.path.join(CONVERSATION_DIR, user_id)
    os.makedirs(path, exist_ok=True)
    return path

def conversation_path(user_id, conversation_id):
    cleaned = re.sub(r"[^a-zA-Z0-9_-]", "", conversation_id or "")
    if not cleaned:
        raise HTTPException(status_code=400, detail="无效的对话 ID")
    return os.path.join(user_dir(user_id), f"{cleaned}.json")

def now_ms():
    return int(time.time() * 1000)

def save_conversation(user_id, conversation):
    with CONVERSATION_LOCK:
        path = conversation_path(user_id, conversation["id"])
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(conversation, f, ensure_ascii=False, indent=2)

def new_conversation(user_id, title="新对话"):
    timestamp = now_ms()
    conversation = {
        "id": uuid.uuid4().hex,
        "title": (title or "新对话")[:80],
        "created_at": timestamp,
        "updated_at": timestamp,
        "messages": [],
    }
    save_conversation(user_id, conversation)
    return conversation

def load_conversation(user_id, conversation_id):
    path = conversation_path(user_id, conversation_id)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="对话不存在")
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def list_conversations(user_id):
    records = []
    for filename in os.listdir(user_dir(user_id)):
        if not filename.endswith(".json"):
            continue
        path = os.path.join(user_dir(user_id), filename)
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            continue
        messages = data.get("messages", [])
        last_message = next((m for m in reversed(messages) if m.get("role") != "system"), None)
        records.append({
            "id": data.get("id"),
            "title": data.get("title", "新对话"),
            "created_at": data.get("created_at", 0),
            "updated_at": data.get("updated_at", 0),
            "last_message": (last_message or {}).get("content", ""),
        })
    return sorted(records, key=lambda item: item["updated_at"], reverse=True)

def canvas_path(canvas_id):
    cleaned = re.sub(r"[^a-zA-Z0-9_-]", "", canvas_id or "")
    if not cleaned:
        raise HTTPException(status_code=400, detail="无效的画布 ID")
    return os.path.join(CANVAS_DIR, f"{cleaned}.json")

def save_canvas(canvas):
    canvas["updated_at"] = now_ms()
    with CANVAS_LOCK:
        with open(canvas_path(canvas["id"]), 'w', encoding='utf-8') as f:
            json.dump(canvas, f, ensure_ascii=False, indent=2)

def new_canvas(title="未命名画布", icon="layers"):
    timestamp = now_ms()
    canvas = {
        "id": uuid.uuid4().hex,
        "title": (title or "未命名画布")[:80],
        "icon": (icon or "🧩")[:4],
        "created_at": timestamp,
        "updated_at": timestamp,
        "nodes": [],
        "connections": [],
        "viewport": {"x": 0, "y": 0, "scale": 1},
    }
    save_canvas(canvas)
    return canvas

def load_canvas(canvas_id):
    path = canvas_path(canvas_id)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="画布不存在")
    with open(path, 'r', encoding='utf-8') as f:
        canvas = json.load(f)
    if canvas.get("deleted_at"):
        raise HTTPException(status_code=404, detail="画布已在回收站")
    return canvas

def load_canvas_any(canvas_id):
    path = canvas_path(canvas_id)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="画布不存在")
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def canvas_record(data):
    return {
        "id": data.get("id"),
        "title": data.get("title", "未命名画布"),
        "icon": data.get("icon", "🧩"),
        "created_at": data.get("created_at", 0),
        "updated_at": data.get("updated_at", 0),
        "deleted_at": data.get("deleted_at", 0),
        "node_count": len(data.get("nodes", [])),
    }

def cleanup_expired_canvas_trash():
    cutoff = now_ms() - CANVAS_TRASH_RETENTION_MS
    with CANVAS_LOCK:
        for filename in os.listdir(CANVAS_DIR):
            if not filename.endswith(".json"):
                continue
            path = os.path.join(CANVAS_DIR, filename)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                deleted_at = int(data.get("deleted_at") or 0)
                if deleted_at and deleted_at < cutoff:
                    os.remove(path)
            except Exception:
                continue

def iter_canvas_records(include_deleted=False):
    cleanup_expired_canvas_trash()
    records = []
    for filename in os.listdir(CANVAS_DIR):
        if not filename.endswith(".json"):
            continue
        try:
            with open(os.path.join(CANVAS_DIR, filename), 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            continue
        is_deleted = bool(data.get("deleted_at"))
        if include_deleted != is_deleted:
            continue
        records.append(canvas_record(data))
    return records

def list_canvases():
    records = iter_canvas_records(include_deleted=False)
    return sorted(records, key=lambda item: item["updated_at"], reverse=True)

def list_deleted_canvases():
    records = iter_canvas_records(include_deleted=True)
    return sorted(records, key=lambda item: item["deleted_at"], reverse=True)

def display_title(text):
    title = re.sub(r"\s+", " ", text or "").strip()
    return title[:24] or "新对话"

def limited_chat_history(messages):
    if MAX_HISTORY_MESSAGES <= 0:
        return []
    return messages[-MAX_HISTORY_MESSAGES:]

def chat_api_headers_for_key(api_key: str, json_body=True):
    headers = {"Accept": "application/json"}
    if json_body:
        headers["Content-Type"] = "application/json"
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers

def chat_stream_headers(headers: Dict[str, str]) -> Dict[str, str]:
    stream_headers = dict(headers)
    stream_headers["Accept"] = "text/event-stream"
    return stream_headers

def resolve_chat_provider(provider: str, model: str, ms_model: str, chat_provider: str = ""):
    if provider == "modelscope":
        if not MODELSCOPE_API_KEY:
            raise HTTPException(status_code=400, detail="未配置 MODELSCOPE_API_KEY，请在 API/.env 中填写。")
        base = MODELSCOPE_CHAT_BASE_URL
        hdrs = {"Authorization": f"Bearer {MODELSCOPE_API_KEY}", "Content-Type": "application/json"}
        mdl = selected_model(ms_model or model, MODELSCOPE_CHAT_MODELS[0] if MODELSCOPE_CHAT_MODELS else "MiniMax/MiniMax-M2.7")
        return base, hdrs, mdl
    endpoint = None
    if chat_provider:
        endpoint = next((item for item in CHAT_API_ENDPOINTS if item.get("id") == chat_provider), None)
    endpoint = endpoint or (CHAT_API_ENDPOINTS[0] if CHAT_API_ENDPOINTS else None)
    base = (endpoint.get("base_url") if endpoint else CHAT_API_BASE_URL).rstrip("/")
    if not base.endswith("/v1"):
        base = base + "/v1"
    hdrs = chat_api_headers_for_key(endpoint.get("api_key", "") if endpoint else CHAT_API_KEY)
    fallback_model = (endpoint.get("default_model") if endpoint else CHAT_MODEL) or CHAT_MODEL
    mdl = selected_model(model, fallback_model)
    return base, hdrs, mdl

def bearer_headers(api_key, label, json_body=True):
    if not api_key:
        raise HTTPException(status_code=400, detail=f"未配置 {label}，请在 API 设置中填写。")
    headers = {"Accept": "application/json", "Authorization": f"Bearer {api_key}"}
    if json_body:
        headers["Content-Type"] = "application/json"
    return headers

def api_headers(json_body=True):
    return bearer_headers(IMAGE_API_KEY, "出图 API Key", json_body)

def image_api_url(path: str) -> str:
    base = IMAGE_API_BASE_URL.rstrip("/")
    if not base.lower().endswith("/v1"):
        base += "/v1"
    return f"{base}/{path.lstrip('/')}"

def chat_api_headers(json_body=True):
    if not CHAT_API_KEY:
        headers = {"Accept": "application/json"}
        if json_body:
            headers["Content-Type"] = "application/json"
        return headers
    return bearer_headers(CHAT_API_KEY, "对话 API Key", json_body)

def selected_model(requested, fallback):
    model = (requested or fallback).strip()
    if not model:
        raise HTTPException(status_code=400, detail="模型名称不能为空")
    if len(model) > 120 or not re.fullmatch(r"[a-zA-Z0-9_.:/+-]+", model):
        raise HTTPException(status_code=400, detail=f"模型名称不合法：{model}")
    return model

def parse_size_value(value):
    match = re.fullmatch(r"\s*(\d+)\s*[xX*×]\s*(\d+)\s*", str(value or ""))
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))

def random_online_seed(provider, max_value):
    key = provider or "default"
    with ONLINE_SEED_LOCK:
        used = ONLINE_USED_SEEDS.setdefault(key, set())
        if len(used) > max_value:
            raise HTTPException(status_code=500, detail="可用随机 seed 已耗尽")
        seed = random.SystemRandom().randint(0, max_value)
        while seed in used:
            seed = random.SystemRandom().randint(0, max_value)
        used.add(seed)
        return seed

def comfy_auth_expired():
    return not COMFY_ORG_AUTH_STATE.get("id_token") or time.time() >= float(COMFY_ORG_AUTH_STATE.get("expires_at") or 0)

def update_comfy_auth_state(id_token="", refresh_token="", expires_in=3600):
    COMFY_ORG_AUTH_STATE["id_token"] = id_token or ""
    if refresh_token:
        COMFY_ORG_AUTH_STATE["refresh_token"] = refresh_token
    COMFY_ORG_AUTH_STATE["expires_at"] = time.time() + max(60, int(expires_in or 3600)) - 60

def comfy_auth_error_message(response):
    try:
        payload = response.json()
        message = payload.get("error", {}).get("message") or payload.get("message")
        if message:
            return str(message)
    except Exception:
        pass
    return f"{response.status_code} {response.reason}"

def refresh_comfy_org_token(refresh_token):
    response = requests.post(
        f"https://securetoken.googleapis.com/v1/token?key={COMFY_ORG_FIREBASE_API_KEY}",
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        timeout=15,
    )
    if response.status_code != 200:
        raise Exception(f"ComfyUI 登录刷新失败：{comfy_auth_error_message(response)}")
    data = response.json()
    update_comfy_auth_state(
        id_token=data.get("id_token", ""),
        refresh_token=data.get("refresh_token", refresh_token),
        expires_in=data.get("expires_in", 3600),
    )
    return COMFY_ORG_AUTH_STATE["id_token"]

def login_comfy_org(email, password):
    response = requests.post(
        f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={COMFY_ORG_FIREBASE_API_KEY}",
        json={"email": email, "password": password, "returnSecureToken": True},
        timeout=15,
    )
    if response.status_code != 200:
        raise Exception(f"ComfyUI 登录失败：{comfy_auth_error_message(response)}")
    data = response.json()
    update_comfy_auth_state(
        id_token=data.get("idToken", ""),
        refresh_token=data.get("refreshToken", ""),
        expires_in=data.get("expiresIn", 3600),
    )
    return COMFY_ORG_AUTH_STATE["id_token"]

def get_comfy_org_auth_token():
    if COMFY_ORG_AUTH_TOKEN:
        return COMFY_ORG_AUTH_TOKEN
    with COMFY_ORG_AUTH_LOCK:
        if not comfy_auth_expired():
            return COMFY_ORG_AUTH_STATE["id_token"]
        refresh_token = COMFY_ORG_AUTH_STATE.get("refresh_token") or COMFY_ORG_REFRESH_TOKEN
        if refresh_token:
            return refresh_comfy_org_token(refresh_token)
        if COMFY_ORG_EMAIL and COMFY_ORG_PASSWORD:
            return login_comfy_org(COMFY_ORG_EMAIL, COMFY_ORG_PASSWORD)
    return ""

def comfy_prompt_extra_data():
    extra = {}
    if COMFY_ORG_API_KEY:
        extra["api_key_comfy_org"] = COMFY_ORG_API_KEY
        return extra
    auth_token = get_comfy_org_auth_token()
    if auth_token:
        extra["auth_token_comfy_org"] = auth_token
    return extra

def text_from_chat_response(data):
    choices = data.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    content = message.get("content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                parts.append(item.get("text") or item.get("content") or "")
        return "\n".join(part for part in parts if part)
    return str(content)

def reasoning_from_chat_response(data):
    choices = data.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    return text_from_value(message.get("reasoning") or message.get("reasoning_content") or "")

def text_from_value(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                parts.append(item.get("text") or item.get("content") or "")
        return "".join(parts)
    return str(content) if content else ""

def text_delta_from_chat_chunk(data):
    choices = data.get("choices") or []
    if not choices:
        return ""
    delta = choices[0].get("delta") or {}
    content = delta.get("content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                parts.append(item.get("text") or item.get("content") or "")
        return "".join(parts)
    return str(content) if content else ""

def reasoning_delta_from_chat_chunk(data):
    choices = data.get("choices") or []
    if not choices:
        return ""
    delta = choices[0].get("delta") or {}
    return text_from_value(delta.get("reasoning") or delta.get("reasoning_content") or "")

def split_reasoning_tags(text: str):
    value = text or ""
    patterns = [
        r"<think>\s*(.*?)\s*</think>",
        r"<\|channel\>thought\s*(.*?)\s*<channel\|>",
    ]
    reasoning_parts = []
    for pattern in patterns:
        matches = re.findall(pattern, value, flags=re.DOTALL | re.IGNORECASE)
        if matches:
            reasoning_parts.extend(part.strip() for part in matches if part.strip())
            value = re.sub(pattern, "", value, flags=re.DOTALL | re.IGNORECASE).strip()
    return "\n\n".join(reasoning_parts).strip(), value.strip()

def sse_event(data):
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

def laplacian_variance(img_bytes):
    """清晰度评分：拉普拉斯方差，越大越清晰。失败返回 -1.0。"""
    try:
        try:
            import numpy as np
        except ImportError:
            print("[清晰度筛选] 未安装 numpy，筛选已降级为取第一张。请 pip install numpy 后重启。")
            return -1.0
        im = Image.open(io.BytesIO(img_bytes)).convert("L")
        w, h = im.size
        longest = max(w, h)
        if longest > 1024:
            scale = 1024.0 / longest
            im = im.resize((max(1, int(w * scale)), max(1, int(h * scale))))
        a = np.asarray(im, dtype=np.float64)
        if a.ndim != 2 or a.shape[0] < 3 or a.shape[1] < 3:
            return -1.0
        lap = (-4 * a[1:-1, 1:-1] + a[:-2, 1:-1] + a[2:, 1:-1]
               + a[1:-1, :-2] + a[1:-1, 2:])
        return float(lap.var())
    except Exception as exc:
        print(f"清晰度评分失败: {exc}")
        return -1.0

def _bytes_from_descriptor(desc):
    try:
        if desc.get("type") == "b64":
            return base64.b64decode(desc["value"])
        value = desc.get("value") or ""
        if value.startswith("data:") and ";base64," in value:
            return base64.b64decode(value.split(";base64,", 1)[1])
        resp = requests.get(value, timeout=20)
        resp.raise_for_status()
        return resp.content
    except Exception:
        return None

def pick_sharpest_descriptor(descriptors):
    """多张候选图时只保留最清晰的一张（在线出图接口路径）。"""
    if len(descriptors) <= 1:
        print(f"[清晰度筛选] 接口只返回 {len(descriptors)} 张图，无可筛选。")
        return descriptors[0]
    best, best_idx, best_score = descriptors[0], 0, -2.0
    scores = []
    for idx, desc in enumerate(descriptors):
        data = _bytes_from_descriptor(desc)
        score = laplacian_variance(data) if data else -1.0
        scores.append(round(score, 1))
        if score > best_score:
            best_score, best, best_idx = score, desc, idx
    print(f"[清晰度筛选] 在线路径共 {len(descriptors)} 张候选，分数={scores}，"
          f"选中第 {best_idx + 1} 张（分数最高 {round(best_score, 1)}）。")
    return best

def _bytes_from_local_ref(ref):
    try:
        path = output_file_from_url(ref)
        if path and os.path.exists(path):
            with open(path, "rb") as f:
                return f.read()
        if os.path.exists(ref):
            with open(ref, "rb") as f:
                return f.read()
        if ref.startswith(("http://", "https://")):
            resp = requests.get(ref, timeout=20)
            resp.raise_for_status()
            return resp.content
    except Exception:
        return None
    return None

def pick_sharpest_ref(refs):
    """多张输出图（本地/远程 URL）时只保留最清晰的一张（ComfyUI 路径）。"""
    refs = [r for r in (refs or []) if r]
    if len(refs) <= 1:
        return refs[0] if refs else None
    best, best_idx, best_score = refs[0], 0, -2.0
    scores = []
    for idx, ref in enumerate(refs):
        data = _bytes_from_local_ref(ref)
        score = laplacian_variance(data) if data else -1.0
        scores.append(round(score, 1))
        if score > best_score:
            best_score, best, best_idx = score, ref, idx
    print(f"[清晰度筛选] ComfyUI 路径共 {len(refs)} 张候选，分数={scores}，"
          f"选中第 {best_idx + 1} 张（分数最高 {round(best_score, 1)}）。")
    return best

def extract_image(data):
    if isinstance(data.get("data"), dict) and isinstance(data["data"].get("data"), dict):
        data = data["data"]["data"]
    images = data.get("data") or []
    print(f"[清晰度筛选] 在线出图接口本次返回 {len(images)} 张图。")
    if not images:
        raise HTTPException(status_code=502, detail="生图接口没有返回图片数据")
    descriptors = []
    for item in images:
        if not isinstance(item, dict):
            continue
        if item.get("url"):
            descriptors.append({"type": "url", "value": item["url"]})
        elif item.get("b64_json"):
            descriptors.append({"type": "b64", "value": item["b64_json"]})
    if not descriptors:
        raise HTTPException(status_code=502, detail="无法识别生图接口返回格式")
    # 接口一次返回多张时（常见一张清晰一张糊），只取最清晰的一张。
    return pick_sharpest_descriptor(descriptors)

def extract_task_id(data):
    if data.get("task_id"):
        return str(data["task_id"])
    if data.get("id") and str(data.get("id", "")).startswith("task"):
        return str(data["id"])
    nested = data.get("data")
    if isinstance(nested, dict):
        return extract_task_id(nested)
    return None

async def wait_for_image_task(client, task_id):
    deadline = time.monotonic() + AI_REQUEST_TIMEOUT
    last_payload = {}
    while time.monotonic() < deadline:
        response = await client.get(image_api_url(f"images/tasks/{task_id}"), headers=api_headers())
        response.raise_for_status()
        last_payload = response.json()
        task_data = last_payload.get("data") if isinstance(last_payload.get("data"), dict) else last_payload
        status = str(task_data.get("status", "")).upper()
        if status == "SUCCESS":
            return last_payload
        if status == "FAILURE":
            reason = task_data.get("fail_reason") or last_payload.get("message") or "生图任务失败"
            raise HTTPException(status_code=502, detail=f"生图任务失败：{reason}")
        await asyncio.sleep(IMAGE_POLL_INTERVAL)
    raise HTTPException(status_code=504, detail=f"生图任务超时，task_id={task_id}")

def output_file_from_url(url):
    if not url or not (url.startswith("/output/") or url.startswith("/assets/")):
        return None
    clean = urllib.parse.unquote(url.split("?", 1)[0])
    root = ASSETS_DIR if clean.startswith("/assets/") else OUTPUT_DIR
    rel = clean[len("/assets/"):] if clean.startswith("/assets/") else clean[len("/output/"):]
    path = os.path.abspath(os.path.join(root, rel))
    safe_root = os.path.abspath(root)
    if os.path.commonpath([safe_root, path]) != safe_root or not os.path.exists(path):
        return None
    return path

def content_type_for_path(path):
    ext = os.path.splitext(path)[1].lower()
    if ext in [".jpg", ".jpeg"]:
        return "image/jpeg"
    if ext == ".webp":
        return "image/webp"
    return "image/png"

def normalize_duck2api_edit_image(content):
    try:
        with Image.open(io.BytesIO(content)) as source:
            image = ImageOps.exif_transpose(source)
            if image.mode in ("RGBA", "LA") or (image.mode == "P" and "transparency" in image.info):
                image = image.convert("RGBA")
            else:
                image = image.convert("RGB")
            image.thumbnail((1024, 1024), Image.LANCZOS)
            output = io.BytesIO()
            image.save(output, "WEBP", quality=90, method=4)
            return output.getvalue()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="参考图片无法转换为 Duck2api 支持的 WebP 格式") from exc

def convert_output_to_jpg(url, quality=88):
    path = output_file_from_url(url)
    if not path:
        return url
    root, ext = os.path.splitext(path)
    if ext.lower() in [".jpg", ".jpeg"]:
        return url
    jpg_path = f"{root}.jpg"
    try:
        with Image.open(path) as img:
            if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                bg = Image.new("RGB", img.size, (255, 255, 255))
                bg.paste(img.convert("RGBA"), mask=img.convert("RGBA").split()[-1])
                img = bg
            else:
                img = img.convert("RGB")
            img.save(jpg_path, "JPEG", quality=quality, optimize=True)
        return f"/output/{os.path.basename(jpg_path)}"
    except Exception as e:
        print(f"转换 JPG 失败: {e}")
        return url

def reference_to_data_url(ref):
    url = (ref.get("url", "") or "").strip()
    if url.startswith("data:"):
        if ";base64," not in url:
            raise HTTPException(status_code=400, detail="图片数据不是有效的 base64 data URI")
        return url
    path = output_file_from_url(url)
    if not path:
        if not url.startswith(("http://", "https://")):
            return ""
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise HTTPException(status_code=400, detail=f"无法读取参考图片：{exc}") from exc
        content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if not content_type.startswith("image/"):
            content_type = "image/png"
        encoded = base64.b64encode(response.content).decode("ascii")
        return f"data:{content_type};base64,{encoded}"
    with open(path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("ascii")
    return f"data:{content_type_for_path(path)};base64,{encoded}"

def thinking_template_model(model: str) -> bool:
    value = (model or "").lower()
    return any(name in value for name in ("gemma", "qwen", "deepseek", "gpt-oss"))

def chat_completion_payload(model: str, messages: List[Dict[str, Any]], reasoning_enabled: bool = False, reasoning_effort: str = "", stream: bool = False) -> Dict[str, Any]:
    payload = {"model": model, "messages": messages}
    if stream:
        payload["stream"] = True
    effort = (reasoning_effort or "medium").strip().lower()
    if reasoning_enabled and effort in {"low", "medium", "high"}:
        payload["reasoning"] = {"effort": effort}
    if thinking_template_model(model):
        payload["chat_template_kwargs"] = {
            "enable_thinking": bool(reasoning_enabled),
            "enableThinking": bool(reasoning_enabled),
        }
    return payload

def modelscope_edit_image_payload(image_inputs: List[str]) -> Dict[str, List[str]]:
    base64_images = []
    url_images = []

    for item in image_inputs or []:
        value = (item or "").strip()
        if not value:
            continue
        if value.startswith("data:"):
            if ";base64," not in value:
                raise HTTPException(status_code=400, detail="ModelScope 图片数据不是有效的 base64 data URI")
            base64_images.append(value.split(",", 1)[1].strip())
        elif value.startswith(("http://", "https://")):
            url_images.append(value)
        else:
            base64_images.append(value)

    if base64_images and url_images:
        raise HTTPException(status_code=400, detail="ModelScope 角度编辑不支持混用 base64 和 URL 输入图")
    if base64_images:
        return {"images": base64_images}
    if url_images:
        return {"image_url": url_images}
    raise HTTPException(status_code=400, detail="角度编辑需要上传输入图片")

async def save_ai_image_to_output(image_data, prefix="online_"):
    filename = f"{prefix}{uuid.uuid4().hex[:10]}.png"
    path = os.path.join(OUTPUT_DIR, filename)
    if image_data["type"] == "b64":
        with open(path, "wb") as f:
            f.write(base64.b64decode(image_data["value"]))
        return await offload_local_file_async(path, f"/output/{filename}")
    value = image_data["value"]
    if value.startswith("/output/"):
        return value
    try:
        async with httpx.AsyncClient(timeout=AI_REQUEST_TIMEOUT) as client:
            response = await client.get(value)
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "")
            if "jpeg" in content_type or "jpg" in content_type:
                filename = filename[:-4] + ".jpg"
                path = os.path.join(OUTPUT_DIR, filename)
            elif "webp" in content_type:
                filename = filename[:-4] + ".webp"
                path = os.path.join(OUTPUT_DIR, filename)
            with open(path, "wb") as f:
                f.write(response.content)
            return await offload_local_file_async(path, f"/output/{filename}")
    except Exception as e:
        print(f"保存上游图片失败: {e}")
        return value

async def generate_ai_image(prompt, size, quality, model, reference_images=None):
    refs = [ref for ref in (reference_images or []) if ref.get("url")]
    async with httpx.AsyncClient(timeout=AI_REQUEST_TIMEOUT) as client:
        if refs:
            files = []
            for index, ref in enumerate(refs[:4], start=1):
                data_url = reference_to_data_url(ref)
                match = re.fullmatch(r"data:(image/[a-zA-Z0-9.+-]+);base64,(.+)", data_url, flags=re.DOTALL)
                if match:
                    try:
                        content = base64.b64decode(match.group(2), validate=True)
                    except (ValueError, base64.binascii.Error) as exc:
                        raise HTTPException(status_code=400, detail="参考图片不是有效的 base64 数据") from exc
                    content = normalize_duck2api_edit_image(content)
                    files.append(("image", (f"reference-{index}.webp", content, "image/webp")))
            if not files:
                raise HTTPException(status_code=400, detail="无法读取参考图片")
            data = {"model": model, "prompt": prompt, "size": size, "quality": quality, "response_format": "url", "n": "1"}
            response = await client.post(image_api_url("images/edits"), headers=api_headers(json_body=False), data=data, files=files)
        else:
            response = await client.post(
                image_api_url("images/generations"),
                headers=api_headers(),
                json={"model": model, "prompt": prompt, "size": size, "quality": quality, "response_format": "url", "n": 1},
            )
        response.raise_for_status()
        raw = response.json()
        try:
            return extract_image(raw), raw
        except HTTPException:
            task_id = extract_task_id(raw)
            if not task_id:
                raise
        task_result = await wait_for_image_task(client, task_id)
        return extract_image(task_result), task_result

def upstream_message_from_record(item):
    role = item.get("role")
    if role not in {"user", "assistant"} or item.get("type") == "image":
        return None
    refs = item.get("attachments") or []
    if refs and role == "user":
        content = [{"type": "text", "text": item.get("content", "")}]
        for ref in refs[:4]:
            url = reference_to_data_url(ref)
            if url:
                content.append({"type": "image_url", "image_url": {"url": url}})
        return {"role": role, "content": content}
    return {"role": role, "content": item.get("content", "")}

def default_asset_library():
    return {"items": [], "folders": [], "updated_at": now_ms()}

def load_asset_library():
    if not os.path.exists(ASSET_LIBRARY_FILE):
        return default_asset_library()
    try:
        with open(ASSET_LIBRARY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return default_asset_library()
        data.setdefault("items", [])
        data.setdefault("folders", [])
        for item in data.get("items", []):
            item.setdefault("folder_id", "")
        return data
    except Exception:
        return default_asset_library()

def save_asset_library(library):
    os.makedirs(DATA_DIR, exist_ok=True)
    library["updated_at"] = now_ms()
    library.setdefault("folders", [])
    library["items"] = sorted(library.get("items", []), key=lambda x: x.get("created_at", 0), reverse=True)
    library["folders"] = sorted(library.get("folders", []), key=lambda x: x.get("created_at", 0))
    with open(ASSET_LIBRARY_FILE, "w", encoding="utf-8") as f:
        json.dump(library, f, ensure_ascii=False, indent=2)

def delete_asset_folder_from_library(library, folder_id: str):
    folder_id = (folder_id or "").strip()
    folders = library.get("folders", [])
    remaining_folders = [folder for folder in folders if folder.get("id") != folder_id]
    removed = len(folders) - len(remaining_folders)
    if not removed:
        return 0, 0
    library["folders"] = remaining_folders
    unfiled = 0
    for item in library.get("items", []):
        if item.get("folder_id") == folder_id:
            item["folder_id"] = ""
            unfiled += 1
    return removed, unfiled

def asset_url_for(filename):
    return f"/assets/library/{filename}"

def safe_asset_name(name, fallback="asset"):
    base = os.path.basename(name or fallback)
    stem, ext = os.path.splitext(base)
    ext = ext.lower() if ext.lower() in {".png", ".jpg", ".jpeg", ".webp", ".gif"} else ".png"
    stem = re.sub(r"[^a-zA-Z0-9_.-]+", "-", stem).strip(".-") or fallback
    return f"{stem[:48]}{ext}"

def add_asset_record(url, name="", remote_url="", source_url=""):
    library = load_asset_library()
    existing = next((item for item in library.get("items", []) if item.get("url") == url or (source_url and item.get("source_url") == source_url)), None)
    if existing:
        if remote_url:
            existing["remote_url"] = remote_url
        existing["name"] = name or existing.get("name") or os.path.basename(url)
        if source_url:
            existing["source_url"] = source_url
        save_asset_library(library)
        return existing
    item = {
        "id": uuid.uuid4().hex,
        "name": name or os.path.basename(url),
        "url": url,
        "remote_url": remote_url,
        "source_url": source_url,
        "folder_id": "",
        "created_at": now_ms(),
    }
    library.setdefault("items", []).append(item)
    save_asset_library(library)
    return item

async def upload_to_lsky(path):
    if not IMAGE_HOST_BASE_URL:
        return ""
    token = IMAGE_HOST_TOKEN
    api_root = lsky_api_root()
    async with httpx.AsyncClient(timeout=AI_REQUEST_TIMEOUT) as client:
        if not token and IMAGE_HOST_USERNAME and IMAGE_HOST_PASSWORD:
            login_url = f"{api_root}/v1/tokens"
            login = await client.post(login_url, headers={"Accept": "application/json"}, data={"email": IMAGE_HOST_USERNAME, "password": IMAGE_HOST_PASSWORD})
            login.raise_for_status()
            payload = login.json()
            token = str(payload.get("data", {}).get("token") or payload.get("token") or "")
        if not token:
            raise HTTPException(status_code=400, detail="图床未配置 Token 或账号密码")
        with open(path, "rb") as fh:
            files = {"file": (os.path.basename(path), fh, content_type_for_path(path))}
            resp = await client.post(f"{api_root}/v1/upload", headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, data={"strategy_id": "2"}, files=files)
        resp.raise_for_status()
        data = resp.json()
        return (
            data.get("data", {}).get("links", {}).get("url")
            or data.get("data", {}).get("url")
            or data.get("url")
            or ""
        )

def lsky_api_root():
    api_root = IMAGE_HOST_BASE_URL.rstrip("/")
    if api_root.endswith("/api/v1"):
        api_root = api_root[:-3]
    elif not api_root.endswith("/api"):
        api_root = api_root + "/api"
    return api_root

def upload_to_lsky_sync(path):
    if not IMAGE_HOST_BASE_URL:
        return ""
    token = IMAGE_HOST_TOKEN
    api_root = lsky_api_root()
    if not token and IMAGE_HOST_USERNAME and IMAGE_HOST_PASSWORD:
        response = requests.post(
            f"{api_root}/v1/tokens",
            headers={"Accept": "application/json"},
            data={"email": IMAGE_HOST_USERNAME, "password": IMAGE_HOST_PASSWORD},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        token = str(payload.get("data", {}).get("token") or payload.get("token") or "")
    if not token:
        raise HTTPException(status_code=400, detail="图床未配置 Token 或账号密码")
    data = post_lsky_file_sync(api_root, token, path)
    url = lsky_response_url(data)
    if not url:
        fallback_path = compressed_upload_copy(path)
        if fallback_path and fallback_path != path:
            try:
                data = post_lsky_file_sync(api_root, token, fallback_path)
            finally:
                try:
                    os.remove(fallback_path)
                except OSError:
                    pass
    return lsky_response_url(data)

def post_lsky_file_sync(api_root, token, path):
    with open(path, "rb") as fh:
        files = {"file": (os.path.basename(path), fh, content_type_for_path(path))}
        response = requests.post(
            f"{api_root}/v1/upload",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            data={"strategy_id": "2"},
            files=files,
            timeout=AI_REQUEST_TIMEOUT,
        )
    response.raise_for_status()
    return response.json()

def lsky_response_url(data):
    if not isinstance(data, dict) or data.get("status") is False:
        return ""
    return (
        data.get("data", {}).get("links", {}).get("url")
        or data.get("data", {}).get("url")
        or data.get("url")
        or ""
    )

def compressed_upload_copy(path):
    try:
        with Image.open(path) as img:
            img = img.convert("RGB")
            img.thumbnail((2048, 2048), Image.LANCZOS)
            temp = tempfile.NamedTemporaryFile(prefix="lsky_upload_", suffix=".jpg", delete=False)
            temp.close()
            img.save(temp.name, "JPEG", quality=88, optimize=True)
            return temp.name
    except Exception as e:
        print(f"图床压缩上传副本失败: {e}")
        return path

async def maybe_upload_asset_remote(path):
    if IMAGE_HOST_STRATEGY not in {"remote", "local_and_remote"}:
        return ""
    if IMAGE_HOST_TYPE != "lsky":
        raise HTTPException(status_code=400, detail="当前只支持兰空图床/Lsky 上传")
    return await upload_to_lsky(path)

def maybe_upload_asset_remote_sync(path):
    if IMAGE_HOST_STRATEGY not in {"remote", "local_and_remote"}:
        return ""
    if IMAGE_HOST_TYPE != "lsky":
        raise HTTPException(status_code=400, detail="当前只支持兰空图床/Lsky 上传")
    return upload_to_lsky_sync(path)

def sync_generated_asset(source_url, name=""):
    source_path = output_file_from_url(source_url)
    if not source_path:
        return None
    if source_url.startswith("/assets/library/"):
        remote_url = maybe_upload_asset_remote_sync(source_path)
        return add_asset_record(source_url, name or os.path.basename(source_path), remote_url, source_url=source_url)
    filename = f"{uuid.uuid4().hex[:10]}_{safe_asset_name(name or os.path.basename(source_path))}"
    target = os.path.join(ASSET_LIBRARY_DIR, filename)
    shutil.copyfile(source_path, target)
    remote_url = maybe_upload_asset_remote_sync(target)
    return add_asset_record(asset_url_for(filename), name or filename, remote_url, source_url=source_url)

def delete_history_records_for_asset(item):
    urls = {item.get("url", ""), item.get("source_url", "")}
    urls = {url for url in urls if isinstance(url, str) and url}
    if not urls or not os.path.exists(HISTORY_FILE):
        return 0
    removed = []
    with HISTORY_LOCK:
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except Exception:
            return 0
        kept = []
        for record in history:
            images = record.get("images", []) or []
            if any(isinstance(url, str) and url in urls for url in images):
                removed.append(record)
            else:
                kept.append(record)
        if removed:
            with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(kept, f, ensure_ascii=False, indent=4)
    for record in removed:
        for img_url in record.get("images", []) or []:
            if isinstance(img_url, str) and img_url.startswith("/output/"):
                path = output_file_from_url(img_url)
                if path:
                    try:
                        os.remove(path)
                    except OSError:
                        pass
    return len(removed)

def delete_asset_file(item):
    path = output_file_from_url(item.get("url", ""))
    if path and os.path.commonpath([os.path.abspath(ASSET_LIBRARY_DIR), path]) == os.path.abspath(ASSET_LIBRARY_DIR):
        try:
            os.remove(path)
        except OSError:
            pass

def remove_asset_records(asset_ids):
    wanted = {asset_id for asset_id in asset_ids if asset_id}
    if not wanted:
        return []
    with ASSET_LIBRARY_LOCK:
        library = load_asset_library()
        removed = [item for item in library.get("items", []) if item.get("id") in wanted]
        library["items"] = [item for item in library.get("items", []) if item.get("id") not in wanted]
        save_asset_library(library)
    return removed

PROMPTDEXTER_BASE_URL = "https://promptdexter.com"
PROMPTDEXTER_IMAGE_SITEMAP_URL = f"{PROMPTDEXTER_BASE_URL}/image-sitemap.xml"
PROMPTDEXTER_HOME_URL = PROMPTDEXTER_BASE_URL
PROMPTDEXTER_SYNC_INTERVAL_MS = 60 * 60 * 1000
PROMPTDEXTER_USER_AGENT = "Infinite-Canvas PromptDexter local sync/1.0"
PROMPTDEXTER_CATALOG_INDEX_VERSION = 2

PROMPTDEXTER_DEFAULT_CATEGORIES = [
    ("featured", "Featured Prompts", "精选提示词"),
    ("people", "People", "人物"),
    ("selfie", "Selfie", "自拍"),
    ("product-photography", "Product Photography", "产品摄影"),
    ("editorial", "Editorial", "编辑摄影"),
    ("anime", "Anime", "动漫"),
    ("digital-art", "Digital Art", "数字艺术"),
    ("traditional-art", "Traditional Art", "传统艺术"),
    ("illustration", "Illustration", "插画"),
    ("sci-fi", "Sci-Fi", "科幻"),
    ("cinematic", "Cinematic", "电影感"),
    ("fashion", "Fashion", "时尚"),
    ("fitness-sports", "Fitness & Sports", "健身与运动"),
    ("travel", "Travel", "旅行"),
]

PROMPTDEXTER_CATEGORY_ZH = {source.lower(): zh for _, source, zh in PROMPTDEXTER_DEFAULT_CATEGORIES}
PROMPTDEXTER_CATEGORY_ZH.update({
    "real people": "人物",
    "featured": "精选提示词",
    "fitness": "健身与运动",
    "sports": "健身与运动",
})
PROMPTDEXTER_CATEGORY_ALIASES = {
    "featured": "featured",
    "featured-prompts": "featured",
    "real-people": "people",
    "people": "people",
    "selfie": "selfie",
    "product-photography": "product-photography",
    "editorial": "editorial",
    "anime": "anime",
    "digital-art": "digital-art",
    "traditional-art": "traditional-art",
    "illustration": "illustration",
    "sci-fi": "sci-fi",
    "sci-fi-art": "sci-fi",
    "cinematic": "cinematic",
    "fashion": "fashion",
    "fitness-sports": "fitness-sports",
    "fitness-and-sports": "fitness-sports",
    "fitness": "fitness-sports",
    "sports": "fitness-sports",
    "travel": "travel",
}

PROMPTDEXTER_TAG_ZH = {
    "realistic": "真实感",
    "female": "女性",
    "male": "男性",
    "woman": "女子",
    "man": "男子",
    "young adult": "青年",
    "adult": "成人",
    "teen": "青少年",
    "full body": "全身",
    "portrait": "肖像",
    "close up": "近景",
    "low angle": "低角度",
    "high angle": "高角度",
    "nature": "自然",
    "outdoors": "户外",
    "indoor": "室内",
    "studio": "影棚",
    "fashion": "时尚",
    "travel": "旅行",
    "editorial": "编辑摄影",
    "cinematic": "电影感",
    "anime": "动漫",
    "digital art": "数字艺术",
    "traditional art": "传统艺术",
    "illustration": "插画",
    "sci-fi": "科幻",
    "product": "产品",
    "product photography": "产品摄影",
    "beauty": "美妆",
    "lifestyle": "生活方式",
    "street": "街景",
    "urban": "城市",
    "outdoor": "户外",
    "natural": "自然",
    "luxury": "高级感",
    "glamour": "魅力",
    "minimal": "极简",
    "vibrant": "鲜艳",
    "black and white": "黑白",
    "monochrome": "单色",
    "wedding": "婚礼",
    "sports bra": "运动内衣",
    "auto rickshaw": "机动三轮车",
}

PROMPTDEXTER_PHRASE_ZH = {
    "auto rickshaw": "机动三轮车",
    "black and white": "黑白",
    "sports bra": "运动内衣",
    "young woman": "年轻女子",
    "young man": "年轻男子",
    "white floral dress": "白色碎花连衣裙",
    "palm tree": "棕榈树",
    "mirror selfie": "镜中自拍",
    "cherry blossoms": "樱花",
    "low angle": "低角度",
    "high angle": "高角度",
    "full body": "全身",
    "close up": "近景",
    "close-up": "近景",
    "red carpet": "红毯",
    "golden hour": "黄金时刻",
}

PROMPTDEXTER_WORD_ZH = {
    "a": "一位",
    "an": "一位",
    "and": "和",
    "with": "带有",
    "in": "穿着",
    "on": "在",
    "by": "在旁边",
    "against": "以为背景",
    "near": "靠近",
    "inside": "在里面",
    "outdoors": "户外",
    "indoors": "室内",
    "young": "年轻",
    "woman": "女子",
    "man": "男子",
    "girl": "女孩",
    "boy": "男孩",
    "couple": "情侣",
    "smiling": "微笑",
    "portrait": "肖像",
    "studio": "影棚",
    "selfie": "自拍",
    "mirror": "镜中",
    "white": "白色",
    "black": "黑色",
    "dark": "深色",
    "blonde": "金发",
    "brown": "棕色",
    "red": "红色",
    "blue": "蓝色",
    "green": "绿色",
    "pink": "粉色",
    "gold": "金色",
    "silver": "银色",
    "purple": "紫色",
    "floral": "碎花",
    "striped": "条纹",
    "mini": "迷你",
    "dress": "连衣裙",
    "gown": "礼服",
    "saree": "纱丽",
    "sari": "纱丽",
    "jacket": "夹克",
    "attire": "服装",
    "sports": "运动",
    "bra": "内衣",
    "bikini": "比基尼",
    "blouse": "上衣",
    "sweater": "毛衣",
    "blazer": "西装外套",
    "hoodie": "连帽衫",
    "wearing": "穿着",
    "seated": "坐着",
    "sitting": "坐着",
    "standing": "站立",
    "kneeling": "跪坐",
    "leaning": "倚靠",
    "holding": "拿着",
    "taking": "拍摄",
    "drawing": "拉开",
    "picking": "拾起",
    "drinking": "饮用",
    "looking": "看向",
    "smile": "微笑",
    "hair": "头发",
    "beard": "胡须",
    "hijab": "头巾",
    "bindi": "额饰",
    "face": "面部",
    "riverbank": "河岸",
    "palm": "棕榈",
    "tree": "树",
    "forest": "森林",
    "garden": "花园",
    "restaurant": "餐厅",
    "bathroom": "浴室",
    "bedroom": "卧室",
    "kitchen": "厨房",
    "window": "窗边",
    "table": "桌边",
    "doorframe": "门框",
    "lawn": "草坪",
    "stone": "石头",
    "fog": "浓雾",
    "field": "田野",
    "courtyard": "庭院",
    "street": "街道",
    "background": "背景",
    "backdrop": "背景",
    "classic": "经典",
    "car": "汽车",
    "ferrari": "法拉利",
    "rickshaw": "三轮车",
    "wedding": "婚礼",
    "chess": "棋子",
    "pieces": "棋子",
    "skateboarder": "滑板手",
    "midair": "腾空",
    "abstract": "抽象",
    "vibrant": "鲜艳",
    "lush": "茂密",
    "gracefully": "优雅",
    "underwater": "水下",
    "muscular": "健硕",
    "elegant": "优雅",
    "fit": "健美",
    "athletic": "运动",
    "royal": "皇家",
    "carpet": "地毯",
    "foliage": "绿植",
    "glamorous": "华丽",
    "kiss": "飞吻",
    "decorated": "装饰",
    "solid": "纯色",
    "deep": "深",
    "miniature": "微缩",
    "diorama": "立体场景",
    "bowl": "碗",
    "bonsai": "盆景",
    "warrior": "战士",
    "sunset": "日落",
    "backlighting": "逆光",
    "silhouette": "剪影",
    "poppies": "罂粟花",
    "teal": "蓝绿色",
    "cosmic": "宇宙",
    "portal": "传送门",
    "hanfu": "汉服",
    "photography": "摄影",
    "fashion": "时尚",
    "cinematic": "电影感",
    "anime": "动漫",
}

def promptdexter_slug(value: str) -> str:
    text = html.unescape(str(value or "")).strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")

def promptdexter_category_item(category_id: str, source_name: str = "") -> Dict[str, str]:
    category_id = promptdexter_slug(category_id or source_name or "featured") or "featured"
    source_name = html.unescape(str(source_name or category_id).strip())
    zh = PROMPTDEXTER_CATEGORY_ZH.get(source_name.lower()) or localize_promptdexter_label(source_name)
    return {"id": category_id, "source_name": source_name, "name": zh}

def promptdexter_category_page_slug(category_id: str) -> str:
    if category_id == "fitness-sports":
        return "fitness-and-sports"
    return category_id

def localize_promptdexter_label(value: str) -> str:
    raw = html.unescape(str(value or "")).strip()
    if not raw:
        return "未分类"
    key = raw.lower().replace("-", " ").replace("_", " ")
    key = re.sub(r"\s+", " ", key).strip()
    if key in PROMPTDEXTER_CATEGORY_ZH:
        return PROMPTDEXTER_CATEGORY_ZH[key]
    if key in PROMPTDEXTER_TAG_ZH:
        return PROMPTDEXTER_TAG_ZH[key]
    if key in PROMPTDEXTER_PHRASE_ZH:
        return PROMPTDEXTER_PHRASE_ZH[key]
    parts = [part for part in re.split(r"[\s\-/&]+", key) if part]
    translated = [PROMPTDEXTER_WORD_ZH.get(part, part) for part in parts]
    result = "".join(translated)
    return result if result and not result.isascii() else raw

def localize_promptdexter_title(title: str) -> str:
    title = html.unescape(str(title or "")).strip()
    if not title:
        return "未命名提示词"
    lower = title.lower()
    exact = {
        "smiling woman in white floral dress by riverbank with palm tree": "一位身穿白色碎花连衣裙的微笑女子站在河岸边，旁边是一棵棕榈树",
    }
    if lower in exact:
        return exact[lower]
    for phrase, zh in sorted(PROMPTDEXTER_PHRASE_ZH.items(), key=lambda item: len(item[0]), reverse=True):
        lower = re.sub(rf"\b{re.escape(phrase)}\b", f" {zh} ", lower)
    words = [word for word in re.split(r"[\s\-/]+", lower.replace("&", " and ")) if word]
    translated = [PROMPTDEXTER_WORD_ZH.get(word, word) for word in words]
    text = "".join(translated)
    if text and not text.isascii():
        return text
    return f"图片提示词：{title}"

def promptdexter_description(title_zh: str, categories_zh: List[str]) -> str:
    cats = "、".join([item for item in categories_zh if item]) or "图像创作"
    return f"创作一幅画面：{title_zh}。适合用于{cats}类图像提示。"

def promptdexter_valid_prompt_text(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if re.fullmatch(r"\$[0-9a-fA-F]{1,4}", text):
        return False
    return len(text) >= 20 or "\n" in text

def default_prompt_library():
    categories = [promptdexter_category_item(cid, source) for cid, source, _ in PROMPTDEXTER_DEFAULT_CATEGORIES]
    return {
        "version": 1,
        "updated_at": now_ms(),
        "my_presets": {
            "id": "mine",
            "name": "我的预设",
            "items": [],
            "categories": [],
        },
        "promptdexter": {
            "id": "promptdexter",
            "name": "PromptDexter 同步源",
            "readonly": True,
            "last_sync_at": 0,
            "categories": categories,
            "items": [],
        },
    }

def normalize_prompt_library(data):
    if not isinstance(data, dict):
        data = default_prompt_library()
    base = default_prompt_library()
    data.setdefault("version", 1)
    data.setdefault("my_presets", base["my_presets"])
    data.setdefault("promptdexter", base["promptdexter"])
    data["my_presets"].setdefault("id", "mine")
    data["my_presets"].setdefault("name", "我的预设")
    data["my_presets"].setdefault("items", [])
    data["my_presets"].setdefault("categories", [])
    source = data["promptdexter"]
    source.setdefault("id", "promptdexter")
    source.setdefault("name", "PromptDexter 同步源")
    source.setdefault("readonly", True)
    source.setdefault("last_sync_at", 0)
    source.setdefault("categories", base["promptdexter"]["categories"])
    source.setdefault("items", [])
    for item in source.get("items", []):
        if item.get("prompt") and not promptdexter_valid_prompt_text(item.get("prompt")):
            item["prompt"] = ""
            item["detail_loaded"] = False
            item["detail_invalid_reason"] = "invalid_prompt_placeholder"
    source["items"] = sorted(source.get("items", []), key=lambda x: x.get("synced_at", 0), reverse=True)
    data["my_presets"]["items"] = sorted(data["my_presets"].get("items", []), key=lambda x: x.get("updated_at", x.get("created_at", 0)), reverse=True)
    data.setdefault("updated_at", now_ms())
    return data

def load_prompt_library():
    if not os.path.exists(PROMPT_LIBRARY_FILE):
        return default_prompt_library()
    try:
        with open(PROMPT_LIBRARY_FILE, "r", encoding="utf-8") as f:
            return normalize_prompt_library(json.load(f))
    except Exception:
        return default_prompt_library()

def save_prompt_library(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    data = normalize_prompt_library(data)
    data["updated_at"] = now_ms()
    with open(PROMPT_LIBRARY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return data

def promptdexter_item_id(url: str) -> str:
    return "pd_" + hashlib.sha1(str(url or "").encode("utf-8")).hexdigest()[:16]

def promptdexter_fetch(url: str, timeout: int = 20) -> str:
    response = requests.get(url, headers={"User-Agent": PROMPTDEXTER_USER_AGENT}, timeout=timeout)
    response.raise_for_status()
    return response.text

def promptdexter_fetch_categories() -> List[Dict[str, str]]:
    categories = [promptdexter_category_item(cid, source) for cid, source, _ in PROMPTDEXTER_DEFAULT_CATEGORIES]
    seen = {item["id"] for item in categories}
    try:
        text = promptdexter_fetch(PROMPTDEXTER_HOME_URL, timeout=15)
        for slug, label in re.findall(r'href=["\']/prompts/([^"\']+)["\'][^>]*>([^<]+)</a>', text):
            cid = promptdexter_slug(slug)
            if cid and cid not in seen:
                categories.append(promptdexter_category_item(cid, label))
                seen.add(cid)
    except Exception as e:
        print(f"PromptDexter 分类同步失败: {e}")
    return categories

def promptdexter_parse_image_sitemap() -> List[Dict[str, str]]:
    xml_text = promptdexter_fetch(PROMPTDEXTER_IMAGE_SITEMAP_URL, timeout=25)
    root = ET.fromstring(xml_text)
    ns = {
        "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
        "image": "http://www.google.com/schemas/sitemap-image/1.1",
    }
    entries = []
    for url_node in root.findall("sm:url", ns):
        loc = (url_node.findtext("sm:loc", default="", namespaces=ns) or "").strip()
        if "/prompt/" not in loc:
            continue
        image_node = url_node.find("image:image", ns)
        image_url = ""
        title = ""
        caption = ""
        if image_node is not None:
            image_url = (image_node.findtext("image:loc", default="", namespaces=ns) or "").strip()
            title = html.unescape((image_node.findtext("image:title", default="", namespaces=ns) or "").strip())
            caption = html.unescape((image_node.findtext("image:caption", default="", namespaces=ns) or "").strip())
        entries.append({
            "url": loc,
            "slug": loc.rstrip("/").split("/")[-1],
            "image_url": image_url,
            "title_original": title,
            "caption": caption,
        })
    return entries

def promptdexter_category_link_index(categories: List[Dict[str, str]]) -> Dict[str, List[str]]:
    index: Dict[str, List[str]] = {}
    for category in categories:
        category_id = category.get("id") or ""
        if not category_id or category_id == "featured":
            continue
        try:
            text = promptdexter_fetch(f"{PROMPTDEXTER_BASE_URL}/prompts/{promptdexter_category_page_slug(category_id)}", timeout=15)
        except Exception as e:
            print(f"PromptDexter 分类页读取失败 {category_id}: {e}")
            continue
        slugs = set()
        for href in re.findall(r'href=["\'](/prompt/[^"\']+)["\']', text):
            slug = href.rstrip("/").split("/")[-1]
            if slug:
                slugs.add(slug)
        for slug in slugs:
            index.setdefault(slug, [])
            if category_id not in index[slug]:
                index[slug].append(category_id)
    return index

def promptdexter_infer_categories(entry: Dict[str, str], categories: List[Dict[str, str]], link_index: Optional[Dict[str, List[str]]] = None) -> List[str]:
    slug = entry.get("slug") or ""
    found = list((link_index or {}).get(slug, []))
    if found:
        return found
    text = " ".join([
        entry.get("slug", ""),
        entry.get("title_original", ""),
        entry.get("caption", ""),
    ]).lower()
    text = text.replace("-", " ")
    def has_keyword(keyword: str) -> bool:
        keyword = str(keyword or "").strip().lower()
        if not keyword:
            return False
        pattern = r"(?<![a-z0-9])" + re.escape(keyword) + r"(?![a-z0-9])"
        return re.search(pattern, text) is not None
    rules = [
        ("selfie", ("selfie", "mirror", "phone", "smartphone")),
        ("product-photography", ("product", "bottle", "cosmetic", "skincare", "supplement", "tube", "cream", "perfume", "ring", "shoe", "sneaker", "boot", "chair", "jewelry", "watch", "bag", "packaging", "still life")),
        ("anime", ("anime", "manga", "waifu", "chibi")),
        ("sci-fi", ("sci fi", "science fiction", "futuristic", "cyberpunk", "robot", "spaceship", "space ", "cosmic", "portal", "alien", "neon city")),
        ("traditional-art", ("watercolor", "oil painting", "pencil", "charcoal", "ink drawing", "gouache", "canvas painting")),
        ("illustration", ("illustration", "illustrated", "vector", "cartoon", "flat design", "storybook", "comic")),
        ("digital-art", ("digital art", "digital painting", "3d render", "rendered", "concept art", "fantasy art", "abstract background", "surreal")),
        ("fitness-sports", ("fitness", "gym", "athletic", "sports", "sport", "workout", "skateboard", "archery", "muscular", "runner", "boxing", "yoga")),
        ("travel", ("travel", "beach", "riverbank", "mountain", "landscape", "desert", "canal", "marina", "boat", "street", "city", "outdoor", "outdoors", "garden", "forest", "field", "terrace", "rooftop")),
        ("cinematic", ("cinematic", "film", "movie", "dramatic", "foggy", "moody", "backlighting", "golden hour", "spotlight", "noir")),
        ("editorial", ("editorial", "magazine", "high fashion", "red carpet", "studio portrait", "fashion portrait", "gala", "luxury hotel")),
        ("fashion", ("fashion", "gown", "dress", "outfit", "garment", "blazer", "hoodie", "sweater", "saree", "sari", "lehenga", "jacket", "corset", "bikini", "runway")),
        ("people", ("woman", "man", "girl", "boy", "person", "people", "portrait", "couple", "model", "bride")),
    ]
    category_ids = {item.get("id") for item in categories}
    for category_id, keywords in rules:
        if category_id in category_ids and any(has_keyword(keyword) for keyword in keywords):
            if category_id not in found:
                found.append(category_id)
    return found or ["featured"]

def promptdexter_normalize_category_id(value: str, category_ids: set) -> Optional[str]:
    slug = promptdexter_slug(value)
    category_id = PROMPTDEXTER_CATEGORY_ALIASES.get(slug) or slug
    if category_id in category_ids:
        return category_id
    return None

def promptdexter_tag_item(value: str) -> Optional[Dict[str, str]]:
    raw = html.unescape(str(value or "").strip())
    tag_id = promptdexter_slug(raw)
    if not tag_id:
        return None
    return {"id": tag_id, "name": localize_promptdexter_label(raw), "source_name": raw}

def promptdexter_add_tag(tags: List[Dict[str, str]], value: str) -> None:
    tag = promptdexter_tag_item(value)
    if tag and tag["id"] not in {item.get("id") for item in tags}:
        tags.append(tag)

def promptdexter_parse_embedded_catalog(text: str, categories: List[Dict[str, str]], page_category_id: str = "") -> Dict[str, Dict[str, Any]]:
    category_ids = {item.get("id") for item in categories}
    page_category = promptdexter_normalize_category_id(page_category_id, category_ids) if page_category_id else None
    records: Dict[str, Dict[str, Any]] = {}
    pattern = r'\{\\"img\\":\\".*?\\"created_at\\":\\"[^\\"]+\\"\}'
    for raw in re.findall(pattern, text, re.S):
        try:
            item = json.loads(raw.encode("utf-8").decode("unicode_escape"))
        except Exception:
            continue
        title = html.unescape(str(item.get("title") or "").strip())
        slug = promptdexter_slug(title)
        if not slug:
            continue
        category_values = item.get("category") if isinstance(item.get("category"), list) else []
        tag_values = item.get("tags") if isinstance(item.get("tags"), list) else []
        item_categories = []
        item_tags: List[Dict[str, str]] = []
        for value in category_values:
            category_id = promptdexter_normalize_category_id(str(value), category_ids)
            if category_id:
                if category_id not in item_categories:
                    item_categories.append(category_id)
            else:
                promptdexter_add_tag(item_tags, str(value))
        if page_category and page_category not in item_categories:
            item_categories.append(page_category)
        if item.get("featured") and "featured" in category_ids and "featured" not in item_categories:
            item_categories.insert(0, "featured")
        for value in tag_values:
            promptdexter_add_tag(item_tags, str(value))
        if not item_categories:
            item_categories = ["featured"]
        image_name = str(item.get("img") or "").strip()
        image_webp = re.sub(r"\.[^.]+$", ".webp", image_name) if image_name else ""
        image_url = f"{PROMPTDEXTER_BASE_URL}/images/explore-thumbnails/{image_webp}" if image_webp else ""
        records[slug] = {
            "slug": slug,
            "title_original": title,
            "prompt": html.unescape(str(item.get("prompt") or "").strip()),
            "image_url": image_url,
            "categories": item_categories,
            "tags": item_tags,
            "featured": bool(item.get("featured")),
            "created_at": item.get("created_at") or "",
        }
    return records

def promptdexter_merge_catalog_record(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    if not existing:
        return incoming
    for category_id in incoming.get("categories") or []:
        existing.setdefault("categories", [])
        if category_id not in existing["categories"]:
            existing["categories"].append(category_id)
    tag_ids = {tag.get("id") for tag in existing.get("tags") or []}
    for tag in incoming.get("tags") or []:
        if tag.get("id") not in tag_ids:
            existing.setdefault("tags", []).append(tag)
            tag_ids.add(tag.get("id"))
    for key in ("title_original", "prompt", "image_url", "created_at"):
        if incoming.get(key) and not existing.get(key):
            existing[key] = incoming[key]
    existing["featured"] = bool(existing.get("featured") or incoming.get("featured"))
    return existing

def promptdexter_compact_catalog_index(index: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        slug: {
            "categories": record.get("categories") or [],
            "tags": record.get("tags") or [],
            "image_url": record.get("image_url") or "",
            "title_original": record.get("title_original") or "",
        }
        for slug, record in index.items()
    }

def promptdexter_fetch_embedded_catalog_index(categories: List[Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    category_ids = [item.get("id") for item in categories if item.get("id") and item.get("id") != "featured"]
    for category_id in category_ids:
        try:
            text = promptdexter_fetch(f"{PROMPTDEXTER_BASE_URL}/prompts/{promptdexter_category_page_slug(category_id)}", timeout=20)
        except Exception as e:
            print(f"PromptDexter 内嵌分类读取失败 {category_id}: {e}")
            continue
        for slug, record in promptdexter_parse_embedded_catalog(text, categories, category_id).items():
            index[slug] = promptdexter_merge_catalog_record(index.get(slug, {}), record)
    return index

def apply_promptdexter_catalog_record(item: Dict[str, Any], record: Dict[str, Any], categories: List[Dict[str, str]]) -> bool:
    changed = False
    if record.get("title_original") and item.get("title_original") != record.get("title_original"):
        item["title_original"] = record["title_original"]
        item["title"] = localize_promptdexter_title(record["title_original"])
        changed = True
    if record.get("image_url") and item.get("image_url") != record.get("image_url"):
        item["image_url"] = record["image_url"]
        changed = True
    if record.get("prompt") and item.get("prompt") != record.get("prompt"):
        item["prompt"] = record["prompt"]
        item["detail_loaded"] = True
        item["detail_loaded_at"] = now_ms()
        changed = True
    if record.get("categories") and item.get("categories") != record.get("categories"):
        item["categories"] = record["categories"]
        changed = True
    if record.get("tags") is not None and item.get("tags") != record.get("tags"):
        item["tags"] = record["tags"]
        changed = True
    if changed:
        category_names = [next((cat.get("name") for cat in categories if cat.get("id") == cid), localize_promptdexter_label(cid)) for cid in item.get("categories", [])]
        item["description"] = promptdexter_description(item.get("title") or localize_promptdexter_title(item.get("title_original", "")), category_names)
        item["synced_at"] = now_ms()
    return changed

def promptdexter_entry_from_cached_item(item: Dict[str, Any]) -> Dict[str, str]:
    return {
        "slug": item.get("slug", ""),
        "title_original": item.get("title_original") or item.get("title", ""),
        "caption": item.get("description_original") or item.get("description", ""),
    }

def reclassify_cached_promptdexter_items(library: Dict[str, Any], link_index: Optional[Dict[str, List[str]]] = None, catalog_index: Optional[Dict[str, Dict[str, Any]]] = None) -> bool:
    source = library.get("promptdexter") or {}
    categories = source.get("categories") or []
    if not categories or not source.get("items"):
        return False
    changed = False
    for item in source.get("items", []):
        if not isinstance(item, dict):
            continue
        catalog_record = (catalog_index or {}).get(item.get("slug") or "")
        if catalog_record:
            if apply_promptdexter_catalog_record(item, catalog_record, categories):
                changed = True
            continue
        if item.get("detail_loaded") and item.get("categories"):
            continue
        inferred_categories = promptdexter_infer_categories(promptdexter_entry_from_cached_item(item), categories, link_index)
        if inferred_categories and item.get("categories") != inferred_categories:
            item["categories"] = inferred_categories
            category_names = [next((cat.get("name") for cat in categories if cat.get("id") == cid), localize_promptdexter_label(cid)) for cid in inferred_categories]
            item["description"] = promptdexter_description(item.get("title") or localize_promptdexter_title(item.get("title_original", "")), category_names)
            changed = True
    return changed

def promptdexter_graph_from_html(text: str) -> List[Dict[str, Any]]:
    graphs = []
    for match in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', text, re.S | re.I):
        raw = html.unescape(match.group(1).strip())
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if isinstance(payload, dict) and isinstance(payload.get("@graph"), list):
            graphs.extend([item for item in payload["@graph"] if isinstance(item, dict)])
        elif isinstance(payload, dict):
            graphs.append(payload)
    return graphs

def promptdexter_meta_content(text: str, name: str) -> str:
    patterns = [
        rf'<meta[^>]+property=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
        rf'<meta[^>]+name=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return html.unescape(match.group(1))
    return ""

def promptdexter_classify_keywords(keywords: List[str], categories: List[Dict[str, str]]) -> tuple:
    category_ids = {item.get("id") for item in categories}
    category_lookup = {item.get("source_name", "").lower(): item.get("id") for item in categories}
    cats = []
    tags = []
    for keyword in keywords:
        raw = html.unescape(str(keyword or "").strip())
        if not raw:
            continue
        slug = promptdexter_slug(raw)
        category_id = PROMPTDEXTER_CATEGORY_ALIASES.get(slug) or (slug if slug in category_ids else category_lookup.get(raw.lower()))
        if category_id and category_id in category_ids:
            if category_id not in cats:
                cats.append(category_id)
        else:
            tag_id = slug or promptdexter_slug(raw)
            if tag_id and tag_id not in [item["id"] for item in tags]:
                tags.append({"id": tag_id, "name": localize_promptdexter_label(raw), "source_name": raw})
    if not cats:
        cats = ["featured"]
    return cats, tags

def promptdexter_parse_detail(url: str, categories: List[Dict[str, str]]) -> Dict[str, Any]:
    text = promptdexter_fetch(url, timeout=25)
    graphs = promptdexter_graph_from_html(text)
    creative = next((item for item in graphs if item.get("@type") == "CreativeWork"), {})
    breadcrumb = next((item for item in graphs if item.get("@type") == "BreadcrumbList"), {})
    title = html.unescape(str(creative.get("name") or promptdexter_meta_content(text, "og:title") or "").replace(" - AI Image Prompt", "").strip())
    prompt_text = html.unescape(str(creative.get("text") or "").strip())
    if not promptdexter_valid_prompt_text(prompt_text):
        prompt_text = ""
    description = html.unescape(str(creative.get("description") or promptdexter_meta_content(text, "description") or "").strip())
    image_url = str(creative.get("thumbnailUrl") or promptdexter_meta_content(text, "og:image") or "").strip()
    keywords_raw = creative.get("keywords") or ""
    if isinstance(keywords_raw, str):
        keywords = [item.strip() for item in keywords_raw.split(",") if item.strip()]
    elif isinstance(keywords_raw, list):
        keywords = [str(item).strip() for item in keywords_raw if str(item).strip()]
    else:
        keywords = []
    breadcrumb_items = breadcrumb.get("itemListElement") if isinstance(breadcrumb, dict) else []
    if isinstance(breadcrumb_items, list):
        for crumb in breadcrumb_items:
            name = str((crumb or {}).get("name") or "").strip()
            if name and name not in {"Home", "Prompts", title}:
                keywords.insert(0, name)
    category_ids, tags = promptdexter_classify_keywords(keywords, categories)
    category_names = [next((cat.get("name") for cat in categories if cat.get("id") == cid), localize_promptdexter_label(cid)) for cid in category_ids]
    title_zh = localize_promptdexter_title(title)
    return {
        "title_original": title,
        "title": title_zh,
        "description_original": description,
        "description": promptdexter_description(title_zh, category_names),
        "prompt": prompt_text,
        "image_url": image_url,
        "categories": category_ids,
        "tags": tags,
        "detail_loaded": bool(prompt_text),
        "detail_loaded_at": now_ms(),
    }

def promptdexter_entry_to_item(entry: Dict[str, str], categories: List[Dict[str, str]], link_index: Optional[Dict[str, List[str]]] = None) -> Dict[str, Any]:
    title_original = entry.get("title_original") or entry.get("slug", "").replace("-", " ")
    title_zh = localize_promptdexter_title(title_original)
    category_ids = promptdexter_infer_categories(entry, categories, link_index)
    category_names = [next((cat.get("name") for cat in categories if cat.get("id") == cid), localize_promptdexter_label(cid)) for cid in category_ids]
    return {
        "id": promptdexter_item_id(entry.get("url", "")),
        "source": "promptdexter",
        "source_url": entry.get("url", ""),
        "slug": entry.get("slug", ""),
        "image_url": entry.get("image_url", ""),
        "title_original": title_original,
        "title": title_zh,
        "description_original": entry.get("caption", ""),
        "description": promptdexter_description(title_zh, category_names),
        "prompt": "",
        "categories": category_ids,
        "tags": [],
        "detail_loaded": False,
        "synced_at": now_ms(),
    }

def update_promptdexter_item_detail(item: Dict[str, Any], categories: List[Dict[str, str]]) -> Dict[str, Any]:
    detail = promptdexter_parse_detail(item.get("source_url") or "", categories)
    item.update({key: value for key, value in detail.items() if value not in ("", [], None)})
    item["detail_loaded"] = bool(item.get("prompt"))
    if item["detail_loaded"]:
        item.pop("detail_invalid_reason", None)
    return item

def sync_promptdexter_library(limit: int = 0, fetch_details: int = 12, force: bool = False, category_id: str = "") -> Dict[str, Any]:
    limit = max(0, min(int(limit or 0), 5000))
    fetch_details = max(0, min(int(fetch_details or 0), 30))
    category_id = promptdexter_slug(category_id)
    with PROMPT_LIBRARY_LOCK:
        library = load_prompt_library()
        source = library["promptdexter"]
        now = now_ms()
        if not category_id and not force and source.get("items") and now - int(source.get("last_sync_at") or 0) < PROMPTDEXTER_SYNC_INTERVAL_MS:
            return {"library": library, "added": 0, "updated": 0, "details": 0, "skipped": True}
    categories = promptdexter_fetch_categories()
    category_ids = {item.get("id") for item in categories}
    if category_id and category_id not in category_ids:
        raise HTTPException(status_code=400, detail="PromptDexter 分类不存在")
    catalog_index = promptdexter_fetch_embedded_catalog_index(categories)
    link_index = promptdexter_category_link_index(categories)
    entries = promptdexter_parse_image_sitemap()
    if category_id:
        entries = [
            entry for entry in entries
            if category_id in ((catalog_index.get(entry.get("slug") or "") or {}).get("categories") or promptdexter_infer_categories(entry, categories, link_index))
        ]
    if limit:
        entries = entries[:limit]
    with PROMPT_LIBRARY_LOCK:
        library = load_prompt_library()
        source = library["promptdexter"]
        source["categories"] = categories
        source["embedded_catalog_index"] = promptdexter_compact_catalog_index(catalog_index)
        source["embedded_catalog_index_at"] = now_ms()
        source["embedded_catalog_index_version"] = PROMPTDEXTER_CATALOG_INDEX_VERSION
        source["category_link_index"] = link_index
        source["category_link_index_at"] = now_ms()
        by_url = {item.get("source_url"): item for item in source.get("items", []) if item.get("source_url")}
        added = 0
        updated = 0
        detail_count = 0
        for entry in entries:
            existing = by_url.get(entry["url"])
            catalog_record = catalog_index.get(entry.get("slug") or "")
            inferred_categories = (catalog_record or {}).get("categories") or promptdexter_infer_categories(entry, categories, link_index)
            inferred_names = [next((cat.get("name") for cat in categories if cat.get("id") == cid), localize_promptdexter_label(cid)) for cid in inferred_categories]
            if existing:
                if entry.get("image_url"):
                    existing["image_url"] = entry["image_url"]
                existing["title_original"] = entry.get("title_original") or existing.get("title_original", "")
                existing["title"] = existing.get("title") or localize_promptdexter_title(existing.get("title_original", ""))
                if catalog_record:
                    apply_promptdexter_catalog_record(existing, catalog_record, categories)
                elif not existing.get("detail_loaded"):
                    existing["categories"] = inferred_categories
                    existing["description"] = promptdexter_description(existing.get("title") or localize_promptdexter_title(existing.get("title_original", "")), inferred_names)
                updated += 1
            else:
                item = promptdexter_entry_to_item(entry, categories, link_index)
                if catalog_record:
                    apply_promptdexter_catalog_record(item, catalog_record, categories)
                source.setdefault("items", []).append(item)
                by_url[entry["url"]] = item
                added += 1
        detail_targets = [item for item in source.get("items", []) if not item.get("detail_loaded")][:fetch_details]
    for item in detail_targets:
        try:
            update_promptdexter_item_detail(item, categories)
            detail_count += 1
        except Exception as e:
            print(f"PromptDexter 详情同步失败 {item.get('source_url')}: {e}")
    with PROMPT_LIBRARY_LOCK:
        library = load_prompt_library()
        source = library["promptdexter"]
        source["categories"] = categories
        source["embedded_catalog_index"] = promptdexter_compact_catalog_index(catalog_index)
        source["embedded_catalog_index_at"] = now_ms()
        source["embedded_catalog_index_version"] = PROMPTDEXTER_CATALOG_INDEX_VERSION
        source["category_link_index"] = link_index
        source["category_link_index_at"] = now_ms()
        existing_by_id = {item.get("id"): item for item in source.get("items", [])}
        for item in by_url.values():
            existing_by_id[item.get("id")] = item
        source["items"] = list(existing_by_id.values())
        if not category_id:
            source["last_sync_at"] = now_ms()
        else:
            sync_map = source.get("category_sync_at") if isinstance(source.get("category_sync_at"), dict) else {}
            sync_map[category_id] = now_ms()
            source["category_sync_at"] = sync_map
        library = save_prompt_library(library)
    return {"library": library, "added": added, "updated": updated, "details": detail_count, "skipped": False, "category_id": category_id}

def find_promptdexter_item(library, item_id: str):
    return next((item for item in library.get("promptdexter", {}).get("items", []) if item.get("id") == item_id), None)

def update_promptdexter_prompt_in_library(library, item_id: str, prompt: str):
    item = find_promptdexter_item(library, item_id)
    if not item:
        return None
    value = str(prompt or "").strip()
    if not value:
        raise ValueError("提示词正文不能为空")
    item["prompt"] = value
    item["detail_loaded"] = True
    item["prompt_edited_at"] = now_ms()
    item.setdefault("detail_loaded_at", now_ms())
    item.pop("detail_invalid_reason", None)
    return item

def find_prompt_preset(library, item_id: str):
    return next((item for item in library.get("my_presets", {}).get("items", []) if item.get("id") == item_id), None)

def prompt_preset_from_payload(payload: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    now = now_ms()
    item = dict(existing or {})
    item.update({
        "title": str(payload.get("title") or item.get("title") or "我的提示词").strip()[:160],
        "description": str(payload.get("description") or item.get("description") or "").strip()[:1000],
        "prompt": str(payload.get("prompt") or item.get("prompt") or "").strip(),
        "image_url": str(payload.get("image_url") or item.get("image_url") or "").strip(),
        "categories": [str(x).strip() for x in payload.get("categories", item.get("categories", [])) if str(x).strip()],
        "tags": [str(x).strip() for x in payload.get("tags", item.get("tags", [])) if str(x).strip()],
        "updated_at": now,
    })
    item.setdefault("id", "preset_" + uuid.uuid4().hex[:12])
    item.setdefault("created_at", now)
    item.setdefault("source", "mine")
    return item

# --- 路由接口 ---

@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"), headers={"Cache-Control": "no-store"})

@app.get("/api/view")
def view_image(filename: str, type: str = "input", subfolder: str = ""):
    for addr in COMFYUI_INSTANCES:
        try:
            url = comfy_url(addr, "/view")
            params = {"filename": filename, "type": type, "subfolder": subfolder}
            r = requests.get(url, params=params, timeout=1)
            if r.status_code == 200:
                return Response(content=r.content, media_type=r.headers.get('Content-Type'))
        except Exception:
            continue
    raise HTTPException(status_code=404, detail="Image not found on any available backend")

@app.get("/api/download-output")
def download_output(url: str, name: str = ""):
    path = output_file_from_url(url)
    if not path:
        raise HTTPException(status_code=404, detail="文件不存在")
    filename = os.path.basename(name) if name else os.path.basename(path)
    return FileResponse(path, media_type=content_type_for_path(path), filename=filename)

@app.post("/api/upload")
async def upload_image(files: List[UploadFile] = File(...)):
    uploaded_files = []
    files_content = []
    for file in files:
        content = await file.read()
        files_content.append((file, content))

    for file, content in files_content:
        success_count = 0
        last_result = None
        for addr in COMFYUI_INSTANCES:
            try:
                files_data = {'image': (file.filename, content, file.content_type)}
                response = requests.post(comfy_url(addr, "/upload/image"), files=files_data, timeout=5)
                if response.status_code == 200:
                    last_result = response.json()
                    success_count += 1
            except Exception as e:
                print(f"Upload error for {addr}: {e}")

        if success_count > 0 and last_result:
            uploaded_files.append({"comfy_name": last_result.get("name", file.filename)})
        else:
            raise HTTPException(status_code=500, detail="Failed to upload to any backend")

    return {"files": uploaded_files}

@app.post("/api/ai/upload")
async def upload_ai_reference(files: List[UploadFile] = File(...)):
    uploaded = []
    for file in files:
        content = await file.read()
        if not content:
            continue
        ext = os.path.splitext(file.filename or "")[1].lower()
        if ext not in [".png", ".jpg", ".jpeg", ".webp"]:
            content_type = (file.content_type or "").lower()
            ext = ".jpg" if "jpeg" in content_type else ".webp" if "webp" in content_type else ".png"
        filename = f"ai_ref_{uuid.uuid4().hex[:12]}{ext}"
        path = os.path.join(OUTPUT_DIR, filename)
        with open(path, "wb") as f:
            f.write(content)
        uploaded.append({"url": f"/output/{filename}", "name": file.filename or filename})
    return {"files": uploaded}

@app.get("/api/assets")
async def list_assets():
    with ASSET_LIBRARY_LOCK:
        return load_asset_library()

@app.post("/api/assets/upload")
async def upload_assets(files: List[UploadFile] = File(...)):
    uploaded = []
    with ASSET_LIBRARY_LOCK:
        for file in files:
            content = await file.read()
            if not content:
                continue
            filename = f"{uuid.uuid4().hex[:10]}_{safe_asset_name(file.filename or 'asset.png')}"
            path = os.path.join(ASSET_LIBRARY_DIR, filename)
            with open(path, "wb") as f:
                f.write(content)
            remote_url = await maybe_upload_asset_remote(path)
            uploaded.append(add_asset_record(asset_url_for(filename), file.filename or filename, remote_url))
    return {"items": uploaded}

@app.post("/api/assets/from-output")
async def create_asset_from_output(payload: AssetCreateRequest):
    src = output_file_from_url(payload.url)
    if not src:
        raise HTTPException(status_code=404, detail="找不到要保存的图片")
    filename = f"{uuid.uuid4().hex[:10]}_{safe_asset_name(payload.name or os.path.basename(src))}"
    target = os.path.join(ASSET_LIBRARY_DIR, filename)
    shutil.copyfile(src, target)
    remote_url = await maybe_upload_asset_remote(target)
    with ASSET_LIBRARY_LOCK:
        item = add_asset_record(asset_url_for(filename), payload.name or filename, remote_url, source_url=payload.url)
    return {"item": item}

@app.post("/api/assets/folders")
async def create_asset_folder(payload: AssetFolderCreateRequest):
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="文件夹名称不能为空")
    with ASSET_LIBRARY_LOCK:
        library = load_asset_library()
        folder = {"id": uuid.uuid4().hex, "name": name[:80], "created_at": now_ms()}
        library.setdefault("folders", []).append(folder)
        save_asset_library(library)
    return {"folder": folder}

@app.post("/api/assets/folders/{folder_id}/rename")
async def rename_asset_folder(folder_id: str, payload: AssetFolderRenameRequest):
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="名称不能为空")
    with ASSET_LIBRARY_LOCK:
        library = load_asset_library()
        folder = next((f for f in library.get("folders", []) if f.get("id") == folder_id), None)
        if not folder:
            raise HTTPException(status_code=404, detail="文件夹不存在")
        folder["name"] = name[:80]
        save_asset_library(library)
    return {"folder": folder}

@app.delete("/api/assets/folders/{folder_id}")
async def delete_asset_folder(folder_id: str):
    with ASSET_LIBRARY_LOCK:
        library = load_asset_library()
        deleted, unfiled = delete_asset_folder_from_library(library, folder_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="文件夹不存在")
        save_asset_library(library)
    return {"ok": True, "deleted": deleted, "unfiled": unfiled}

@app.post("/api/assets/move")
async def move_assets(payload: AssetMoveRequest):
    ids = {asset_id for asset_id in payload.ids if asset_id}
    folder_id = payload.folder_id or ""
    with ASSET_LIBRARY_LOCK:
        library = load_asset_library()
        if folder_id and not any(folder.get("id") == folder_id for folder in library.get("folders", [])):
            raise HTTPException(status_code=404, detail="文件夹不存在")
        moved = 0
        for item in library.get("items", []):
            if item.get("id") in ids:
                item["folder_id"] = folder_id
                moved += 1
        save_asset_library(library)
    return {"ok": True, "moved": moved}

@app.post("/api/assets/duplicate")
async def duplicate_asset(payload: AssetDuplicateRequest):
    with ASSET_LIBRARY_LOCK:
        library = load_asset_library()
        src = next((it for it in library.get("items", []) if it.get("id") == payload.id), None)
        if not src:
            raise HTTPException(status_code=404, detail="资产不存在")
        folder_id = payload.folder_id or ""
        if folder_id and not any(f.get("id") == folder_id for f in library.get("folders", [])):
            folder_id = ""
        src_url = src.get("url", "")
        src_remote = src.get("remote_url", "")
        src_name = src.get("name") or "asset"
    # 取原图字节（本地优先，其次远程），生成独立副本，避免删副本时误删原图/历史
    data = None
    local = output_file_from_url(src_url) or output_file_from_url(src.get("source_url", ""))
    if local:
        try:
            with open(local, "rb") as fh:
                data = fh.read()
        except OSError:
            data = None
    if data is None:
        fetch_url = src_remote or (src_url if src_url.startswith("http://") or src_url.startswith("https://") else "")
        if fetch_url:
            try:
                async with httpx.AsyncClient(timeout=AI_REQUEST_TIMEOUT) as client:
                    resp = await client.get(fetch_url)
                    resp.raise_for_status()
                    data = resp.content
            except Exception as e:
                print(f"复制资产取图失败: {e}")
                data = None
    new_name = f"{src_name} 副本"
    if data is not None:
        filename = f"{uuid.uuid4().hex[:10]}_{safe_asset_name(src_name)}"
        target = os.path.join(ASSET_LIBRARY_DIR, filename)
        with open(target, "wb") as fh:
            fh.write(data)
        remote_url = await maybe_upload_asset_remote(target)
        if IMAGE_HOST_STRATEGY == "remote" and remote_url:
            new_url = remote_url
            try:
                os.remove(target)
            except OSError:
                pass
        else:
            new_url = asset_url_for(filename)
    else:
        # 兜底：拿不到字节时克隆记录（与原图共享 URL）
        new_url = src_url
        remote_url = src_remote
    item = {
        "id": uuid.uuid4().hex,
        "name": new_name,
        "url": new_url,
        "remote_url": remote_url,
        "source_url": "",
        "folder_id": folder_id,
        "created_at": now_ms(),
    }
    with ASSET_LIBRARY_LOCK:
        library = load_asset_library()
        library.setdefault("items", []).append(item)
        save_asset_library(library)
    return {"item": item}

@app.post("/api/assets/rename")
async def rename_asset(payload: AssetRenameRequest):
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="名称不能为空")
    with ASSET_LIBRARY_LOCK:
        library = load_asset_library()
        item = next((it for it in library.get("items", []) if it.get("id") == payload.id), None)
        if not item:
            raise HTTPException(status_code=404, detail="资产不存在")
        item["name"] = name[:120]
        save_asset_library(library)
    return {"item": item}

@app.post("/api/assets/ensure")
async def ensure_asset(payload: AssetEnsureRequest):
    url = (payload.url or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="缺少 url")
    # 已在库中（按 url / remote_url / source_url 匹配）直接返回
    with ASSET_LIBRARY_LOCK:
        library = load_asset_library()
        found = next((it for it in library.get("items", [])
                      if it.get("url") == url or it.get("remote_url") == url or it.get("source_url") == url), None)
    if found:
        return {"item": found, "created": False}
    # 取字节，落库为独立资产
    data = None
    ext = ".png"
    local = output_file_from_url(url)
    if local:
        try:
            with open(local, "rb") as fh:
                data = fh.read()
            ext = os.path.splitext(local)[1] or ".png"
        except OSError:
            data = None
    elif url.startswith("data:"):
        try:
            header, b64 = url.split(",", 1)
            data = base64.b64decode(b64)
            if "jpeg" in header or "jpg" in header:
                ext = ".jpg"
            elif "webp" in header:
                ext = ".webp"
            elif "gif" in header:
                ext = ".gif"
        except Exception as e:
            print(f"ensure 解析 data URL 失败: {e}")
            data = None
    elif url.startswith("http://") or url.startswith("https://"):
        try:
            async with httpx.AsyncClient(timeout=AI_REQUEST_TIMEOUT) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.content
                ct = resp.headers.get("Content-Type", "")
                if "jpeg" in ct or "jpg" in ct:
                    ext = ".jpg"
                elif "webp" in ct:
                    ext = ".webp"
                elif "png" in ct:
                    ext = ".png"
        except Exception as e:
            print(f"ensure 拉取远程图片失败: {e}")
            data = None
    if data is None:
        # 兜底：直接以该 url 建记录（可能是外链），不落盘
        with ASSET_LIBRARY_LOCK:
            item = add_asset_record(url, payload.name or os.path.basename(url) or "asset", source_url=url)
        return {"item": item, "created": True}
    base = safe_asset_name(payload.name or "asset")
    filename = f"{uuid.uuid4().hex[:10]}_{base}{ext if not base.endswith(ext) else ''}"
    target = os.path.join(ASSET_LIBRARY_DIR, filename)
    with open(target, "wb") as fh:
        fh.write(data)
    remote_url = await maybe_upload_asset_remote(target)
    if IMAGE_HOST_STRATEGY == "remote" and remote_url:
        new_url = remote_url
        try:
            os.remove(target)
        except OSError:
            pass
    else:
        new_url = asset_url_for(filename)
    item = {
        "id": uuid.uuid4().hex,
        "name": payload.name or base,
        "url": new_url,
        "remote_url": remote_url,
        "source_url": url,
        "folder_id": "",
        "created_at": now_ms(),
    }
    with ASSET_LIBRARY_LOCK:
        library = load_asset_library()
        library.setdefault("items", []).append(item)
        save_asset_library(library)
    return {"item": item, "created": True}

@app.post("/api/assets/bulk-delete")
async def bulk_delete_assets(payload: AssetBulkDeleteRequest):
    removed = remove_asset_records(payload.ids)
    removed_history = 0
    for item in removed:
        removed_history += delete_history_records_for_asset(item)
        delete_asset_file(item)
    return {"ok": True, "deleted": len(removed), "history_deleted": removed_history}

@app.delete("/api/assets/{asset_id}")
async def delete_asset(asset_id: str):
    removed = remove_asset_records([asset_id])
    removed_history = 0
    for item in removed:
        removed_history += delete_history_records_for_asset(item)
        delete_asset_file(item)
    return {"ok": True, "deleted": len(removed), "history_deleted": removed_history}

@app.post("/api/assets/upload-to-comfy")
async def upload_asset_to_comfy(payload: AssetToComfyRequest):
    path = output_file_from_url(payload.url)
    if not path:
        raise HTTPException(status_code=404, detail="资产文件不存在")
    content = open(path, "rb").read()
    name = safe_asset_name(payload.name or os.path.basename(path), fallback=os.path.basename(path))
    success_count = 0
    last_result = None
    last_error = ""
    for addr in COMFYUI_INSTANCES:
        try:
            files_data = {"image": (name, content, content_type_for_path(path))}
            response = requests.post(comfy_url(addr, "/upload/image"), files=files_data, timeout=5)
            if response.status_code == 200:
                last_result = response.json()
                success_count += 1
            else:
                last_error = response.text[:300]
        except Exception as e:
            last_error = str(e)
            print(f"Asset upload error for {addr}: {e}")
    if success_count > 0 and last_result:
        return {"comfy_name": last_result.get("name", name), "url": payload.url, "name": name}
    detail = "资产上传到 ComfyUI 失败"
    if last_error:
        detail = f"{detail}：{last_error}"
    raise HTTPException(status_code=500, detail=detail)

@app.get("/api/prompt-library")
async def get_prompt_library():
    with PROMPT_LIBRARY_LOCK:
        library = load_prompt_library()
        source = library.get("promptdexter") or {}
        cached_catalog_index = source.get("embedded_catalog_index") if isinstance(source.get("embedded_catalog_index"), dict) else {}
        catalog_index_current = int(source.get("embedded_catalog_index_version") or 0) >= PROMPTDEXTER_CATALOG_INDEX_VERSION
        cached_link_index = source.get("category_link_index") if isinstance(source.get("category_link_index"), dict) else {}
        if cached_catalog_index and catalog_index_current:
            if reclassify_cached_promptdexter_items(library, cached_link_index, cached_catalog_index):
                library = save_prompt_library(library)
            return {"library": library}
        if cached_link_index and not source.get("items"):
            if reclassify_cached_promptdexter_items(library, cached_link_index):
                library = save_prompt_library(library)
            return {"library": library}
        needs_index = bool(source.get("items"))
    fetched_categories = None
    fetched_link_index = None
    fetched_catalog_index = None
    if needs_index:
        try:
            fetched_categories = promptdexter_fetch_categories()
            fetched_catalog_index = promptdexter_fetch_embedded_catalog_index(fetched_categories)
            fetched_link_index = promptdexter_category_link_index(fetched_categories)
        except Exception as e:
            print(f"PromptDexter 默认分类同步失败: {e}")
    with PROMPT_LIBRARY_LOCK:
        library = load_prompt_library()
        source = library.get("promptdexter") or {}
        if fetched_categories is not None and fetched_catalog_index is not None:
            source["categories"] = fetched_categories
            source["embedded_catalog_index"] = promptdexter_compact_catalog_index(fetched_catalog_index)
            source["embedded_catalog_index_at"] = now_ms()
            source["embedded_catalog_index_version"] = PROMPTDEXTER_CATALOG_INDEX_VERSION
        if fetched_categories is not None and fetched_link_index is not None:
            source["categories"] = fetched_categories
            source["category_link_index"] = fetched_link_index
            source["category_link_index_at"] = now_ms()
        catalog_index = source.get("embedded_catalog_index") if isinstance(source.get("embedded_catalog_index"), dict) else {}
        link_index = source.get("category_link_index") if isinstance(source.get("category_link_index"), dict) else {}
        if reclassify_cached_promptdexter_items(library, link_index, catalog_index):
            library = save_prompt_library(library)
        elif fetched_categories is not None and (fetched_link_index is not None or fetched_catalog_index is not None):
            library = save_prompt_library(library)
        return {"library": library}

@app.post("/api/promptdexter/sync")
async def sync_promptdexter(payload: PromptDexterSyncRequest):
    try:
        return sync_promptdexter_library(payload.limit, payload.fetch_details, payload.force, payload.category_id)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"PromptDexter 同步失败：{str(e)[:300]}")
    except ET.ParseError:
        raise HTTPException(status_code=502, detail="PromptDexter 站点索引解析失败")

@app.post("/api/promptdexter/items/{item_id}/refresh")
async def refresh_promptdexter_item(item_id: str):
    categories = promptdexter_fetch_categories()
    with PROMPT_LIBRARY_LOCK:
        library = load_prompt_library()
        item = find_promptdexter_item(library, item_id)
        if not item:
            raise HTTPException(status_code=404, detail="提示词不存在")
    try:
        update_promptdexter_item_detail(item, categories)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"PromptDexter 详情同步失败：{str(e)[:300]}")
    with PROMPT_LIBRARY_LOCK:
        library = load_prompt_library()
        source = library["promptdexter"]
        source["categories"] = categories
        for index, current in enumerate(source.get("items", [])):
            if current.get("id") == item_id:
                source["items"][index] = item
                break
        library = save_prompt_library(library)
    return {"library": library, "item": item}

@app.patch("/api/promptdexter/items/{item_id}/prompt")
async def update_promptdexter_prompt(item_id: str, payload: PromptDexterPromptUpdateRequest):
    try:
        with PROMPT_LIBRARY_LOCK:
            library = load_prompt_library()
            item = update_promptdexter_prompt_in_library(library, item_id, payload.prompt)
            if not item:
                raise HTTPException(status_code=404, detail="提示词不存在")
            library = save_prompt_library(library)
        return {"library": library, "item": item}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/prompt-library/presets")
async def create_prompt_preset(payload: PromptPresetCreateRequest):
    with PROMPT_LIBRARY_LOCK:
        library = load_prompt_library()
        source_item = find_promptdexter_item(library, payload.source_item_id) if payload.source_item_id else None
        if source_item:
            item = {
                "id": "preset_" + uuid.uuid4().hex[:12],
                "source": "mine",
                "source_item_id": source_item.get("id", ""),
                "title": source_item.get("title") or "我的提示词",
                "description": source_item.get("description") or "",
                "prompt": source_item.get("prompt") or "",
                "image_url": source_item.get("image_url") or "",
                "categories": [next((cat.get("name") for cat in library["promptdexter"].get("categories", []) if cat.get("id") == cid), cid) for cid in source_item.get("categories", [])],
                "tags": [tag.get("name") for tag in source_item.get("tags", []) if tag.get("name")],
                "created_at": now_ms(),
                "updated_at": now_ms(),
            }
        else:
            item = prompt_preset_from_payload(payload.dict())
        if not item.get("prompt"):
            raise HTTPException(status_code=400, detail="提示词正文不能为空")
        library["my_presets"].setdefault("items", []).insert(0, item)
        library = save_prompt_library(library)
    return {"library": library, "item": item}

@app.patch("/api/prompt-library/presets/{item_id}")
async def update_prompt_preset(item_id: str, payload: PromptPresetUpdateRequest):
    with PROMPT_LIBRARY_LOCK:
        library = load_prompt_library()
        items = library["my_presets"].setdefault("items", [])
        for index, item in enumerate(items):
            if item.get("id") == item_id:
                items[index] = prompt_preset_from_payload(payload.dict(), item)
                library = save_prompt_library(library)
                return {"library": library, "item": items[index]}
    raise HTTPException(status_code=404, detail="提示词预设不存在")

@app.delete("/api/prompt-library/presets/{item_id}")
async def delete_prompt_preset(item_id: str):
    with PROMPT_LIBRARY_LOCK:
        library = load_prompt_library()
        items = library["my_presets"].setdefault("items", [])
        kept = [item for item in items if item.get("id") != item_id]
        if len(kept) == len(items):
            raise HTTPException(status_code=404, detail="提示词预设不存在")
        library["my_presets"]["items"] = kept
        library = save_prompt_library(library)
    return {"library": library, "deleted": 1}

@app.get("/api/config")
async def ai_config():
    preferred_chat_model = CHAT_MODELS[0] if CHAT_MODELS else CHAT_MODEL
    return {
        "base_url": IMAGE_API_BASE_URL,
        "image_api_base_url": IMAGE_API_BASE_URL,
        "chat_api_base_url": CHAT_API_BASE_URL,
        "chat_model": preferred_chat_model,
        "image_model": IMAGE_MODEL,
        "chat_models": CHAT_MODELS,
        "chat_api_endpoints": public_chat_endpoints(),
        "image_models": IMAGE_MODELS,
        "has_api_key": bool(IMAGE_API_KEY),
        "has_image_api_key": bool(IMAGE_API_KEY),
        "has_chat_api_key": bool(CHAT_API_KEY),
        "ms_chat_models": MODELSCOPE_CHAT_MODELS,
        "has_ms_key": bool(MODELSCOPE_API_KEY),
        "has_comfy_org_auth": bool(COMFY_ORG_API_KEY or COMFY_ORG_AUTH_TOKEN or COMFY_ORG_AUTH_STATE.get("id_token") or (COMFY_ORG_EMAIL and COMFY_ORG_PASSWORD) or COMFY_ORG_REFRESH_TOKEN),
        "has_comfy_org_api_key": bool(COMFY_ORG_API_KEY),
        "comfy_org_email": COMFY_ORG_EMAIL,
        "comfyui_instances": ",".join(COMFYUI_INSTANCES),
        "image_host_base_url": IMAGE_HOST_BASE_URL,
        "image_host_username": IMAGE_HOST_USERNAME,
        "image_host_strategy": IMAGE_HOST_STRATEGY,
        "has_image_host_password": bool(IMAGE_HOST_PASSWORD),
        "has_image_host_token": bool(IMAGE_HOST_TOKEN),
    }

@app.post("/api/settings")
async def save_settings(payload: SettingsRequest):
    existing_endpoints = {item.get("id"): item for item in CHAT_API_ENDPOINTS if item.get("id")}
    endpoint_source = payload.chat_api_endpoints or [{
        "id": "default",
        "name": "默认对话 API",
        "base_url": payload.chat_api_base_url,
        "api_key": payload.chat_api_key,
        "models": payload.chat_api_models,
        "default_model": payload.chat_api_default_model,
    }]
    normalized_endpoints = [
        endpoint for index, item in enumerate(endpoint_source)
        if (endpoint := normalize_chat_endpoint(item, index, existing_endpoints))
    ]
    first_chat_endpoint = normalized_endpoints[0] if normalized_endpoints else {
        "base_url": payload.chat_api_base_url,
        "api_key": payload.chat_api_key,
        "models": split_csv(payload.chat_api_models),
        "default_model": payload.chat_api_default_model,
    }
    updates = {
        "COMFYUI_INSTANCES": payload.comfyui_instances,
        "COMFY_ORG_EMAIL": payload.comfy_org_email,
        "IMAGE_API_BASE_URL": payload.image_api_base_url,
        "IMAGE_API_MODELS": payload.image_api_models,
        "IMAGE_API_DEFAULT_MODEL": payload.image_api_default_model,
        "CHAT_API_BASE_URL": first_chat_endpoint.get("base_url", ""),
        "CHAT_API_MODELS": ",".join(first_chat_endpoint.get("models") or []),
        "CHAT_API_DEFAULT_MODEL": first_chat_endpoint.get("default_model", ""),
        "CHAT_API_ENDPOINTS": json.dumps(normalized_endpoints, ensure_ascii=False),
        "IMAGE_HOST_BASE_URL": payload.image_host_base_url,
        "IMAGE_HOST_USERNAME": payload.image_host_username,
        "IMAGE_HOST_STRATEGY": payload.image_host_strategy or "local",
    }
    secret_updates = {
        "COMFY_ORG_API_KEY": payload.comfy_org_api_key,
        "IMAGE_API_KEY": payload.image_api_key,
        "CHAT_API_KEY": first_chat_endpoint.get("api_key", ""),
        "IMAGE_HOST_PASSWORD": payload.image_host_password,
        "IMAGE_HOST_TOKEN": payload.image_host_token,
    }
    updates.update({key: value for key, value in secret_updates.items() if value})
    update_env_values(updates)
    reload_runtime_config()
    return {"ok": True, "config": await ai_config()}

@app.post("/api/settings/test-chat")
async def test_chat_settings():
    base, headers, model = resolve_chat_provider("chat", CHAT_MODEL, "")
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(base + "/chat/completions", headers=headers, json={"model": model, "messages": [{"role": "user", "content": "ping"}], "max_tokens": 8})
        response.raise_for_status()
    return {"ok": True, "model": model}

@app.post("/api/settings/test-image-host")
async def test_image_host():
    if not IMAGE_HOST_BASE_URL:
        raise HTTPException(status_code=400, detail="未配置图床地址")
    return {"ok": True, "configured": bool(IMAGE_HOST_TOKEN or (IMAGE_HOST_USERNAME and IMAGE_HOST_PASSWORD))}

@app.get("/api/models")
async def ai_models():
    return {"chat_models": CHAT_MODELS, "image_models": IMAGE_MODELS}

# --- ModelScope Token (从 env 读取，不再支持通过 UI 修改) ---

@app.get("/api/config/token")
async def get_global_token():
    # 优先读 env，回退到 global_config.json（兼容旧数据）
    if MODELSCOPE_API_KEY:
        return {"token": MODELSCOPE_API_KEY}
    if os.path.exists(GLOBAL_CONFIG_FILE):
        try:
            with open(GLOBAL_CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return {"token": config.get("modelscope_token", "")}
        except:
            pass
    return {"token": ""}

@app.get("/api/comfy-auth/status")
async def comfy_auth_status():
    return {
        "authenticated": bool(COMFY_ORG_API_KEY or COMFY_ORG_AUTH_TOKEN or COMFY_ORG_AUTH_STATE.get("id_token") or COMFY_ORG_REFRESH_TOKEN),
        "has_api_key": bool(COMFY_ORG_API_KEY),
        "has_token": bool(COMFY_ORG_AUTH_TOKEN or COMFY_ORG_AUTH_STATE.get("id_token") or COMFY_ORG_REFRESH_TOKEN),
        "has_env_login": bool(COMFY_ORG_EMAIL and COMFY_ORG_PASSWORD),
    }

@app.post("/api/comfy-auth/login")
async def comfy_auth_login(payload: ComfyLoginRequest):
    global COMFY_ORG_API_KEY
    api_key = (payload.api_key or "").strip()
    email = (payload.email or "").strip()
    password = payload.password or ""
    if api_key:
        COMFY_ORG_API_KEY = api_key
        return {"success": True, "mode": "api_key"}
    if not email or not password:
        raise HTTPException(status_code=400, detail="请输入 ComfyUI 登录邮箱和密码，或填写 Comfy 登录密钥。")
    try:
        with COMFY_ORG_AUTH_LOCK:
            login_comfy_org(email, password)
        return {"success": True, "mode": "password"}
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

# --- 在线生图 (独立出图 API) ---

@app.post("/api/online-image")
async def online_image(payload: OnlineImageRequest):
    model = selected_model(payload.model, IMAGE_MODEL)
    quality = payload.quality if payload.quality in {"low", "medium", "high", "auto"} else "auto"
    size = payload.size or "1024x1024"
    try:
        image_data, raw = await generate_ai_image(payload.prompt, size, quality, model, [ref.dict() for ref in payload.reference_images])
    except HTTPException:
        raise
    except httpx.HTTPStatusError as exc:
        detail = ""
        try:
            detail = exc.response.text
        except Exception:
            detail = str(exc)
        raise HTTPException(status_code=exc.response.status_code, detail=f"出图接口错误（{model}）：{detail}") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"请求出图接口失败（{model}）：{exc}") from exc
    local_url = await save_ai_image_to_output(image_data, prefix="online_")
    is_edit = bool(payload.reference_images)
    result = {
        "images": [local_url],
        "timestamp": time.time(),
        "prompt": payload.prompt,
        "type": "image_edit" if is_edit else "online",
        "model": model,
        "provider": "image_api",
        "reference_images": [ref.url for ref in payload.reference_images if ref.url],
        "raw_usage": raw.get("usage") if isinstance(raw, dict) else None,
    }
    result["params"] = {
        "provider": "image_api",
        "model": result["model"],
        "size": payload.size,
        "quality": payload.quality,
        "aspect_ratio": payload.aspect_ratio,
        "resolution": payload.resolution,
        "thinking_level": payload.thinking_level,
    }
    save_to_history(result)
    return result

# --- Canvas LLM ---

@app.post("/api/canvas-llm")
async def canvas_llm(payload: CanvasLLMRequest):
    chat_base, chat_hdrs, model = resolve_chat_provider(payload.provider, payload.model, payload.ms_model, payload.chat_provider)
    upstream_messages = [{"role": "system", "content": payload.system_prompt or SYSTEM_PROMPT}]
    for item in limited_chat_history(payload.messages):
        role = item.get("role")
        content = item.get("content")
        if role in {"user", "assistant"} and content:
            upstream_messages.append({"role": role, "content": content})
    upstream_messages.append({"role": "user", "content": payload.message})
    try:
        async with httpx.AsyncClient(timeout=AI_REQUEST_TIMEOUT) as client:
            response = await client.post(
                f"{chat_base}/chat/completions",
                headers=chat_hdrs,
                json={"model": model, "messages": upstream_messages},
            )
            response.raise_for_status()
            if not response.content:
                raise HTTPException(status_code=502, detail="上游接口返回了空响应")
            raw = response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"上游接口错误：{exc.response.text}") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"请求上游接口失败：{exc}") from exc
    text = text_from_chat_response(raw).strip() or "接口返回了空回复。"
    return {"text": text, "model": model, "raw_usage": raw.get("usage") if isinstance(raw, dict) else None}

# --- 对话管理 ---

@app.get("/api/conversations")
async def conversations(request: Request, x_user_id: str = Header(default="")):
    user_id = safe_user_id(x_user_id, request)
    return {"user_id": user_id, "conversations": list_conversations(user_id)}

@app.post("/api/conversations")
async def create_conversation(payload: ConversationCreateRequest, request: Request, x_user_id: str = Header(default="")):
    user_id = safe_user_id(x_user_id, request)
    return {"conversation": new_conversation(user_id, payload.title)}

@app.get("/api/conversations/{conversation_id}")
async def get_conversation(conversation_id: str, request: Request, x_user_id: str = Header(default="")):
    user_id = safe_user_id(x_user_id, request)
    return {"conversation": load_conversation(user_id, conversation_id)}

@app.delete("/api/conversations/{conversation_id}")
async def delete_conversation(conversation_id: str, request: Request, x_user_id: str = Header(default="")):
    user_id = safe_user_id(x_user_id, request)
    path = conversation_path(user_id, conversation_id)
    if os.path.exists(path):
        os.remove(path)
    return {"ok": True}

# --- 画布管理 ---

def load_cowart_state(path: str, fallback: Any):
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return fallback

def save_cowart_state(path: str, payload: Any):
    temp_path = f"{path}.tmp"
    with COWART_STATE_LOCK:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(temp_path, path)

@app.get("/api/canvas")
async def get_cowart_canvas():
    return {
        "snapshot": load_cowart_state(COWART_CANVAS_FILE, None),
        "storage": "infinite-canvas",
    }

@app.put("/api/canvas")
async def save_cowart_canvas(snapshot: Dict[str, Any]):
    if not isinstance(snapshot.get("store"), dict) or not isinstance(snapshot.get("schema"), dict):
        raise HTTPException(status_code=400, detail="Expected a tldraw store snapshot")
    save_cowart_state(COWART_CANVAS_FILE, snapshot)
    return {"ok": True, "storage": "infinite-canvas"}

@app.put("/api/selection")
async def save_cowart_selection(selection: Dict[str, Any]):
    save_cowart_state(COWART_SELECTION_FILE, selection)
    return {"ok": True}

@app.get("/api/view-state")
async def get_cowart_view_state():
    return {
        "viewState": load_cowart_state(COWART_VIEW_STATE_FILE, {
            "version": 1,
            "currentPageId": None,
            "camera": {"x": 0, "y": 0, "z": 1},
            "updatedAt": None,
        })
    }

@app.put("/api/view-state")
async def save_cowart_view_state(view_state: Dict[str, Any]):
    save_cowart_state(COWART_VIEW_STATE_FILE, view_state)
    return {"ok": True}

@app.get("/api/canvases")
async def canvases():
    return {"canvases": list_canvases()}

@app.get("/api/canvases/trash")
async def trashed_canvases():
    return {"canvases": list_deleted_canvases(), "retention_days": 30}

@app.post("/api/canvases")
async def create_canvas(payload: CanvasCreateRequest):
    return {"canvas": new_canvas(payload.title, payload.icon)}

@app.get("/api/canvases/{canvas_id}")
async def get_canvas(canvas_id: str):
    return {"canvas": load_canvas(canvas_id)}

@app.put("/api/canvases/{canvas_id}")
async def update_canvas(canvas_id: str, payload: CanvasSaveRequest):
    canvas = load_canvas(canvas_id)
    canvas["title"] = (payload.title or canvas.get("title") or "未命名画布")[:80]
    canvas["icon"] = (payload.icon or canvas.get("icon") or "layers")[:32]
    canvas["nodes"] = payload.nodes
    canvas["connections"] = payload.connections
    canvas["viewport"] = payload.viewport
    save_canvas(canvas)
    return {"canvas": canvas}

@app.delete("/api/canvases/{canvas_id}")
async def delete_canvas(canvas_id: str):
    canvas = load_canvas_any(canvas_id)
    if not canvas.get("deleted_at"):
        canvas["deleted_at"] = now_ms()
        save_canvas(canvas)
    return {"ok": True}

@app.post("/api/canvases/{canvas_id}/restore")
async def restore_canvas(canvas_id: str):
    canvas = load_canvas_any(canvas_id)
    if canvas.get("deleted_at"):
        canvas.pop("deleted_at", None)
        save_canvas(canvas)
    return {"canvas": canvas}

@app.delete("/api/canvases/{canvas_id}/purge")
async def purge_canvas(canvas_id: str):
    path = canvas_path(canvas_id)
    if os.path.exists(path):
        os.remove(path)
    return {"ok": True}

# --- GPT 对话 ---

@app.post("/api/chat")
async def chat(payload: ChatRequest, request: Request, x_user_id: str = Header(default="")):
    user_id = safe_user_id(x_user_id, request)
    conversation = (
        load_conversation(user_id, payload.conversation_id)
        if payload.conversation_id
        else new_conversation(user_id, display_title(payload.message))
    )
    if not conversation.get("messages"):
        conversation["title"] = display_title(payload.message)

    refs = [ref.dict() for ref in payload.reference_images if ref.url]
    user_message = {
        "id": uuid.uuid4().hex,
        "role": "user",
        "content": payload.message,
        "created_at": now_ms(),
        "attachments": refs,
        "mode": payload.mode,
    }
    conversation["messages"].append(user_message)
    conversation["updated_at"] = now_ms()
    save_conversation(user_id, conversation)

    if payload.mode == "image":
        model = selected_model(payload.image_model or payload.model, IMAGE_MODEL)
        try:
            image_data, raw = await generate_ai_image(payload.message, payload.size, payload.quality, model, refs)
            local_url = await save_ai_image_to_output(image_data, prefix="chat_")
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=f"上游生图接口错误：{exc.response.text}") from exc
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"请求上游生图接口失败：{exc}") from exc
        assistant_message = {
            "id": uuid.uuid4().hex,
            "role": "assistant",
            "type": "image",
            "content": payload.message,
            "image_url": local_url,
            "created_at": now_ms(),
            "model": model,
            "raw_usage": raw.get("usage") if isinstance(raw, dict) else None,
        }
    else:
        chat_base, chat_hdrs, model = resolve_chat_provider(payload.provider, payload.model, payload.ms_model, payload.chat_provider)
        history = conversation["messages"][:-1]
        upstream_messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        for item in limited_chat_history(history):
            msg = upstream_message_from_record(item)
            if msg:
                upstream_messages.append(msg)
        msg = upstream_message_from_record(user_message)
        if msg:
            upstream_messages.append(msg)
        try:
            async with httpx.AsyncClient(timeout=AI_REQUEST_TIMEOUT) as client:
                response = await client.post(
                    f"{chat_base}/chat/completions",
                    headers=chat_hdrs,
                    json=chat_completion_payload(model, upstream_messages, payload.reasoning_enabled, payload.reasoning_effort),
                )
                response.raise_for_status()
                raw = response.json()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=f"上游接口错误：{exc.response.text}") from exc
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"请求上游接口失败：{exc}") from exc
        assistant_message = {
            "id": uuid.uuid4().hex,
            "role": "assistant",
            "content": text_from_chat_response(raw).strip() or "接口返回了空回复。",
            "created_at": now_ms(),
            "model": model,
            "raw_usage": raw.get("usage") if isinstance(raw, dict) else None,
        }
        reasoning = reasoning_from_chat_response(raw).strip() if payload.reasoning_enabled else ""
        if not reasoning:
            tagged_reasoning, content = split_reasoning_tags(assistant_message["content"])
            if content:
                assistant_message["content"] = content
            if payload.reasoning_enabled:
                reasoning = tagged_reasoning
        if reasoning:
            assistant_message["reasoning"] = reasoning

    conversation["messages"].append(assistant_message)
    conversation["updated_at"] = now_ms()
    save_conversation(user_id, conversation)
    return {"conversation": conversation, "message": assistant_message}

@app.post("/api/chat/stream")
async def chat_stream(payload: ChatRequest, request: Request, x_user_id: str = Header(default="")):
    if payload.mode == "image":
        raise HTTPException(status_code=400, detail="图片模式请使用 /api/chat")

    user_id = safe_user_id(x_user_id, request)
    conversation = (
        load_conversation(user_id, payload.conversation_id)
        if payload.conversation_id
        else new_conversation(user_id, display_title(payload.message))
    )
    if not conversation.get("messages"):
        conversation["title"] = display_title(payload.message)

    refs = [ref.dict() for ref in payload.reference_images if ref.url]
    user_message = {
        "id": uuid.uuid4().hex,
        "role": "user",
        "content": payload.message,
        "created_at": now_ms(),
        "attachments": refs,
        "mode": payload.mode,
    }
    conversation["messages"].append(user_message)
    conversation["updated_at"] = now_ms()
    save_conversation(user_id, conversation)

    chat_base, chat_hdrs, model = resolve_chat_provider(payload.provider, payload.model, payload.ms_model, payload.chat_provider)
    history = conversation["messages"][:-1]
    upstream_messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for item in limited_chat_history(history):
        msg = upstream_message_from_record(item)
        if msg:
            upstream_messages.append(msg)
    msg = upstream_message_from_record(user_message)
    if msg:
        upstream_messages.append(msg)

    async def stream():
        content_parts = []
        reasoning_parts = []
        raw_usage = None
        yield sse_event({"type": "meta", "conversation": conversation})
        try:
            async with httpx.AsyncClient(timeout=AI_REQUEST_TIMEOUT) as client:
                async with client.stream(
                    "POST",
                    f"{chat_base}/chat/completions",
                    headers=chat_stream_headers(chat_hdrs),
                    json=chat_completion_payload(model, upstream_messages, payload.reasoning_enabled, payload.reasoning_effort, stream=True),
                ) as response:
                    if response.status_code >= 400:
                        detail = await response.aread()
                        yield sse_event({"type": "error", "detail": f"上游接口错误：{detail.decode('utf-8', errors='ignore')}"})
                        return
                    async for line in response.aiter_lines():
                        if not line:
                            continue
                        if line.startswith("data:"):
                            line = line[5:].strip()
                        if line == "[DONE]":
                            break
                        try:
                            chunk = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(chunk, dict) and chunk.get("usage"):
                            raw_usage = chunk.get("usage")
                        reasoning_delta = reasoning_delta_from_chat_chunk(chunk)
                        if payload.reasoning_enabled and reasoning_delta:
                            reasoning_parts.append(reasoning_delta)
                            yield sse_event({"type": "reasoning_delta", "delta": reasoning_delta})
                        delta = text_delta_from_chat_chunk(chunk)
                        if delta:
                            content_parts.append(delta)
                            yield sse_event({"type": "delta", "delta": delta})
        except httpx.HTTPError as exc:
            yield sse_event({"type": "error", "detail": f"请求上游接口失败：{exc}"})
            return

        content = "".join(content_parts).strip()
        reasoning = "".join(reasoning_parts).strip()
        tagged_reasoning, stripped_content = split_reasoning_tags(content)
        if tagged_reasoning:
            reasoning = "\n\n".join(part for part in [reasoning, tagged_reasoning] if part).strip()
            content = stripped_content
        assistant_message = {
            "id": uuid.uuid4().hex,
            "role": "assistant",
            "content": content or "接口返回了空回复。",
            "created_at": now_ms(),
            "model": model,
            "raw_usage": raw_usage,
        }
        if payload.reasoning_enabled and reasoning:
            assistant_message["reasoning"] = reasoning
        conversation["messages"].append(assistant_message)
        conversation["updated_at"] = now_ms()
        save_conversation(user_id, conversation)
        yield sse_event({"type": "done", "conversation": conversation, "message": assistant_message})

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

# --- 历史记录 ---

@app.get("/api/history")
async def get_history_api(type: str = None):
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if type:
                    data = [item for item in data if item.get("type", "zimage") == type]
                data = [item for item in data if item.get("images") and len(item["images"]) > 0]

                def sort_key(item):
                    ts = item.get("timestamp", 0)
                    if isinstance(ts, (int, float)):
                        return float(ts)
                    return 0

                data.sort(key=sort_key, reverse=True)
                return data
        except Exception as e:
            print(f"读取历史文件失败: {e}")
            return []
    return []

@app.get("/api/queue_status")
async def get_queue_status(client_id: str):
    with QUEUE_LOCK:
        total = len(QUEUE)
        positions = [i + 1 for i, t in enumerate(QUEUE) if t["client_id"] == client_id]
        position = positions[0] if positions else 0
    return {"total": total, "position": position}

@app.post("/api/history/delete")
async def delete_history(req: DeleteHistoryRequest):
    if not os.path.exists(HISTORY_FILE):
        return {"success": False, "message": "History file not found"}
    try:
        with HISTORY_LOCK:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                history = json.load(f)
            target_record = None
            new_history = []
            for item in history:
                is_match = False
                item_ts = item.get("timestamp", 0)
                if isinstance(req.timestamp, (int, float)) and isinstance(item_ts, (int, float)):
                    if abs(float(item_ts) - float(req.timestamp)) < 0.001:
                        is_match = True
                elif str(item_ts) == str(req.timestamp):
                    is_match = True
                if is_match:
                    target_record = item
                else:
                    new_history.append(item)
            if target_record:
                with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
                    json.dump(new_history, f, ensure_ascii=False, indent=4)

        if target_record:
            if not req.delete_files:
                return {"success": True}
            for img_url in target_record.get("images", []):
                if img_url.startswith("/output/"):
                    filename = img_url.split("/")[-1]
                    file_path = os.path.join(OUTPUT_DIR, filename)
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except Exception as e:
                            print(f"Failed to delete file {file_path}: {e}")
            return {"success": True}
        else:
            return {"success": False, "message": "Record not found"}
    except Exception as e:
        print(f"Delete history error: {e}")
        return {"success": False, "message": str(e)}

# --- ModelScope 角度控制 ---

@app.post("/api/angle/poll_status")
async def poll_angle_cloud(req: CloudPollRequest):
    base_url = 'https://api-inference.modelscope.cn/'
    clean_token = (req.api_key or MODELSCOPE_API_KEY).strip()
    if not clean_token:
        raise HTTPException(status_code=400, detail="未提供 ModelScope API Key")

    headers = {
        "Authorization": f"Bearer {clean_token}",
        "Content-Type": "application/json",
        "X-ModelScope-Async-Mode": "true"
    }
    task_id = req.task_id
    print(f"Resuming polling for Angle Task: {task_id}")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            for i in range(300):
                await asyncio.sleep(2)
                try:
                    result = await client.get(
                        f"{base_url}v1/tasks/{task_id}",
                        headers={**headers, "X-ModelScope-Task-Type": "image_generation"},
                    )
                    data = result.json()
                    status = data.get("task_status")

                    if status == "SUCCEED":
                        img_url = data["output_images"][0]
                        local_path = ""
                        try:
                            async with httpx.AsyncClient() as dl_client:
                                img_res = await dl_client.get(img_url)
                                if img_res.status_code == 200:
                                    filename = f"cloud_angle_{int(time.time())}.png"
                                    file_path = os.path.join(OUTPUT_DIR, filename)
                                    with open(file_path, "wb") as f:
                                        f.write(img_res.content)
                                    local_path = offload_local_file(file_path, f"/output/{filename}")
                                else:
                                    local_path = img_url
                        except Exception:
                            local_path = img_url

                        record = {"timestamp": time.time(), "prompt": f"Resumed {task_id}", "images": [local_path], "type": "angle", "client_id": req.client_id}
                        save_to_history(record)
                        if req.client_id:
                            await manager.send_personal_message({"type": "cloud_status", "status": "SUCCEED", "task_id": task_id}, req.client_id)
                        return {"url": local_path}

                    elif status == "FAILED":
                        if req.client_id:
                            await manager.send_personal_message({"type": "cloud_status", "status": "FAILED", "task_id": task_id}, req.client_id)
                        raise HTTPException(status_code=502, detail=f"ModelScope task failed: {data}")

                    if i % 5 == 0 and req.client_id:
                        await manager.send_personal_message({
                            "type": "cloud_status", "status": f"{status} ({i}/300)",
                            "task_id": task_id, "progress": i, "total": 300
                        }, req.client_id)

                except HTTPException:
                    raise
                except Exception as loop_e:
                    print(f"Angle polling error: {loop_e}")
                    continue

            if req.client_id:
                await manager.send_personal_message({"type": "cloud_status", "status": "TIMEOUT", "task_id": task_id}, req.client_id)
            return {"status": "timeout", "task_id": task_id, "message": "Task still pending"}

    except Exception as e:
        print(f"Angle polling error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/angle/generate")
async def generate_angle_cloud(req: CloudGenRequest):
    base_url = 'https://api-inference.modelscope.cn/'
    clean_token = (req.api_key or MODELSCOPE_API_KEY).strip()
    if not clean_token:
        raise HTTPException(status_code=400, detail="未提供 ModelScope API Key")

    headers = {
        "Authorization": f"Bearer {clean_token}",
        "Content-Type": "application/json",
        "X-ModelScope-Async-Mode": "true"
    }
    payload = {
        "model": "Qwen/Qwen-Image-Edit-2511",
        "prompt": req.prompt.strip(),
    }
    payload.update(modelscope_edit_image_payload(req.image_urls))

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            submit_res = await client.post(f"{base_url}v1/images/generations", headers=headers, json=payload)
            if submit_res.status_code != 200:
                try:
                    detail = submit_res.json()
                except:
                    detail = submit_res.text
                raise HTTPException(status_code=submit_res.status_code, detail=detail)

            task_id = submit_res.json().get("task_id")
            print(f"Angle Task submitted, ID: {task_id}")

            for i in range(300):
                await asyncio.sleep(2)
                try:
                    result = await client.get(
                        f"{base_url}v1/tasks/{task_id}",
                        headers={**headers, "X-ModelScope-Task-Type": "image_generation"},
                    )
                    data = result.json()
                    status = data.get("task_status")

                    if status == "SUCCEED":
                        img_url = data["output_images"][0]
                        local_path = ""
                        try:
                            async with httpx.AsyncClient() as dl_client:
                                img_res = await dl_client.get(img_url)
                                if img_res.status_code == 200:
                                    filename = f"cloud_angle_{int(time.time())}.png"
                                    file_path = os.path.join(OUTPUT_DIR, filename)
                                    with open(file_path, "wb") as f:
                                        f.write(img_res.content)
                                    local_path = offload_local_file(file_path, f"/output/{filename}")
                                else:
                                    local_path = img_url
                        except Exception:
                            local_path = img_url

                        record = {"timestamp": time.time(), "prompt": req.prompt, "images": [local_path], "type": "angle", "client_id": req.client_id}
                        save_to_history(record)
                        if req.client_id:
                            await manager.send_personal_message({"type": "cloud_status", "status": "SUCCEED", "task_id": task_id}, req.client_id)
                        if GLOBAL_LOOP:
                            asyncio.run_coroutine_threadsafe(manager.broadcast_new_image(record), GLOBAL_LOOP)
                        return {"url": local_path, "task_id": task_id}

                    elif status == "FAILED":
                        if req.client_id:
                            await manager.send_personal_message({"type": "cloud_status", "status": "FAILED", "task_id": task_id}, req.client_id)
                        raise HTTPException(status_code=502, detail=f"ModelScope task failed: {data}")

                    if i % 5 == 0 and req.client_id:
                        await manager.send_personal_message({
                            "type": "cloud_status", "status": f"{status} ({i}/300)",
                            "task_id": task_id, "progress": i, "total": 300
                        }, req.client_id)

                except HTTPException:
                    raise
                except Exception as loop_e:
                    print(f"Angle polling error: {loop_e}")
                    continue

            if req.client_id:
                await manager.send_personal_message({"type": "cloud_status", "status": "TIMEOUT", "task_id": task_id}, req.client_id)
            return {"status": "timeout", "task_id": task_id, "message": "Task still pending"}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Angle generation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# --- ModelScope Z-Image 云端生图 ---

@app.post("/generate")
async def generate_cloud(req: CloudGenRequest):
    base_url = 'https://api-inference.modelscope.cn/'
    clean_token = (req.api_key or MODELSCOPE_API_KEY).strip()
    if not clean_token:
        raise HTTPException(status_code=400, detail="未提供 ModelScope API Key")

    headers = {
        "Authorization": f"Bearer {clean_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "Tongyi-MAI/Z-Image-Turbo",
        "prompt": req.prompt.strip(),
        "size": req.resolution,
        "n": 1
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            submit_res = await client.post(
                f"{base_url}v1/images/generations",
                headers={**headers, "X-ModelScope-Async-Mode": "true"},
                json=payload
            )
            if submit_res.status_code != 200:
                try:
                    detail = submit_res.json()
                except:
                    detail = submit_res.text
                raise HTTPException(status_code=submit_res.status_code, detail=detail)

            task_id = submit_res.json().get("task_id")
            print(f"Z-Image Task submitted, ID: {task_id}")

            for i in range(200):
                await asyncio.sleep(3)
                try:
                    result = await client.get(
                        f"{base_url}v1/tasks/{task_id}",
                        headers={**headers, "X-ModelScope-Task-Type": "image_generation"},
                    )
                    data = result.json()
                    status = data.get("task_status")

                    if i % 5 == 0:
                        print(f"Task {task_id} status check {i}: {status}")

                    if status == "SUCCEED":
                        img_url = data["output_images"][0]
                        local_path = ""
                        try:
                            async with httpx.AsyncClient() as dl_client:
                                img_res = await dl_client.get(img_url)
                                if img_res.status_code == 200:
                                    filename = f"cloud_{int(time.time())}.png"
                                    file_path = os.path.join(OUTPUT_DIR, filename)
                                    with open(file_path, "wb") as f:
                                        f.write(img_res.content)
                                    local_path = offload_local_file(file_path, f"/output/{filename}")
                                else:
                                    local_path = img_url
                        except Exception as dl_e:
                            print(f"Download error: {dl_e}")
                            local_path = img_url

                        record = {"timestamp": time.time(), "prompt": req.prompt, "images": [local_path], "type": "cloud"}
                        save_to_history(record)
                        try:
                            await manager.broadcast_new_image(record)
                        except Exception:
                            pass
                        return {"url": local_path}

                    elif status == "FAILED":
                        raise Exception(f"ModelScope task failed: {data}")

                except Exception as loop_e:
                    print(f"Polling error (retrying): {loop_e}")
                    continue

            raise Exception("Cloud generation timeout")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Cloud generation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# --- ModelScope 通用图片生成（支持图生图） ---

@app.post("/api/ms/generate")
async def ms_generate(req: MsGenerateRequest):
    base_url = 'https://api-inference.modelscope.cn/'
    clean_token = MODELSCOPE_API_KEY.strip()
    if not clean_token:
        raise HTTPException(status_code=400, detail="未配置 MODELSCOPE_API_KEY，请在 API/.env 中填写。")

    headers = {
        "Authorization": f"Bearer {clean_token}",
        "Content-Type": "application/json",
        "X-ModelScope-Async-Mode": "true"
    }
    payload = {
        "model": req.model,
        "prompt": req.prompt.strip(),
    }
    if req.width and req.height:
        payload["width"] = req.width
        payload["height"] = req.height
    if req.image_urls:
        payload["image_url"] = req.image_urls
    if req.loras is not None:
        payload["loras"] = req.loras

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            submit_res = await client.post(
                f"{base_url}v1/images/generations",
                headers=headers,
                json=payload
            )
            if submit_res.status_code != 200:
                try:
                    detail = submit_res.json()
                except:
                    detail = submit_res.text
                raise HTTPException(status_code=submit_res.status_code, detail=detail)

            task_id = submit_res.json().get("task_id")
            print(f"MS Generate Task submitted ({req.model}), ID: {task_id}")

            TERMINAL_FAILED_STATUSES = {"FAILED", "FAIL", "ERROR", "CANCELED", "CANCELLED", "TIMEOUT", "REVOKED"}

            for i in range(300):
                await asyncio.sleep(2)
                try:
                    result = await client.get(
                        f"{base_url}v1/tasks/{task_id}",
                        headers={**headers, "X-ModelScope-Task-Type": "image_generation"},
                    )
                    data = result.json()
                    status = data.get("task_status")
                    print(f"MS Task {task_id} poll {i}: status={status}")

                    if status == "SUCCEED":
                        img_url = data["output_images"][0]
                        local_path = ""
                        try:
                            async with httpx.AsyncClient() as dl_client:
                                img_res = await dl_client.get(img_url)
                                if img_res.status_code == 200:
                                    filename = f"ms_{req.model.replace('/', '_').replace(':', '_')}_{int(time.time())}.png"
                                    file_path = os.path.join(OUTPUT_DIR, filename)
                                    with open(file_path, "wb") as f:
                                        f.write(img_res.content)
                                    local_path = offload_local_file(file_path, f"/output/{filename}")
                                else:
                                    local_path = img_url
                        except Exception:
                            local_path = img_url

                        record = {
                            "timestamp": time.time(),
                            "prompt": req.prompt,
                            "images": [local_path],
                            "type": "klein",
                            "model": req.model,
                        }
                        save_to_history(record)
                        if GLOBAL_LOOP:
                            asyncio.run_coroutine_threadsafe(manager.broadcast_new_image(record), GLOBAL_LOOP)
                        return {"url": local_path, "task_id": task_id}

                    elif status in TERMINAL_FAILED_STATUSES:
                        error_info = data.get("error_info") or data.get("message") or data.get("detail") or str(data)
                        raise HTTPException(status_code=502, detail=f"MS task {status}: {error_info}")

                except HTTPException:
                    raise
                except Exception as loop_e:
                    print(f"MS polling error: {loop_e}")
                    continue

            raise HTTPException(status_code=504, detail="MS 生图超时")

    except HTTPException:
        raise
    except Exception as e:
        print(f"MS generate error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# --- 本地 ComfyUI 生图 ---

def apply_nanobanana_edit_references(workflow: Dict[str, Any], image_names: List[str]):
    images = [name for name in image_names[:10] if name]
    if not images:
        return
    nano_node = workflow.get("24")
    if not isinstance(nano_node, dict):
        return
    nano_inputs = nano_node.setdefault("inputs", {})
    for index, image_name in enumerate(images, start=1):
        node_id = "16" if index == 1 else str(1000 + index)
        workflow[node_id] = {
            "inputs": {"image": image_name},
            "class_type": "LoadImage",
            "_meta": {"title": f"加载图像 {index}"}
        }
        nano_inputs[f"model.images.image_{index}"] = [node_id, 0]

def apply_prompt_to_workflow(workflow: Dict[str, Any], prompt: str) -> List[str]:
    prompt = (prompt or "").strip()
    if not prompt:
        return []

    preferred_matches = []
    fallback_matches = []
    for node_id, node in workflow.items():
        if not isinstance(node, dict):
            continue
        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            continue
        class_type = str(node.get("class_type") or "").lower()
        title = str((node.get("_meta") or {}).get("title") or "").lower()
        searchable = f"{class_type} {title}"

        for field in ("text", "prompt", "positive"):
            if field not in inputs or field == "system_prompt":
                continue
            value = inputs.get(field)
            if not isinstance(value, str):
                continue
            target = (node_id, field)
            if "positive" in searchable or "cliptextencode" in class_type or "prompt" in searchable:
                preferred_matches.append(target)
            else:
                fallback_matches.append(target)

    matches = preferred_matches or fallback_matches
    for node_id, field in matches:
        workflow[node_id]["inputs"][field] = prompt
    return [f"{node_id}.{field}" for node_id, field in matches]

@app.post("/api/generate")
def generate(req: GenerateRequest):
    global NEXT_TASK_ID
    current_task = None
    target_backend = None
    with QUEUE_LOCK:
        task_id = NEXT_TASK_ID
        NEXT_TASK_ID += 1
        current_task = {"task_id": task_id, "client_id": req.client_id}
        QUEUE.append(current_task)

    try:
        required_images = []
        for node_id, node_inputs in req.params.items():
            if isinstance(node_inputs, dict) and "image" in node_inputs:
                image_name = node_inputs["image"]
                if isinstance(image_name, str) and image_name:
                    required_images.append(image_name)
        for image_name in req.reference_images:
            if image_name and image_name not in required_images:
                required_images.append(image_name)

        target_backend = get_best_backend(required_images)
        with LOAD_LOCK:
            BACKEND_LOCAL_LOAD[target_backend] += 1

        for image_name in required_images:
            need_sync = False
            try:
                check_url = comfy_url(target_backend, f"/view?filename={urllib.parse.quote(image_name)}&type=input")
                resp = requests.get(check_url, stream=True, timeout=0.5)
                resp.close()
                if resp.status_code != 200:
                    need_sync = True
            except:
                need_sync = True

            if need_sync:
                image_content = None
                image_type = "image/png"
                for addr in COMFYUI_INSTANCES:
                    if addr == target_backend: continue
                    try:
                        src_url = comfy_url(addr, f"/view?filename={urllib.parse.quote(image_name)}&type=input")
                        r = requests.get(src_url, timeout=5)
                        if r.status_code == 200:
                            image_content = r.content
                            image_type = r.headers.get("Content-Type", "image/png")
                            break
                    except: continue

                if image_content:
                    try:
                        files = {'image': (image_name, image_content, image_type)}
                        requests.post(comfy_url(target_backend, "/upload/image"), files=files, timeout=10)
                    except Exception as e:
                        print(f"Sync upload failed: {e}")

        workflow_path = os.path.join(WORKFLOW_DIR, req.workflow_json)
        if not os.path.exists(workflow_path) and req.workflow_json == "Z-Image.json":
            workflow_path = WORKFLOW_PATH
        if not os.path.exists(workflow_path):
            raise Exception(f"Workflow file not found: {req.workflow_json}")

        with open(workflow_path, 'r', encoding='utf-8') as f:
            workflow = json.load(f)

        if req.workflow_json == "nanobanana Edit.json":
            apply_nanobanana_edit_references(workflow, req.reference_images)

        seed = random.randint(1, 10**15)

        prompt_targets = apply_prompt_to_workflow(workflow, req.prompt)
        if req.prompt and not prompt_targets:
            raise Exception("当前工作流没有找到可写入的提示词节点")
        if "144" in workflow:
            workflow["144"]["inputs"]["width"] = req.width
            workflow["144"]["inputs"]["height"] = req.height
        if "22" in workflow:
            workflow["22"]["inputs"]["seed"] = seed
        if "158" in workflow:
            workflow["158"]["inputs"]["noise_seed"] = seed
        for node_id in ["146", "181"]:
            if node_id in workflow and "inputs" in workflow[node_id] and "seed" in workflow[node_id]["inputs"]:
                workflow[node_id]["inputs"]["seed"] = seed
        if "184" in workflow and "inputs" in workflow["184"] and "seed" in workflow["184"]["inputs"]:
            workflow["184"]["inputs"]["seed"] = seed
        if "172" in workflow and "inputs" in workflow["172"] and "seed" in workflow["172"]["inputs"]:
            workflow["172"]["inputs"]["seed"] = seed % 4294967295
        if "14" in workflow and "inputs" in workflow["14"] and "seed" in workflow["14"]["inputs"]:
            workflow["14"]["inputs"]["seed"] = seed

        for node_id, node_inputs in req.params.items():
            if node_id in workflow:
                if "inputs" not in workflow[node_id]:
                    workflow[node_id]["inputs"] = {}
                for input_name, value in node_inputs.items():
                    workflow[node_id]["inputs"][input_name] = value

        p = {"prompt": workflow, "client_id": CLIENT_ID}
        extra_data = comfy_prompt_extra_data()
        if extra_data:
            p["extra_data"] = extra_data
        data = json.dumps(p).encode('utf-8')
        try:
            post_req = urllib.request.Request(
                comfy_url(target_backend, "/prompt"),
                data=data,
                headers={"Content-Type": "application/json"},
            )
            prompt_id = json.loads(urllib.request.urlopen(post_req, timeout=10).read())['prompt_id']
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            raise Exception(f"HTTP Error {e.code}: {error_body}")

        history_data = None
        for i in range(300):
            try:
                res = get_comfy_history(target_backend, prompt_id)
                if prompt_id in res:
                    history_data = res[prompt_id]
                    break
            except Exception:
                pass
            time.sleep(1)

        if not history_data:
            raise Exception("ComfyUI 渲染超时")

        status = history_data.get("status") or {}
        if status.get("status_str") == "error" or status.get("completed") is False:
            detail = "ComfyUI 执行失败"
            for message in status.get("messages") or []:
                if isinstance(message, list) and len(message) > 1 and message[0] == "execution_error":
                    payload = message[1] if isinstance(message[1], dict) else {}
                    node = payload.get("node_type") or payload.get("node_id") or "unknown"
                    reason = (payload.get("exception_message") or payload.get("exception_type") or "").strip()
                    detail = f"ComfyUI 节点 {node} 执行失败：{reason or '未知错误'}"
                    if "Please login first" in reason:
                        detail += "。后端未拿到 ComfyUI 登录态，请先完成 ComfyUI 登录。"
                    break
            raise Exception(detail)

        local_urls = []
        current_timestamp = time.time()
        if 'outputs' in history_data:
            for node_id in history_data['outputs']:
                node_output = history_data['outputs'][node_id]
                if 'images' in node_output:
                    for img in node_output['images']:
                        comfy_url_path = f"/view?filename={img['filename']}&subfolder={img['subfolder']}&type={img['type']}"
                        prefix = f"{req.type}_{int(current_timestamp)}_"
                        local_path = download_image(target_backend, comfy_url_path, prefix=prefix)
                        if req.convert_to_jpg:
                            local_path = convert_output_to_jpg(local_path)
                        local_urls.append(local_path)

        if not local_urls:
            raise Exception("ComfyUI 没有返回图片输出")

        # 一次生成返回多张时（常见一张清晰一张糊），只保留最清晰的一张。
        if len(local_urls) > 1:
            best = pick_sharpest_ref(local_urls)
            if best:
                local_urls = [best]

        result = {
            "prompt": req.prompt if req.prompt else "Detail Enhance",
            "images": local_urls,
            "seed": seed,
            "timestamp": current_timestamp,
            "type": req.type,
            "params": req.params,
            "client_id": req.client_id
        }
        if req.model:
            result["model"] = req.model
        if req.reference_images:
            result["reference_images"] = req.reference_images
        save_to_history(result)
        if GLOBAL_LOOP:
            asyncio.run_coroutine_threadsafe(manager.broadcast_new_image(result), GLOBAL_LOOP)
        return result

    except Exception as e:
        return {"images": [], "error": str(e)}
    finally:
        if target_backend:
            with LOAD_LOCK:
                if BACKEND_LOCAL_LOAD.get(target_backend, 0) > 0:
                    BACKEND_LOCAL_LOAD[target_backend] -= 1
        if current_task:
            with QUEUE_LOCK:
                if current_task in QUEUE:
                    QUEUE.remove(current_task)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)
