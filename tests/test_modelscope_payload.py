import unittest
import base64
import io
import os
from unittest.mock import patch

from fastapi import HTTPException
from PIL import Image

from main import (
    OUTPUT_DIR,
    chat_api_headers_for_key,
    chat_completion_payload,
    chat_stream_headers,
    delete_asset_folder_from_library,
    generate_ai_image,
    modelscope_edit_image_payload,
    reference_to_data_url,
    reasoning_delta_from_chat_chunk,
    split_reasoning_tags,
    normalize_prompt_library,
    promptdexter_valid_prompt_text,
    update_promptdexter_item_detail,
    update_promptdexter_prompt_in_library,
)


class ImageApiEditTests(unittest.IsolatedAsyncioTestCase):
    async def test_data_uri_edit_uses_duck2api_multipart_endpoint(self):
        request = {}

        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {"data": [{"b64_json": "edited-image"}]}

        class FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return None

            async def post(self, url, **kwargs):
                request["url"] = url
                request["data"] = kwargs.get("data")
                request["files"] = kwargs.get("files")
                return FakeResponse()

        source = io.BytesIO()
        Image.new("RGB", (16, 8), "red").save(source, "PNG")
        reference = "data:image/png;base64," + base64.b64encode(source.getvalue()).decode()
        with patch("main.IMAGE_API_BASE_URL", "https://duck2api.example/v1"), \
             patch("main.IMAGE_API_KEY", "sk-test"), \
             patch("main.httpx.AsyncClient", return_value=FakeClient()):
            image, _ = await generate_ai_image(
                "add glasses", "1024x1024", "auto", "gpt-5.4-mini", [{"url": reference}]
            )

        self.assertEqual(request["url"], "https://duck2api.example/v1/images/edits")
        self.assertEqual(request["data"]["model"], "gpt-5.4-mini")
        self.assertEqual(request["files"][0][0], "image")
        self.assertEqual(request["files"][0][1][0], "reference-1.webp")
        self.assertEqual(request["files"][0][1][2], "image/webp")
        normalized = request["files"][0][1][1]
        self.assertEqual(normalized[:4], b"RIFF")
        self.assertEqual(normalized[8:12], b"WEBP")
        with Image.open(io.BytesIO(normalized)) as image_file:
            self.assertEqual(image_file.size, (16, 8))
        self.assertEqual(image, {"type": "b64", "value": "edited-image"})

    async def test_remote_reference_is_downloaded_before_duck2api_edit(self):
        request = {}
        source = io.BytesIO()
        Image.new("RGB", (12, 6), "blue").save(source, "PNG")

        class FakeDownloadResponse:
            content = source.getvalue()
            headers = {"Content-Type": "image/png"}

            def raise_for_status(self):
                return None

        class FakeEditResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {"data": [{"b64_json": "edited-image"}]}

        class FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return None

            async def post(self, url, **kwargs):
                request["url"] = url
                request["files"] = kwargs.get("files")
                return FakeEditResponse()

        with patch("main.IMAGE_API_BASE_URL", "https://duck2api.example/v1"), \
             patch("main.IMAGE_API_KEY", "sk-test"), \
             patch("main.requests.get", return_value=FakeDownloadResponse()), \
             patch("main.httpx.AsyncClient", return_value=FakeClient()):
            image, _ = await generate_ai_image(
                "add glasses", "1024x1024", "auto", "gpt-5.4-mini",
                [{"url": "https://r2.example/source.png"}],
            )

        self.assertEqual(request["url"], "https://duck2api.example/v1/images/edits")
        self.assertEqual(len(request["files"]), 1)
        normalized = request["files"][0][1][1]
        self.assertEqual(normalized[:4], b"RIFF")
        self.assertEqual(normalized[8:12], b"WEBP")
        with Image.open(io.BytesIO(normalized)) as image_file:
            self.assertEqual(image_file.size, (12, 6))
        self.assertEqual(image, {"type": "b64", "value": "edited-image"})

    def test_image_edit_navigation_uses_cowart_duck2api_build(self):
        root = os.path.join(os.path.dirname(__file__), "..")
        with open(os.path.join(root, "static", "index.html"), encoding="utf-8") as f:
            index_source = f.read()
        with open(os.path.join(root, "static", "cowart", "assets", "index-ChYAZ9ph.js"), encoding="utf-8") as f:
            cowart_source = f.read()

        self.assertIn('id="frame-klein" data-src="/static/cowart/index.html', index_source)
        self.assertIn("/api/online-image", cowart_source)
        self.assertIn('yFt="gpt-5.4-nano"', cowart_source)
        self.assertIn("CowartSourceRef", cowart_source)

    def test_completed_ai_holder_does_not_keep_generation_sweep(self):
        root = os.path.join(os.path.dirname(__file__), "..")
        with open(os.path.join(root, "static", "cowart", "assets", "index-ChYAZ9ph.js"), encoding="utf-8") as f:
            cowart_source = f.read()

        self.assertIn("function CowartAiHolderComplete", cowart_source)
        self.assertIn("CowartAiHolderComplete(ed,s.id)", cowart_source)
        self.assertIn("function CowartClearCompletedAiHolders", cowart_source)
        self.assertIn("CowartClearCompletedAiHolders(ed)", cowart_source)


class ModelScopePayloadTests(unittest.TestCase):
    def test_data_uri_uses_base64_images_field(self):
        payload = modelscope_edit_image_payload(["data:image/png;base64,abc123"])

        self.assertEqual(payload, {"images": ["abc123"]})

    def test_remote_url_uses_image_url_field(self):
        payload = modelscope_edit_image_payload(["https://example.com/input.png"])

        self.assertEqual(payload, {"image_url": ["https://example.com/input.png"]})

    def test_rejects_mixed_base64_and_url_inputs(self):
        with self.assertRaises(HTTPException):
            modelscope_edit_image_payload([
                "data:image/png;base64,abc123",
                "https://example.com/input.png",
            ])


class ChatStreamHeaderTests(unittest.TestCase):
    def test_stream_headers_request_sse(self):
        headers = chat_api_headers_for_key("sk-test")

        stream_headers = chat_stream_headers(headers)

        self.assertEqual(stream_headers["Accept"], "text/event-stream")
        self.assertEqual(headers["Accept"], "application/json")
        self.assertEqual(stream_headers["Authorization"], "Bearer sk-test")


class ChatCompletionPayloadTests(unittest.TestCase):
    def test_reasoning_is_omitted_when_disabled(self):
        payload = chat_completion_payload("model", [{"role": "user", "content": "hi"}])

        self.assertNotIn("reasoning", payload)
        self.assertNotIn("chat_template_kwargs", payload)

    def test_thinking_template_is_disabled_for_gemma_when_reasoning_disabled(self):
        payload = chat_completion_payload("google/gemma-4-26b-a4b", [{"role": "user", "content": "hi"}])

        self.assertEqual(payload["chat_template_kwargs"], {"enable_thinking": False, "enableThinking": False})

    def test_reasoning_uses_default_effort_when_enabled(self):
        payload = chat_completion_payload("google/gemma-4-26b-a4b", [{"role": "user", "content": "hi"}], reasoning_enabled=True)

        self.assertEqual(payload["reasoning"], {"effort": "medium"})
        self.assertEqual(payload["chat_template_kwargs"], {"enable_thinking": True, "enableThinking": True})

    def test_reasoning_delta_reads_lm_studio_reasoning_content(self):
        chunk = {"choices": [{"delta": {"reasoning_content": "思考中"}}]}

        self.assertEqual(reasoning_delta_from_chat_chunk(chunk), "思考中")

    def test_split_reasoning_tags_removes_think_block(self):
        reasoning, content = split_reasoning_tags("<think>先算一下</think>答案是 2")

        self.assertEqual(reasoning, "先算一下")
        self.assertEqual(content, "答案是 2")


class ReferenceImageTests(unittest.TestCase):
    def test_output_reference_is_encoded_as_data_url(self):
        filename = "test_reference_to_data_url.png"
        path = os.path.join(OUTPUT_DIR, filename)
        with open(path, "wb") as f:
            f.write(b"png-data")
        try:
            data_url = reference_to_data_url({"url": f"/output/{filename}"})
        finally:
            os.remove(path)

        self.assertEqual(data_url, "data:image/png;base64,cG5nLWRhdGE=")


class AssetFolderTests(unittest.TestCase):
    def test_delete_folder_unfiles_assets_without_deleting_items(self):
        library = {
            "folders": [{"id": "folder-1", "name": "Folder"}],
            "items": [
                {"id": "asset-1", "folder_id": "folder-1"},
                {"id": "asset-2", "folder_id": ""},
            ],
        }

        deleted, unfiled = delete_asset_folder_from_library(library, "folder-1")

        self.assertEqual(deleted, 1)
        self.assertEqual(unfiled, 1)
        self.assertEqual(library["folders"], [])
        self.assertEqual(library["items"][0]["folder_id"], "")
        self.assertEqual(len(library["items"]), 2)


class PromptDexterDetailTests(unittest.TestCase):
    def test_rejects_promptdexter_placeholder_prompt_text(self):
        self.assertFalse(promptdexter_valid_prompt_text("$20"))
        self.assertFalse(promptdexter_valid_prompt_text("$af"))
        self.assertTrue(promptdexter_valid_prompt_text("A detailed prompt with enough real words."))

    def test_normalize_clears_placeholder_prompt_text(self):
        library = {
            "promptdexter": {
                "items": [
                    {"id": "pd-1", "prompt": "$20", "detail_loaded": True, "synced_at": 123}
                ]
            }
        }

        normalized = normalize_prompt_library(library)
        item = normalized["promptdexter"]["items"][0]

        self.assertEqual(item["prompt"], "")
        self.assertFalse(item["detail_loaded"])
        self.assertEqual(item["detail_invalid_reason"], "invalid_prompt_placeholder")

    def test_detail_refresh_keeps_sync_order_timestamp(self):
        item = {
            "source_url": "https://promptdexter.com/example",
            "prompt": "",
            "detail_loaded": False,
            "synced_at": 123,
        }
        detail = {
            "prompt": "Detailed prompt",
            "detail_loaded": True,
            "detail_loaded_at": 456,
            "categories": ["featured"],
        }

        with patch("main.promptdexter_parse_detail", return_value=detail):
            update_promptdexter_item_detail(item, [{"id": "featured", "name": "精选提示词"}])

        self.assertEqual(item["prompt"], "Detailed prompt")
        self.assertTrue(item["detail_loaded"])
        self.assertEqual(item["synced_at"], 123)

    def test_prompt_text_edit_keeps_sync_order_timestamp(self):
        library = {
            "promptdexter": {
                "items": [
                    {"id": "pd-1", "prompt": "old", "detail_loaded": True, "synced_at": 123}
                ]
            }
        }

        item = update_promptdexter_prompt_in_library(library, "pd-1", "new prompt")

        self.assertEqual(item["prompt"], "new prompt")
        self.assertTrue(item["detail_loaded"])
        self.assertEqual(item["synced_at"], 123)


if __name__ == "__main__":
    unittest.main()
