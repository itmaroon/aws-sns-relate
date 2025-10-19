import os
import json
import mimetypes
import urllib.request
import urllib.error

def lambda_handler(event, context):
    access_token = event.get("access_token")
    media_url = event.get("media_url")
    job_id = event.get("job_id", "")
    caption = event.get("caption", "")
    text = event.get("text", "")

    if not access_token or not media_url:
        return {"error": "missing access_token or media_url"}

    mime_type, _ = mimetypes.guess_type(media_url)
    if not mime_type:
        mime_type = "application/octet-stream"

    # ===== Content-Length 取得 (HEADの代わりにGETのRangeで代用) =====
    try:
        req = urllib.request.Request(media_url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            total_bytes = int(resp.headers.get("Content-Length", "0"))
    except Exception as e:
        return {"error": f"failed to get Content-Length (via Range GET): {e}"}

    if total_bytes == 0:
        return {"error": "total_bytes=0 (object may not be accessible)"}

    # ===== カテゴリ判定 =====
    if mime_type.startswith("video/"):
        media_category = "tweet_video"
    elif mime_type.startswith("image/"):
        media_category = "tweet_image"
    else:
        media_category = "tweet_media"

    # ===== Initialize API 呼び出し =====
    endpoint = "https://api.x.com/2/media/upload/initialize"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "media_type": mime_type,
        "total_bytes": total_bytes,
        "media_category": media_category
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(endpoint, data=data, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            code = resp.status
            raw_body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raw_body = e.read().decode("utf-8")
        return {"error": f"HTTPError {e.code}", "body": raw_body}
    except Exception as e:
        return {"error": f"Request failed: {e}"}

    try:
        body = json.loads(raw_body)
    except Exception:
        body = {"raw": raw_body}

    if code < 200 or code >= 300:
        return {"error": f"init_failed: {code}", "response": body}

    media_id = body.get("data", {}).get("id")
    if not media_id:
        return {"error": "no media_id returned", "response": body}

    return {
        "job_id": job_id,
        "media_url": media_url,
        "media_id": media_id,
        "media_type": mime_type,
        "media_category": media_category,
        "total_bytes": total_bytes,
        "caption": caption,
        "text": text,
        "status": "initialized",
        "access_token": access_token
    }
