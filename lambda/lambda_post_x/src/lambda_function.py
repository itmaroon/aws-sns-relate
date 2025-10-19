import json
import urllib.request
import urllib.error
import time
from ddb_helpers import set_status

def lambda_handler(event, context):
    """
    Posts a tweet to X API v2.
    Supports text-only or text+media posts.
    Handles rate limit (429) gracefully.
    """

    access_token = event.get("access_token")
    text = event.get("text")
    media_ids = event.get("media_ids", [])
    job_id = event.get("job_id", "")

    if not access_token or not text:
        return {"error": "missing required parameters"}

    post_data = {"text": text}
    if media_ids:
        # X expects media_ids as an array
        post_data["media"] = {"media_ids": media_ids}

    url = "https://api.x.com/2/posts"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    data_bytes = json.dumps(post_data).encode("utf-8")
    req = urllib.request.Request(url, data=data_bytes, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            code = resp.status
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        # --- レート制限対応 ---
        if e.code == 429:
            retry_after_header = e.headers.get("x-rate-limit-reset")
            if retry_after_header:
                try:
                    retry_after_ts = int(retry_after_header)
                    wait_seconds = max(0, retry_after_ts - int(time.time()))
                    wait_minutes = (wait_seconds + 59) // 60
                    return {
                        "error": "rate_limit",
                        "message": f"Xの投稿制限に達しました。あと約 {wait_minutes} 分後に再試行してください。",
                        "wait_seconds": wait_seconds,
                        "retry_after_timestamp": retry_after_ts
                    }
                except ValueError:
                    pass
            return {
                "error": "rate_limit",
                "message": "Xの投稿制限に達しました。しばらく待って再試行してください。",
                "status_code": e.code
            }
        return {
            "error": f"HTTPError: {e.code}",
            "response": body
        }
    except urllib.error.URLError as e:
        return {
            "error": "network_error",
            "message": str(e)
        }

    # --- レスポンス解析 ---
    try:
        response_json = json.loads(body)
    except json.JSONDecodeError:
        return {
            "error": "invalid_json",
            "raw_response": body
        }

    # --- 成功判定 ---
    if 200 <= code < 300 and "data" in response_json and "id" in response_json["data"]:
        set_status(job_id, response_json["data"]["id"]) 
        return {
            "status": "success",
            "job_id": job_id,
            "x_post_id": response_json["data"]["id"],
        }

    # --- エラー処理 ---
    if code == 429:
        set_status(job_id, "ERROR,rate_limit")
        return {
            "error": "rate_limit",
            "message": "Xの投稿制限に達しました。しばらく待って再試行してください。",
            "status_code": code,
            "job_id": job_id,
            "x_post_id": response_json["data"]["id"],
        }

    set_status(job_id, "ERROR,post_failed")
    return {
        "error": "post_failed",
        "status_code": code,
        "job_id": job_id,
        "x_post_id": response_json["data"]["id"],
    }
