import os
import json
import urllib.request
import urllib.error
import urllib.parse
import time

def lambda_handler(event, context):
    """
    event には最低以下がある前提：
    {
        "media_id": "...",
        "access_token": "...",
        "job_id": "...",
        // 以前の処理で渡されたその他情報
    }
    """
    media_id = event.get("media_id")
    access_token = event.get("access_token")
    job_id = event.get("job_id")

    if not media_id or not access_token:
        return {"complete": False, "error": "missing media_id or access_token"}

    # URL に command=STATUS と media_id を付与
    base_url = "https://api.x.com/2/media/upload"
    query = urllib.parse.urlencode({
        "command": "STATUS",
        "media_id": media_id,
    })
    status_url = f"{base_url}?{query}"

    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    print(f"status_url: {status_url}")
    req = urllib.request.Request(status_url, headers=headers, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            # 例: { "data": { "processing_info": { "state": "...", "check_after_secs": ... } } }
            processing_info = data.get("data", {}).get("processing_info") or {}
            state = processing_info.get("state")
            check_after = processing_info.get("check_after_secs")
            print(f"state: {state}, check_after: {check_after}")
            result = {
                "complete": False,
                "status": state,
                "check_after": check_after,
                "media_id": media_id,
                "job_id": job_id,
                "access_token": access_token
            }

            # 成功 or 失敗状態なら complete を True に
            if state == "succeeded" or state == "failed":
                result["complete"] = True

            return result

    except urllib.error.HTTPError as e:
        body = e.read().decode()
        return {"complete": False, "error": f"status_failed: HTTP {e.code}", "body": body, "media_id": media_id, "job_id": job_id}
    except urllib.error.URLError as e:
        return {"complete": False, "error": f"URLError: {e.reason}", "media_id": media_id, "job_id": job_id}
    except Exception as e:
        return {"complete": False, "error": str(e), "media_id": media_id, "job_id": job_id}
