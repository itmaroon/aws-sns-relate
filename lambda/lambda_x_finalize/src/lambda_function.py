import json
import urllib.request
import urllib.error

def lambda_handler(event, context):
    """
    Finalize the uploaded media on X API v2.
    If succeeded immediately → return success
    Otherwise → Step Functions will call lambda_poll_media_status
    """

    access_token = event.get("access_token")
    media_id = event.get("media_id")
    job_id = event.get("job_id", "")
    max_wait_sec = int(event.get("max_wait_sec", 180))

    if not all([access_token, media_id]):
        return {"error": "missing required parameters"}

    finalize_url = f"https://api.x.com/2/media/upload/{media_id}/finalize"

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    req = urllib.request.Request(finalize_url, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            code = resp.status
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        return {"error": f"finalize_failed: HTTP {e.code}", "response": raw}

    if code < 200 or code >= 300:
        return {"error": f"finalize_failed: HTTP {code}", "response": raw}

    body = json.loads(raw)
    data = body.get("data", {})
    info = data.get("processing_info", {})

    state = data.get("processing_state") or info.get("state") or "succeeded"
    check_after = int(info.get("check_after_secs", 5))

    # --- 即完了なら成功を返す ---
    if state == "succeeded":
        return {
            "status": "succeeded",
            "media_id": media_id,
            "job_id": job_id,
            "state": state
        }

    # --- 失敗ならエラーを返す ---
    if state == "failed":
        return {
            "error": "media_processing_failed",
            "state": state,
            "response": raw
        }

    # --- 未完了: ステートマシンでpoll_media_statusに進ませる ---
    return {
        "status": "processing",
        "media_id": media_id,
        "job_id": job_id,
        "state": state,
        "check_after": check_after,
        "access_token": access_token,
        "max_wait_sec": max_wait_sec
    }
