import json, urllib.parse, urllib.request
from ddb_helpers import set_status

GRAPH = "https://graph.facebook.com/v20.0"

def _post_form(url: str, data: dict, timeout=20):
    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return {"ok": True, "status": resp.status, "body": json.loads(raw.decode("utf-8"))}
    except urllib.error.HTTPError as e:
        # ★400/401/…でも本文を読み取って返す
        err_body = (e.read() or b"").decode("utf-8", errors="replace")
        try:
            err_json = json.loads(err_body)
        except Exception:
            err_json = {"raw": err_body}
        return {"ok": False, "status": e.code, "body": err_json}
    except Exception as e:
        return {"ok": False, "status": 0, "body": {"error": str(e)}}

def lambda_handler(event, ctx):
    """
    event 例:
    {
      "job": {"access_token":"...", "ig_user_id":"...", "caption":"..."},
      "video_url":"https://presigned-s3-url",
      "bucket":"...", "key":"..."
    }
    """
    job       = event["job"]
    ig_user   = job["ig_user_id"]
    token     = job["access_token"]
    caption   = job.get("caption", "")
    video_url = event["video_url"]

    url  = f"{GRAPH}/{ig_user}/media"
    data = {
        "media_type": "REELS",
        "video_url": video_url,
        "caption": caption,
        "access_token": token
    }

    res = _post_form(url, data)
    
    if res["ok"]:
        # ここでは最終確定しない（Publish までいく想定）
        pass
    else:
        set_status(job["job_id"], f"ERROR,create_container,{res.get('status')}")
    return res

