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
        err = (e.read() or b"").decode("utf-8", errors="replace")
        try:
            j = json.loads(err)
        except Exception:
            j = {"raw": err}
        return {"ok": False, "status": e.code, "body": j}
    except Exception as e:
        return {"ok": False, "status": 0, "body": {"error": str(e)}}

def lambda_handler(event, ctx):
    """
    event 例:
    {
      "job": {"access_token":"...","ig_user_id":"..."},
      "cid": {"creation_id":"1789..."}
    }
    """
    token = event["job"]["access_token"]
    ig    = event["job"]["ig_user_id"]
    cid   = event["cid"]["creation_id"]

    url = f"{GRAPH}/{ig}/media_publish"
    data = {"creation_id": cid, "access_token": token}
    res = _post_form(url, data)

    if res["ok"]:
        media_id = res.get("body", {}).get("id")
        set_status(event["job"]["job_id"], str(media_id or ""))  # ← 成功は media_id をそのまま
        return {"ok": True, "status": res["status"], "media_id": media_id, "raw": res["body"]}
    else:
        set_status(event["job"]["job_id"], f"ERROR,publish,{res.get('status')}")
        return {"ok": False, "status": res["status"], "media_id": None, "raw": res["body"]}

    
