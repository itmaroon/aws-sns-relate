import json, urllib.request
from ddb_helpers import set_status

GRAPH = "https://graph.facebook.com/v20.0"

def _get(url, timeout=12):
    req = urllib.request.Request(url, method="GET")
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
    cid   = event["cid"]["creation_id"]

    url = f"{GRAPH}/{cid}?fields=status_code&access_token={token}"
    res = _get(url)

    # 返却形：Step Functions の Choice で使いやすいように
    code = (res.get("body", {}).get("status_code") or "").upper()
    if not res["ok"]:
        set_status(event["job"]["job_id"], f"ERROR,check_status,{res.get('status')}")
    elif code == "ERROR":
        set_status(event["job"]["job_id"], "ERROR,check_status,GRAPH_ERROR")
    # IN_PROGRESS は何もしない
    return {"ok": res["ok"], "status": res["status"], "code": code, "raw": res["body"]}
