import os, json, boto3, re, urllib.parse

REGION = os.getenv("AWS_REGION", "ap-northeast-1")
TABLE  = os.getenv("JOBS_TABLE", "convert_jobs")
API_TOKEN = os.getenv("API_TOKEN")  # 任意: ある場合は X-API-Token と一致すれば通す

ddb   = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)

def _resp(code, body):
    return {
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, X-API-Token, X-Site-Url, X-Wp-Id"
        },
        "body": json.dumps(body, ensure_ascii=False)
    }

def _lower_headers(event):
    return { (k or "").lower(): v for k, v in (event.get("headers") or {}).items() }

def _normalize_site_url(u: str) -> str:
    """比較用：スキーム/ホスト/パス末尾スラッシュの差異を吸収"""
    if not u:
        return ""
    u = u.strip()
    parsed = urllib.parse.urlparse(u)
    # スキーム/ホストは小文字化。末尾のスラッシュは削る
    scheme = (parsed.scheme or "https").lower()
    netloc = (parsed.netloc or parsed.path).lower()
    path   = parsed.path if parsed.netloc else ""
    path = re.sub(r"/+$", "", path or "")
    return f"{scheme}://{netloc}{path}"

def lambda_handler(event, ctx):
    pathp = event.get("pathParameters") or {}
    qp    = event.get("queryStringParameters") or {}
    headers = _lower_headers(event)

    job_id  = pathp.get("job_id") or qp.get("job_id")
    if not job_id:
        return _resp(400, {"error": "job_id required"})

    # 1) レコード取得
    try:
        r = ddb.get_item(Key={"job_id": job_id})
        item = r.get("Item")
        if not item:
            return _resp(404, {"error":"not found", "job_id": job_id})
    except Exception as e:
        return _resp(500, {"error": f"ddb get failed: {e}", "job_id": job_id})

    # レコード側の識別子（/presign 時に保存している前提）
    rec_wp_id    = str(item.get("wp_id", ""))         # 例: "1262"
    rec_site_url = _normalize_site_url(item.get("site_url", ""))

    # 2) 認可チェック
    # 2-1) マスターAPIトークン（任意）
    if API_TOKEN and headers.get("x-api-token") == API_TOKEN:
        authorized = True
    else:
        # 2-2) 呼び出し元のサイト認証（ヘッダで自己申告）
        req_wp_id    = str(headers.get("x-wp-id", "") or qp.get("wp_id", "")).strip()
        req_site_url = _normalize_site_url(headers.get("x-site-url", "") or qp.get("site_url", ""))

        authorized = (rec_wp_id and rec_wp_id == req_wp_id) and (rec_site_url and rec_site_url == req_site_url)

    if not authorized:
        return _resp(403, {
            "error": "forbidden",
            "hint": "X-API-Token or both X-Site-Url & X-Wp-Id must match saved record.",
            "job_id": job_id
        })

    # 3) 削除実行（冪等）
    try:
        ddb.delete_item(Key={"job_id": job_id})
        return _resp(200, {"ok": True, "job_id": job_id})
    except Exception as e:
        return _resp(500, {"error": f"ddb delete failed: {e}", "job_id": job_id})
