# lambda_presign.py
import os, json, uuid, base64, time
from urllib.parse import urlencode, quote
import boto3
from botocore.client import Config

REGION          = os.getenv("REGION", "ap-northeast-1")     # AWS_REGION は予約キーなので使わない
IN_BUCKET       = os.getenv("IN_BUCKET")                    # 例: itmar-video-upload-bucket
OUT_BUCKET      = os.getenv("OUT_BUCKET") or IN_BUCKET
IN_PREFIX       = (os.getenv("IN_PREFIX") or "in/").lstrip("/")
DEFAULT_EXPIRES = int(os.getenv("DEFAULT_EXPIRES", "900"))
JOBS_TABLE      = os.getenv("JOBS_TABLE", "convert_jobs")
KMS_KEY_ID      = os.getenv("KMS_KEY_ID")                   # IG 連携時のみ必須

s3  = boto3.client("s3", region_name=REGION, config=Config(signature_version="s3v4"))
ddb = boto3.resource("dynamodb", region_name=REGION)
kms = boto3.client("kms", region_name=REGION)
jobs = ddb.Table(JOBS_TABLE)

MIME_MAP = {
    "mp4":"video/mp4","m4v":"video/mp4","mov":"video/quicktime","webm":"video/webm",
    "jpg":"image/jpeg","jpeg":"image/jpeg","png":"image/png","gif":"image/gif",
    "webp":"image/webp","bmp":"image/bmp","svg":"image/svg+xml",
}

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type":"application/json"}, "body": json.dumps(body, ensure_ascii=False)}

def _sanitize_ext(v: str) -> str:
    v = (v or "").strip().lower()
    return v[1:] if v.startswith(".") else v

def _choose_mime(ext: str) -> str:
    return MIME_MAP.get(ext, "application/octet-stream")

def _bound_expires(v: int) -> int:
    v = int(v or DEFAULT_EXPIRES)
    if v < 60: v = 60
    if v > 3600: v = 3600
    return v

def lambda_handler(event, context):
    headers = { (k or "").lower(): v for k, v in (event.get("headers") or {}).items() }
    body_raw = event.get("body") or "{}"
    body     = json.loads(body_raw) if isinstance(body_raw, str) else (body_raw or {})

    # 共通
    op = (body.get("op") or "put").lower()
    site_url    = (headers.get("x-site-url") or body.get("site_url") or "").strip()
    webhook_url = (headers.get("x-webhook-url") or "").strip()

    # ===== op=get: 署名付き GET URL を返す =====
    if op == "get":
        # 例: { "op":"get", "bucket": "...", "key": "...", "expires": 600 }
        bucket  = (body.get("bucket") or IN_BUCKET or "").strip()
        key     = (body.get("key") or "").strip()
        expires = _bound_expires(body.get("expires"))

        if not bucket or not key:
            return _resp(400, {"error": "bucket and key required"})

        # セキュリティ: 許可バケットのみに限定（必要に応じて調整）
        allowed = {b for b in [IN_BUCKET, OUT_BUCKET] if b}
        if bucket not in allowed:
            return _resp(403, {"error": "bucket not allowed"})

        try:
            get_url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expires
            )
            return _resp(200, {
                "bucket": bucket,
                "key": key,
                "get_url": get_url,
                "expires_in": expires
            })
        except Exception as e:
            return _resp(500, {"error": f"presign(get) failed: {e}"})

    # ===== ここから op=put =====
    if not IN_BUCKET:
        return _resp(500, {"error":"IN_BUCKET not set"})

    # PUT は FB トークン必須（IG 投稿連携のため）
    fb_token = (headers.get("x-fb-token") or headers.get("x-fb-access-token") or "").strip()
    if not fb_token:
        return _resp(401, {"error":"missing X-FB-Token"})

    ig_user_id  = (body.get("ig_user_id") or "").strip()
    wp_id       = (body.get("wp_id") or "").strip()   # 空なら“アップロードのみ”モード
    caption     = (body.get("caption") or "").strip()

    ext          = _sanitize_ext(body.get("ext") or "jpg")
    content_type = (body.get("type") or _choose_mime(ext)).strip()
    out_key      = (body.get("out_key") or "").strip()
    expires      = _bound_expires(body.get("expires"))
    in_key       = f"{IN_PREFIX.rstrip('/')}/{uuid.uuid4()}.{ext}"

    # 変換パラメータを文字列に
    raw_params = body.get("params")
    if isinstance(raw_params, (dict, list)):
        params_str = json.dumps(raw_params, separators=(",", ":"), ensure_ascii=False)
    elif isinstance(raw_params, str):
        params_str = raw_params
    else:
        params_str = ""

    create_job = bool(wp_id)  # True: IG 連携（DDB保存/変換投稿フロー起動）

    tagging_str = ""
    metadata    = {}
    job_id      = None

    if create_job:
        if not KMS_KEY_ID:
            return _resp(500, {"error":"KMS_KEY_ID not set"})

        # FBトークン暗号化
        try:
            enc = kms.encrypt(KeyId=KMS_KEY_ID, Plaintext=fb_token.encode("utf-8"))
            token_cipher_b64 = base64.b64encode(enc["CiphertextBlob"]).decode("ascii")
        except Exception as e:
            return _resp(500, {"error": f"kms encrypt failed: {e}"})

        # ジョブ作成（GSI = wp_id-updated_at-index を使うため wp_id は非空前提）
        job_id = str(uuid.uuid4())
        now    = int(time.time())
        item = {
            "job_id": job_id,
            "platform": "ig",
            "status": "pending",
            "created_at": now,
            "updated_at": now,
            "site_url": site_url or "",
            "ig_user_id": ig_user_id,
            "wp_id": wp_id,
            "caption": caption,
            "token_cipher": token_cipher_b64,
            "in_bucket": IN_BUCKET,
            "in_key": in_key,
            "out_bucket": OUT_BUCKET,
            "out_key": out_key,
        }
        try:
            jobs.put_item(Item=item)
        except Exception as e:
            return _resp(500, {"error": f"ddb put failed: {e}"})

        # Tagging（変換/投稿の起動スイッチ）
        tags = { "out_bucket": OUT_BUCKET, "out_key": out_key, "transcode": "true" }
        tagging_str = urlencode(tags, quote_via=quote, safe="")

        # Metadata（後段へ job-id/params/webhook を渡す）
        metadata["job-id"] = job_id
        if params_str:
            try:
                params_str.encode("ascii")
                metadata["params"] = params_str
            except Exception:
                metadata["params-b64"] = base64.b64encode(params_str.encode("utf-8")).decode("ascii")
        if webhook_url:
            metadata["cb-b64"] = base64.b64encode(webhook_url.encode("utf-8")).decode("ascii")

    else:
        # アップロードのみ：DDB保存・変換起動なし
        if params_str:
            try:
                params_str.encode("ascii")
                metadata["params"] = params_str
            except Exception:
                metadata["params-b64"] = base64.b64encode(params_str.encode("utf-8")).decode("ascii")
        if webhook_url:
            metadata["cb-b64"] = base64.b64encode(webhook_url.encode("utf-8")).decode("ascii")

    # presign (PUT)
    try:
        params = {
            "Bucket": IN_BUCKET,
            "Key": in_key,
            "ContentType": content_type,
            "Metadata": metadata or {},
        }
        if tagging_str:
            params["Tagging"] = tagging_str

        put_url = s3.generate_presigned_url("put_object", Params=params, ExpiresIn=expires)
    except Exception as e:
        return _resp(500, {"error": f"presign(put) failed: {e}"})

    required_headers = {"Content-Type": content_type}
    if tagging_str:
        required_headers["x-amz-tagging"] = tagging_str
    for mk, mv in (metadata or {}).items():
        required_headers[f"x-amz-meta-{mk}"] = mv

    resp = {
        "bucket": IN_BUCKET,
        "key": in_key,
        "put_url": put_url,
        "required_headers": required_headers,
        "x_amz_meta": metadata,
        "x_amz_tagging": tagging_str or None,
        "content_type": content_type,
        "expires_in": expires,
    }
    if job_id:
        resp.update({"job_id": job_id, "out_bucket": OUT_BUCKET, "out_key": out_key or None})

    return _resp(200, resp)
