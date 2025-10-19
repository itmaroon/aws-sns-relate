
import os, json, uuid, base64, time, boto3
from botocore.client import Config

REGION     = os.getenv("REGION", "ap-northeast-1")
JOBS_TABLE = os.getenv("JOBS_TABLE", "convert_jobs")
KMS_KEY_ID = os.getenv("KMS_KEY_ID")
STATE_MACHINE_ARN = os.getenv("STATE_MACHINE_ARN")

ddb = boto3.resource("dynamodb", region_name=REGION).Table(JOBS_TABLE)
kms = boto3.client("kms", region_name=REGION)
sf = boto3.client("stepfunctions")

def _resp(c,b): return {"statusCode":c,"headers":{"Content-Type":"application/json"},"body":json.dumps(b,ensure_ascii=False)}

def lambda_handler(event, ctx):
    headers = {(k or "").lower(): v for k,v in (event.get("headers") or {}).items()}
    body = event.get("body") or "{}"
    body = json.loads(body) if isinstance(body, str) else (body or {})

    wp_id      = (body.get("wp_id") or "").strip()
    text       = (body.get("text") or "").strip()
    media_urls = body.get("media_urls") or []
    x_token    = (headers.get("x-x-token") or "").strip()
    site_url    = (headers.get("x-site-url") or body.get("site_url") or "").strip()

    if not KMS_KEY_ID: return _resp(500, {"error":"KMS_KEY_ID not set"})
    if not wp_id:      return _resp(400, {"error":"wp_id required"})
    if not x_token:    return _resp(401, {"error":"missing X-X-Token"})

    # 1) アクセストークンを即暗号化（保存は常に暗号化体のみ）
    try:
        enc = kms.encrypt(KeyId=KMS_KEY_ID, Plaintext=x_token.encode("utf-8"))
        token_cipher_b64 = base64.b64encode(enc["CiphertextBlob"]).decode("ascii")
    except Exception as e:
        return _resp(500, {"error": f"kms encrypt failed: {e}"})

    # 2) ジョブ作成
    job_id = str(uuid.uuid4())
    now    = int(time.time())
    item = {
        "job_id": job_id,
        "platform": "X",
        "status": "pending",
        "created_at": now,
        "updated_at": now,
        "wp_id": wp_id,
        "text": text,
        "media_urls": media_urls,     # 最初の Lambda が S3 へ取り込み
        "token_cipher": token_cipher_b64,  # ← 平文は保存しない
        "site_url": site_url
    }
    print(f"item: {item}")
    try:
        ddb.put_item(Item=item)
    except Exception as e:
        return _resp(500, {"error": f"ddb put failed: {e}"})

    # 3) Step Functions をここで起動
    sf.start_execution(
        stateMachineArn=os.getenv("STATE_MACHINE_ARN"),
        input=json.dumps({"job_id": job_id})
    )

    return _resp(200, {"ok": True, "job_id": job_id})
