# lambda_convert_notifier.py
import os, json, base64, urllib.parse, urllib.request, time, socket, ssl
import boto3

AWS_REGION   = os.getenv("AWS_REGION", "ap-northeast-1")
GET_EXPIRES  = int(os.getenv("GET_EXPIRES", "3600"))
UPLOAD_PREFIX= (os.getenv("UPLOAD_PREFIX") or "converted/").lstrip("/")
SF_ARN       = os.getenv("SF_IG_POST_ARN")  # ← Step Functions の ARN（設定されていれば起動）

s3 = boto3.client("s3", region_name=AWS_REGION)
sf = boto3.client("stepfunctions", region_name=AWS_REGION) if SF_ARN else None

def _post_json(url: str, payload: dict, timeout=10, retries=2, backoff=1.5):
    """Webhook へ JSON POST（シンプルな再試行付き + 例外の見える化）"""
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    req.add_header("User-Agent", "itmar-notifier/1.0")

    last_err = None
    for i in range(retries + 1):
        try:
            print(f"DEBUG _post_json try={i} url={url} bytes={len(data)} timeout={timeout}")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status = resp.status
                body = (resp.read() or b"")[:500]
                print("webhook response:", status, body)
                return status, body

        except urllib.error.HTTPError as e:
            err_body = (e.read() or b"")[:500]
            print("ERROR HTTPError:", e.code, e.reason, err_body)
            if 400 <= e.code < 500:
                raise
            last_err = e

        except urllib.error.URLError as e:
            print("ERROR URLError:", repr(e.reason))
            last_err = e

        except (socket.timeout, ssl.SSLError) as e:
            print("ERROR Timeout/SSL:", type(e).__name__, str(e))
            last_err = e

        except Exception as e:
            print("ERROR Other:", type(e).__name__, str(e))
            last_err = e

        if i < retries:
            time.sleep(backoff ** i)

    raise last_err if last_err else RuntimeError("unknown webhook error")


def lambda_handler(event, context):
    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key    = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])

        # 早期フィルタ
        if UPLOAD_PREFIX and not key.startswith(UPLOAD_PREFIX):
            print("skip not under prefix:", key); continue

        # 出力オブジェクトのメタデータ取得
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
        except Exception as e:
            print("ERROR head_object:", e, bucket, key)
            continue

        meta = head.get("Metadata", {})  # x-amz-meta-* は小文字化される
        size         = head.get("ContentLength", 0)
        content_type = head.get("ContentType", "")
        etag         = (head.get("ETag") or "").strip('"')

        # presign GET（Graph が取りに来る）
        try:
            get_url = s3.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=GET_EXPIRES
            )
        except Exception as e:
            print("ERROR presign GET:", e); continue

        print("presign OK:", get_url, "meta:", meta)

        # ---- Step Functions 起動 ----
        job_id = meta.get("job-id")
        if SF_ARN and job_id and sf:
            sf_input = {
                "job_id": job_id,
                "video_url": get_url,
                "bucket": bucket,
                "key": key,
                "object": {
                    "size": size,
                    "content_type": content_type,
                    "etag": etag
                }
            }
            try:
                resp = sf.start_execution(
                    stateMachineArn=SF_ARN,
                    input=json.dumps(sf_input, ensure_ascii=False)
                )
                print("SF started:", resp.get("executionArn"))
                continue
            except Exception as e:
                print("ERROR start_execution:", e, "sf_input:", sf_input)
                continue

        else:
            if not job_id:
                print("WARN: job-id not found in metadata; Step Functions skipped")

        # ---- Webhook 通知（任意：cb-b64 があれば送る）----
        cb_b64 = meta.get("cb-b64")
        if not cb_b64:
            print("no cb-b64 metadata; webhook skip. key=", key)
            continue

        try:
            webhook_url = base64.b64decode(cb_b64).decode("utf-8").strip()
        except Exception as e:
            print("ERROR decode cb-b64:", e); continue

        if not webhook_url:
            print("empty webhook_url; skip", key); continue

        payload = {
            "event": "object_converted",
            "bucket": bucket,
            "key": key,
            "url": get_url,
            "expires_in": GET_EXPIRES,
            "size": size,
            "content_type": content_type,
            "etag": etag,
            "metadata": meta
        }
        print("payload:", payload)

        try:
            status, body = _post_json(webhook_url, payload)
            print("Webhook OK:", status, webhook_url, "resp:", (body or b"")[:200])
        except Exception as e:
            print("ERROR webhook POST:", e, webhook_url)
