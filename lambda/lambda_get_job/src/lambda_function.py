import os, json, base64, boto3

REGION = os.getenv("AWS_REGION", "ap-northeast-1")
TABLE  = os.getenv("JOBS_TABLE", "convert_jobs")

ddb = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)
kms = boto3.client("kms", region_name=REGION)

def lambda_handler(event, ctx):
    # Notifier から渡された Step Functions 入力のまま来る想定:
    # event = { "job_id": "...", "video_url": "...", "bucket": "...", "key": "..." }
    print("get_job", event)
    job_id = event["job_id"]
    r = ddb.get_item(Key={"job_id": job_id})
    item = r.get("Item")
    if not item:
        raise RuntimeError(f"job not found: {job_id}")

    token_cipher_b64 = item["token_cipher"]
    token = kms.decrypt(CiphertextBlob=base64.b64decode(token_cipher_b64))["Plaintext"].decode("utf-8")

    # 後段の HTTP タスク用に返す
    return {
        "job_id": job_id,
        "access_token": token,
        "ig_user_id": item.get("ig_user_id",""),
        "text": item.get("text", ""),
        "caption": item.get("caption", ""),
        "media_urls": item.get("media_urls", []),
        "wp_id": item.get("wp_id", ""),
        "site_url": item.get("site_url", "")
    }
