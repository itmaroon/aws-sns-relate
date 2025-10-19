import os
import json
import boto3
import urllib.parse

REGION = os.getenv("AWS_REGION", "ap-northeast-1")
JOBS_TABLE = os.getenv("JOBS_TABLE", "convert_jobs")
IN_BUCKET = os.getenv("IN_BUCKET")
OUT_BUCKET = os.getenv("OUT_BUCKET")

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION).Table(JOBS_TABLE)


def _safe(fn, *a, **k):
    try:
        return fn(*a, **k)
    except Exception as e:
        print("WARN:", fn.__name__, "failed:", e)
        raise


def _extract_s3_key_from_url(url: str):
    if not url:
        return None
    try:
        parsed = urllib.parse.urlparse(url)
        return urllib.parse.unquote_plus(parsed.path.lstrip("/"))
    except Exception as e:
        print("WARN extract_s3_key_from_url:", e)
        return None


def lambda_handler(event, ctx):
    """
    Step Functions および API Gateway 両対応
    """
    out_bucket = event.get("bucket") or OUT_BUCKET
    out_key = event.get("key")
    job = event.get("job") or {}
    job_id = job.get("job_id") or event.get("job_id")

    qp = event.get("queryStringParameters") or {}
    media_path = qp.get("media_path")

    deleted = {"out": False, "src": False, "batch": []}
    failure = None

    try:
        # --- (1) 単発削除 ---
        if media_path:
            media_path = urllib.parse.unquote_plus(media_path)
            _safe(s3.delete_object, Bucket=IN_BUCKET, Key=media_path)
            deleted["src"] = True
            return {"deleted": deleted, "src_bucket": IN_BUCKET, "src_key": media_path}

        # --- (2) DynamoDB参照モード ---
        if job_id:
            try:
                r = ddb.get_item(Key={"job_id": job_id})
                item = r.get("Item") or {}
                media_urls = item.get("media_urls", [])
                print(f"JOB {job_id} media_urls:", media_urls)
                # URL配列指定による削除（X投稿対応）
                for url in media_urls:
                    key = _extract_s3_key_from_url(url)
                    if not key:
                        continue
                    _safe(s3.delete_object, Bucket=IN_BUCKET, Key=key)
                    deleted["batch"].append({"bucket": IN_BUCKET, "key": key})
                # in_key指定による削除（Insta対応）
                in_key = item.get("in_key", '')
                if in_key:
                    _safe(s3.delete_object, Bucket=IN_BUCKET, Key=in_key)

                deleted["src"] = True

            except Exception as e:
                failure = f"DDB read/delete failed: {e}"
                print("ERROR:", failure)

        # --- (3) 出力側の削除 ---
        if out_bucket and out_key:
            try:
                _safe(s3.delete_object, Bucket=out_bucket, Key=out_key)
                deleted["out"] = True
            except Exception as e:
                failure = f"Delete output object failed: {e}"
                print("ERROR:", failure)

    except Exception as e:
        failure = f"General failure: {e}"
        print("FATAL:", failure)

    # --- (4) 戻り値構成 ---
    result = {
        "deleted": deleted,
        "src_bucket": IN_BUCKET,
        "out_bucket": out_bucket,
        "job_id": job_id,
    }
    if failure:
        result["failure"] = failure

    return result
