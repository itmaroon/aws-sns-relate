# lambda_get_job_status.py (Python 3.11)
import os
import json
import boto3
from boto3.dynamodb.conditions import Key

REGION = os.getenv("AWS_REGION", "ap-northeast-1")
TABLE = os.getenv("JOBS_TABLE", "convert_jobs")
# site_urlベースのGSI（例: site_url-updated_at-index）
GSI_SITEURL = os.getenv("JOBS_GSI_SITEURL", "site_url-updated_at-index")

ddb = boto3.resource("dynamodb", region_name=REGION)
table = ddb.Table(TABLE)

def _resp(code, body):
    return {
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "GET, OPTIONS"
        },
        "body": json.dumps(body, ensure_ascii=False)
    }

def lambda_handler(event, ctx):
    """
    /jobs?site_url=https://example.com
    または /jobs/{site_url} に対応。
    site_url に一致する全ジョブを返す。
    """
    qp = event.get("queryStringParameters") or {}
    path = event.get("pathParameters") or {}

    site_url = qp.get("site_url") or path.get("site_url")
    if not site_url:
        return _resp(400, {"error": "site_url required"})

    try:
        # GSIを使ってsite_urlで絞り込み、全件取得（更新日時降順）
        response = table.query(
            IndexName=GSI_SITEURL,
            KeyConditionExpression=Key("site_url").eq(site_url),
            ScanIndexForward=False  # 降順（最新が最初）
        )
        items = response.get("Items", [])

        # ページング対応：LastEvaluatedKeyがある場合は繰り返し取得
        while "LastEvaluatedKey" in response:
            response = table.query(
                IndexName=GSI_SITEURL,
                KeyConditionExpression=Key("site_url").eq(site_url),
                ExclusiveStartKey=response["LastEvaluatedKey"],
                ScanIndexForward=False
            )
            items.extend(response.get("Items", []))

        if not items:
            return _resp(404, {"error": "no jobs found for site_url", "site_url": site_url})

        # 必要なフィールドだけ返却
        body = [
            {
                "job_id": it.get("job_id", ""),
                "wp_id": it.get("wp_id", ""),
                "status": it.get("status", ""),
                "updated_at": int(it.get("updated_at", 0)),
                "media_id": it.get("media_id", ""),
                "platform": it.get("platform", ""),  # 例: "x" / "instagram"
            }
            for it in items
        ]

        return _resp(200, {"site_url": site_url, "jobs": body})

    except Exception as e:
        return _resp(500, {"error": str(e)})
