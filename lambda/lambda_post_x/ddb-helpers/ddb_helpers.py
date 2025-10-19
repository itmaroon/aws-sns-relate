# ddb_helpers.py
import os, time, boto3

REGION = os.getenv("AWS_REGION", "ap-northeast-1")
JOBS_TABLE = os.getenv("JOBS_TABLE", "convert_jobs")
_ddb = boto3.resource("dynamodb", region_name=REGION).Table(JOBS_TABLE)

def set_status(job_id: str, status: str):
    """convert_jobs[job_id].status を一発更新（updated_at も付与）"""
    if not job_id:
        return
    _ddb.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET #s = :s, updated_at = :u",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": status, ":u": int(time.time())},
    )
