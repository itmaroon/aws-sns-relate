import os
import json
import urllib.request
import urllib.error
import io

CHUNK_SIZE = 4 * 1024 * 1024  # 4MB（WordPress版と合わせる）

def lambda_handler(event, context):
    """
    Append media data to X (v2 API) upload session
    Endpoint: https://api.x.com/2/media/upload/{media_id}/append
    """

    access_token = event.get("access_token")
    media_id = event.get("media_id")
    media_url = event.get("media_url")
    job_id = event.get("job_id", "")
    media_type = event.get("media_type", "application/octet-stream")

    if not all([access_token, media_id, media_url]):
        return {"error": "missing required parameters"}

    try:
        # ===== メディアデータを取得 (S3 Presigned URL など) =====
        with urllib.request.urlopen(media_url, timeout=60) as resp:
            data = resp.read()

        total_bytes = len(data)
        segment_index = 0
        offset = 0

        while offset < total_bytes:
            chunk = data[offset:offset + CHUNK_SIZE]
            if not chunk:
                break

            endpoint = f"https://api.x.com/2/media/upload/{media_id}/append"
            boundary = "----itmarBoundary"
            eol = "\r\n"

            # multipart/form-data 組み立て
            body = io.BytesIO()
            body.write(f"--{boundary}{eol}".encode("utf-8"))
            body.write(b'Content-Disposition: form-data; name="segment_index"')
            body.write(f"{eol}{eol}{segment_index}{eol}".encode("utf-8"))

            body.write(f"--{boundary}{eol}".encode("utf-8"))
            body.write(
                f'Content-Disposition: form-data; name="media"; filename="part{segment_index}"{eol}'.encode("utf-8")
            )
            body.write(f"Content-Type: {media_type}{eol}{eol}".encode("utf-8"))
            body.write(chunk)
            body.write(f"{eol}--{boundary}--{eol}".encode("utf-8"))
            body_bytes = body.getvalue()

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Content-Length": str(len(body_bytes)),
            }

            req = urllib.request.Request(endpoint, data=body_bytes, headers=headers, method="POST")

            try:
                with urllib.request.urlopen(req, timeout=60) as res:
                    code = res.status
                    raw = res.read().decode("utf-8")
            except urllib.error.HTTPError as e:
                raw = e.read().decode("utf-8")
                return {
                    "error": f"append_failed: HTTP {e.code}",
                    "segment_index": segment_index,
                    "response": raw,
                    "endpoint": endpoint,
                }

            if code < 200 or code >= 300:
                return {
                    "error": f"append_failed: HTTP {code}",
                    "segment_index": segment_index,
                    "response": raw,
                    "endpoint": endpoint,
                }

            # 次チャンクへ
            offset += CHUNK_SIZE
            segment_index += 1

        return {
            "status": "appended",
            "job_id": job_id,
            "media_id": media_id,
            "uploaded_segments": segment_index,
            "total_bytes": total_bytes,
            "media_type": media_type,
            "access_token": access_token
        }

    except urllib.error.URLError as e:
        return {"error": f"network_error: {str(e)}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}
