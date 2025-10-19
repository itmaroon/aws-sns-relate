import os, json, time, urllib.parse, tempfile, shutil, subprocess, boto3

AWS_REGION   = os.getenv("AWS_REGION", "ap-northeast-1")
UPLOAD_BUCKET= os.getenv("UPLOAD_BUCKET")
JOBS_TABLE   = os.getenv("JOBS_TABLE", "video_jobs_by_src")
FFMPEG       = "/opt/bin/ffmpeg"  # レイヤーの配置先

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table    = dynamodb.Table(JOBS_TABLE)
s3       = boto3.client("s3", region_name=AWS_REGION)

def _get_head_and_tags(bucket: str, key: str) -> tuple[dict, dict]:
    """HeadObject と Tagging を取得して dict 化して返す"""
    head = s3.head_object(Bucket=bucket, Key=key)
    # メタデータは小文字キーで返る (x-amz-meta-xxx → metadata["xxx"])
    metadata = head.get("Metadata", {})
    content_type = head.get("ContentType", "")

    tagset = s3.get_object_tagging(Bucket=bucket, Key=key).get("TagSet", [])
    tags = {t["Key"]: t["Value"] for t in tagset}

    info = {"content_type": content_type, "metadata": metadata}
    return info, tags

def _update_status(src_key, status, size_bytes=None, extra=None):
    update_expr = ["#s = :s", "updated_at = :t"]
    ean = {"#s": "status"}
    eav = {":s": status, ":t": int(time.time())}

    if size_bytes is not None:
        update_expr.append("size_bytes = :sz")
        eav[":sz"] = int(size_bytes)

    if extra:
        for i, (k_attr, v) in enumerate(extra.items()):
            ph = f":v{i}"
            update_expr.append(f"{k_attr} = {ph}")
            eav[ph] = v

    table.update_item(
        Key={"src_key": src_key},
        UpdateExpression="SET " + ", ".join(update_expr),
        ExpressionAttributeNames=ean,
        ExpressionAttributeValues=eav,
    )

def lambda_handler(event, ctx):
    
    # S3:ObjectCreated イベント想定
    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])

        # 早期フィルタ
        if UPLOAD_BUCKET and bucket != UPLOAD_BUCKET:
            print("skip other bucket:", bucket); continue
        
        # ここで HeadObject / Tagging を取得
        info, tags = _get_head_and_tags(bucket, key)
        print("INFO content_type:", info["content_type"])
        print("INFO metadata:", info["metadata"])
        print("INFO tags:", tags)
        #　metadata又はtagが空なら変換処理しない
        if info["metadata"]=={} and tags=={}:
            print("skip not tagged:", key); continue
        #　出力先がないなら変換処理しない
        if tags.get("out_key") == "":
            print("skip not uploaded:", key);
        # 出力先情報 
        dst_key = tags.get("out_key")
        dst_bucket = tags.get("out_bucket")

        # 作業ディレクトリ
        work = tempfile.mkdtemp(prefix="ffwork_", dir="/tmp")
        in_path  = os.path.join(work, "input.mp4")
        out_path = os.path.join(work, "output.mp4")

        try:
            print("[DL] s3://%s/%s -> %s" % (bucket, key, in_path))
            s3.download_file(bucket, key, in_path)
            md = info.get("metadata", {})  # {'params': '{"width":1080,...}'}
            params = {}

            raw = md.get("params")
            if raw:  # 文字列をdictへ
                try:
                    params = json.loads(raw)
                except Exception as e:
                    print("WARN: params JSON parse failed:", e)
                    params = {}

            width  = int(params.get("width", 1080))
            height = int(params.get("height", 1920))
            fps    = int(params.get("fps", 30))
            vbr    = str(params.get("video_bitrate", "5M"))
            abr    = str(params.get("audio_bitrate", "128k"))
            asr    = int(params.get("audio_samplerate", 44100))

            vf = (
                f"scale='min({width},iw)':'min({height},ih)':"
                "force_original_aspect_ratio=decrease,"
                "pad=ceil(iw/2)*2:ceil(ih/2)*2:(ow-iw)/2:(oh-ih)/2"
            )

            cmd = [
                FFMPEG, "-y",
                "-i", in_path,
                "-vf", vf,
                "-r", str(fps),
                "-c:v", "libx264",
                "-profile:v", "high", "-level", "4.1",
                "-pix_fmt", "yuv420p",
                "-b:v", vbr, "-maxrate", vbr, "-bufsize", "10M",
                "-g", str(max(1, fps*2)),
                "-c:a", "aac", "-b:a", abr, "-ar", str(asr),
                "-movflags", "+faststart",
                out_path
            ]
            print("[CMD]", " ".join(cmd));
            t0 = time.time()
            rc = subprocess.run(cmd, capture_output=True, text=True).returncode
            print("[FFMPEG] rc=", rc, "elapsed=", round(time.time()-t0,2), "s")
            if rc != 0 or not os.path.exists(out_path):
                _update_status(k, "error")
                # 直近のエラーメッセージをログ
                print("[ERR] ffmpeg failed")
                continue

            print("[UL] %s -> s3://%s/%s" % (out_path, dst_bucket, dst_key))
            # metadataを渡す
            out_meta={}
            # job-id（必須）
            job_id = md.get("job-id")
            if job_id:
                out_meta["job-id"] = job_id
            # ← Webhook の Base64
            cb_b64  = md.get("cb-b64")                  
            if cb_b64:
                out_meta["cb-b64"] = cb_b64
                
            s3.upload_file(
                out_path, 
                dst_bucket, 
                dst_key, 
                ExtraArgs={
                    "ContentType":"video/mp4",
                    "Metadata": out_meta,  
                }
            )
            # S3の実体を確認（サイズ・ETag・ContentTypeなど取れる）
            head = s3.head_object(Bucket=dst_bucket, Key=dst_key)
            size_bytes = head["ContentLength"]
            content_type = head.get("ContentType")
            etag = head.get("ETag")
            print("HEAD:", content_type, size_bytes, etag)
            _update_status(
                key, 
                "done",
                size_bytes=size_bytes,
                extra={"content_type": content_type, "etag": etag},
            )

        finally:
            try:
                shutil.rmtree(work)
            except Exception as e:
                print("cleanup warn:", e)
