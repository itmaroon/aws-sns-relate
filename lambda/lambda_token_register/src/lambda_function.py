import json
import boto3
import hashlib
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('video-converter-tokens')

def lambda_handler(event, context):
    """Facebookページトークンを登録"""
    
    try:
        body = json.loads(event['body'])
        
        facebook_page_token = body.get('facebook_page_token')
        facebook_page_id = body.get('facebook_page_id')
        site_url = body.get('site_url')
        
        if not facebook_page_token:
            return response(400, {'error': 'facebook_page_token is required'})
        
        # ユーザーIDを生成 (サイトURLのハッシュを使用)
        user_id = 'usr_' + hashlib.sha256(site_url.encode()).hexdigest()[:16]
        
        # 既存のトークンをチェック
        existing = table.get_item(Key={'facebook_page_token': facebook_page_token})
        
        if 'Item' in existing:
            # 既存のトークン - 更新
            table.update_item(
                Key={'facebook_page_token': facebook_page_token},
                UpdateExpression='SET last_used_at = :now, site_url = :url, facebook_page_id = :page_id',
                ExpressionAttributeValues={
                    ':now': datetime.utcnow().isoformat() + 'Z',
                    ':url': site_url,
                    ':page_id': facebook_page_id
                }
            )
            
            return response(200, {
                'success': True,
                'message': 'Token updated',
                'user_id': existing['Item']['user_id']
            })
        
        # 新規トークン - 登録
        item = {
            'facebook_page_token': facebook_page_token,
            'user_id': user_id,
            'site_url': site_url,
            'facebook_page_id': facebook_page_id,
            'tier': 'basic',  # デフォルトプラン
            'monthly_limit': 100,
            'monthly_usage': 0,
            'is_active': True,
            'created_at': datetime.utcnow().isoformat() + 'Z',
            'last_used_at': datetime.utcnow().isoformat() + 'Z',
            'expires_at': (datetime.utcnow() + timedelta(days=365)).isoformat() + 'Z'
        }
        
        table.put_item(Item=item)
        
        return response(201, {
            'success': True,
            'message': 'Token registered successfully',
            'user_id': user_id,
            'tier': 'basic',
            'monthly_limit': 100
        })
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return response(500, {'error': 'Internal server error'})

def response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(body)
    }
