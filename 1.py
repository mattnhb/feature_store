import json

def lambda_handler(event, context):

    from boto3 import client

    conn = client('s3')  # again assumes boto.cfg setup, assume AWS S3
    response = conn.list_objects_v2(
        Bucket='mattnrepl',
        Prefix='aquiteste/testelistbu/')
    print(response)
    files = [content['Key'] for content in response.get('Contents', [])]
    return {
        "len": len(files),
        "files": files
    }
 
