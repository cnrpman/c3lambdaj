import json

max_write_limit = 50

def lambda_worker(lambda_client, lambda_arn, s3_route, target, callback):
    """
    thread-safety reference:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html?highlight=multithreading#multithreading-and-multiprocessing
    """
    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload='"'+s3_route+'"'
    )

    body = response.get("Payload")
    raw_bytes = body.read()
    obj = json.loads(raw_bytes)
    body.close()

    with open(target, "w") as f:
        f.write(obj)

    callback()