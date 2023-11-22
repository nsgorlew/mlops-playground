from datetime import datetime, date
from io import StringIO

import boto3
import json
import os


class Persist:

    def __init__(self):
        pass

    @staticmethod
    def push(data):
        """
        persistence_path = f"{os.getcwd()}/{date.today()}"
        if local:
            if os.path.exists(persistence_path):
                with open(f"{persistence_path}/{data['appID']}_{datetime.now().strftime('%Y-%m-%d')}.json", "w") as f:
                    json.dump(data, f)
            else:
                os.mkdir(persistence_path, 0o666)
                with open(f"{persistence_path}/{data['appID']}_{datetime.now().strftime('%Y-%m-%d')}.json", "w") as f:
                    json.dump(data, f)

            return True
        """
        bucket = "mlops-playground"
        key = f"/{datetime.now().strftime('%Y-%m-%d')}/{data['appID']}.json"

        # using credentials for minio local server
        s3 = boto3.client("s3",
                          endpoint_url=os.environ["ENDPOINT_URL"],
                          aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                          aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
                          )
        s3.put_object(Body=json.dumps(data), Bucket=bucket, Key=key)
        return True

    @staticmethod
    def pull(bucket, key):
        s3 = boto3.client("s3",
                          endpoint_url=os.environ["ENDPOINT_URL"],
                          aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                          aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
                          )
        res = s3.get_object(Bucket=bucket, Key=key)
        return res["Body"].read()

    @staticmethod
    def push_training_testing_data(bucket, key, frame):
        # using credentials for minio local server
        s3 = boto3.client("s3",
                          endpoint_url=os.environ["ENDPOINT_URL"],
                          aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                          aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
                          )

        json_buffer = StringIO()

        frame.to_json(json_buffer, orient="records", lines=True)

        obj = s3.Object(Bucket=bucket, Key=key)
        result = obj.put(Body=json_buffer.getvalue())
        return result
