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
                          endpoint_url="http://localhost:9000",
                          aws_access_key_id="fPa43Tw12ozMPvHo78QO",
                          aws_secret_access_key="q95TYfQvZH1PUFYaPftk5nbnP3TWl59MtXgyY6K8"
                          )
        s3.put_object(Body=json.dumps(data), Bucket=bucket, Key=key)
        return True

    @staticmethod
    def pull(bucket, key):
        s3 = boto3.client("s3",
                          endpoint_url="http://localhost:9000",
                          aws_access_key_id="fPa43Tw12ozMPvHo78QO",
                          aws_secret_access_key="q95TYfQvZH1PUFYaPftk5nbnP3TWl59MtXgyY6K8"
                          )
        res = s3.get_object(Bucket=bucket, Key=key)
        return res["Body"].read()

    @staticmethod
    def push_training_testing_data(bucket, key, frame):
        # using credentials for minio local server
        s3 = boto3.client("s3",
                          endpoint_url="http://localhost:9000",
                          aws_access_key_id="fPa43Tw12ozMPvHo78QO",
                          aws_secret_access_key="q95TYfQvZH1PUFYaPftk5nbnP3TWl59MtXgyY6K8"
                          )

        json_buffer = StringIO()

        frame.to_json(json_buffer, orient="records", lines=True)

        obj = s3.Object(Bucket=bucket, Key=key)
        result = obj.put(Body=json_buffer.getvalue())
        return result
