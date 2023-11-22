from datetime import datetime, date

import boto3
import json
import os


class Persist:

    def __init__(self):
        pass

    @staticmethod
    def push(data, local=False):
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
