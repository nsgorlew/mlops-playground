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
                          endpoint_url="http://localhost:9000",
                          aws_access_key_id="fPa43Tw12ozMPvHo78QO",
                          aws_secret_access_key="q95TYfQvZH1PUFYaPftk5nbnP3TWl59MtXgyY6K8"
                          )
        s3.put_object(Body=json.dumps(data), Bucket=bucket, Key=key)
        return True
