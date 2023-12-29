from minio import Minio

# Create client with access and secret key
client = Minio('localhost:9000/',
               '62IJ8HBBMLT0SRTAJNP5',
               'ERPTUXBEd1pg8uvDMkkbiCUXTkaWsVSNkK+gQM9l',
                secure=False
               )

# check if bucket already exists
found = client.bucket_exists("mlops-playground")
# create bucket if it does not exist
if not found:
    client.make_bucket("mlops-playground")
else:
    print("Bucket 'mlops-playground' already exists")