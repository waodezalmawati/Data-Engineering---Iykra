from google.cloud import storage
import wget
import io, os, json

from dotenv import load_dotenv
load_dotenv()

CREDENTIALS = json.loads(os.environ.get('CREDENTIALS'))

if os.path.exists('credentials.json'):
    pass
else:
    with open('credentials.json', 'w') as credFile:
        json.dump(CREDENTIALS, credFile)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'

"""Uploads a file to the bucket."""
# The ID of your GCS bucket
bucket_name = "de-fellowship-7"

# The path to your file to upload
source_file_name = "https://i.pinimg.com/564x/c2/e0/75/c2e075bf8c8c1faec0567269afbd76b4.jpg"

# The ID of your GCS object
destination_blob_name = "task1-img-destination"

def upload_file(bucket_name, source_file_name, destination_blob_name):
    filename = wget.download(source_file_name)
    # bucket = storage_client.get_bucket(bucket_name)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(filename, content_type='image/jpg')
    os.remove(filename)

upload_file(bucket_name, source_file_name, destination_blob_name)

# storage_client = storage.Client()
# bucket = storage_client.bucket(bucket_name)
# blob = bucket.blob(destination_blob_name)

# blob.upload_from_filename(source_file_name)

print(
    "File {} uploaded to {}.".format(
        source_file_name, destination_blob_name
    )
)




# from google.cloud import storage
# import wget
# import io, os

# project_id = 'ace-mile-362915'
# bucket_name = 'de-fellowship-7'
# destination_blob_name = 'docker_upload'
# storage_client = storage.Client.from_service_account_json('my_creds.json')

# source_file_name = 'https://i.pinimg.com/564x/c2/e0/75/c2e075bf8c8c1faec0567269afbd76b4.jpg'

# def upload_file(bucket_name, source_file_name, destination_blob_name):
#     filename = wget.download(source_file_name)
#     bucket = storage_client.get_bucket(bucket_name)
#     blob = bucket.blob(destination_blob_name)
#     blob.upload_from_filename(filename, content_type='image/jpg')
#     os.remove(filename)

# upload_file(bucket_name, source_file_name, destination_blob_name)
