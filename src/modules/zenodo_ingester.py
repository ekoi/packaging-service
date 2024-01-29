import json
from decouple import config
import requests

# Retrieve configurations from environment variables.
BASE_URL = config('ZENODO_BASE_URL')
DEPOSITION_ENDPOINT = config('DEPOSITION_ENDPOINT')
ACCESS_TOKEN = config('ZENODO_ACCESS_TOKEN')

class ZenodoUploader:
    def __init__(self, base_url, deposition_endpoint, access_token):
        # Initialize ZenodoUploader with API details.
        self.base_url = base_url
        self.deposition_endpoint = deposition_endpoint
        self.access_token = access_token

    def create_new_deposition(self, metadata):
        # Create a new deposition on Zenodo.
        url = f"{self.base_url}{self.deposition_endpoint}?access_token={self.access_token}"
        headers = {"Content-Type": "application/json"}

        data = json.dumps({"metadata": metadata})
        response = requests.post(url, data=data, headers=headers)

        if response.status_code == 201:
            return response.json()
        else:
            raise ValueError(f"Failed to create new deposition. Response: {response.text}")

    def upload_file_to_deposition(self, deposition_id, file):
        # Upload a file to an existing deposition on Zenodo.
        file_path = file.filename
        file.save(file_path)
        url = f"{self.base_url}{self.deposition_endpoint}/{deposition_id}/files?access_token={self.access_token}"
        data = {'name': file_path.split('/')[-1]}
        with open(file_path, 'rb') as fp:
            files = {'file': fp}
            response = requests.post(url, data=data, files=files)

        if response.status_code == 201:
            return response.json()
        else:
            raise ValueError(f"Failed to upload file. Response: {response.text}")

# Create an instance of the ZenodoUploader class.
uploader = ZenodoUploader(BASE_URL, DEPOSITION_ENDPOINT, ACCESS_TOKEN)


# Routes for the deposit and file upload.
@router.post('/create-deposition')
def create_deposition(request: Request):
    metadata = request.json.get('metadata')
    try:
        response = ZenodoUploader.create_new_deposition(metadata)
        return jsonable_encoder(response), 201
    except ValueError as e:
        return jsonable_encoder({'error': str(e)}), 400

@router.post('/upload-file/{deposition_id}')
def upload_file(deposition_id, file: UploadFile):
    if not file:
        return jsonable_encoder({'error': 'No file provided'}), 400

    try:
        response = ZenodoUploader.upload_file_to_deposition(deposition_id, file)
        return jsonable_encoder(response), 201
    except ValueError as e:
        return jsonable_encoder({'error': str(e)}), 400