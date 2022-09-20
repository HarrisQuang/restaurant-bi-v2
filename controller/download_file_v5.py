import requests
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'even-impulse-302623-7a2843af23d8.json'

credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
access_token = credentials.create_delegated(credentials._service_account_email).get_access_token().access_token

spreadsheet_id = '1dQ6ArCkx5g4hjazYsDci_w2BtJail2NNA6BlNxJ-aKw'
url = "https://www.googleapis.com/drive/v3/files/" + spreadsheet_id + "/export?mimeType=application%2Fvnd.openxmlformats-officedocument.spreadsheetml.sheet"
res = requests.get(url, headers={"Authorization": "Bearer " + access_token})

with open("sample.xlsx", 'wb') as f:
    f.write(res.content)