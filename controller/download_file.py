from __future__ import print_function

import io

import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'even-impulse-302623-7a2843af23d8.json'
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)


def download_file(real_file_id, cred):
    """Downloads a file
    Args:
        real_file_id: ID of the file to download
    Returns : IO object with location.

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    # creds, _ = google.auth.default()

    try:
        # create drive api client
        service = build('drive', 'v3', credentials=cred)

        file_id = real_file_id

        # pylint: disable=maybe-no-member
        # request = service.files().get_media(fileId=file_id)
        request = service.files().export_media(fileId=file_id, mimeType='text/csv')
        # file = io.BytesIO()
        file = io.FileIO('Spam.csv', mode='wb')
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(F'Download {int(status.progress() * 100)}.')

    except HttpError as error:
        print(F'An error occurred: {error}')
        file = None


if __name__ == '__main__':
    download_file(real_file_id='1mU0VhTKePNyaVZlfM6CgxdczAHMVXm0nKOkafnoM7g0', cred=credentials)