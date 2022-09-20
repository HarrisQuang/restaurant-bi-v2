import csv
import gspread
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'even-impulse-302623-7a2843af23d8.json'
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

docid = "1mU0VhTKePNyaVZlfM6CgxdczAHMVXm0nKOkafnoM7g0"

client = gspread.authorize(credentials)
spreadsheet = client.open_by_key(docid)
for i, worksheet in enumerate(spreadsheet.worksheets()):
    filename = docid + '-worksheet' + str(i) + '.csv'
    with open(filename, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(worksheet.get_all_values())