import gsheets
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'even-impulse-302623-7a2843af23d8.json'
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
url = 'https://docs.google.com/spreadsheets/d/1dQ6ArCkx5g4hjazYsDci_w2BtJail2NNA6BlNxJ-aKw/edit?usp=sharing'
sheets = gsheets.Sheets(credentials)
sheet = sheets.get(url)
sheet.to_csv(encoding='utf-16', dialect='excel', make_filename='%(title)s - %(sheet)s.csv')