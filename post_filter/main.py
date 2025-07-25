
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from functions_framework import cloud_event

@cloud_event
def post_filter(cloud_event):
    data = cloud_event.data["message"]["data"].decode()
    msg_id, keep_str = data.split('|')
    keep = keep_str == 'True'
    creds = service_account.Credentials.from_service_account_file(
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
        scopes=['https://www.googleapis.com/auth/gmail.modify']
    )
    gmail = build('gmail', 'v1', credentials=creds)
    mods = {
        'removeLabelIds': [] if keep else ['INBOX'],
        'addLabelIds': ['INBOX'] if keep else [os.environ['BOX_LABEL_ID']],
    }
    # Apply labels atomically
    gmail.users().messages().modify(
        userId='me', id=msg_id, body=mods
    ).execute()