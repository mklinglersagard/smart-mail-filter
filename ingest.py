import base64
import os
from google.cloud import pubsub_v1, aiplatform
from google.oauth2 import service_account
from googleapiclient.discovery import build
from functions_framework import cloud_event

# Initialize Vertex AI
aiplatform.init(
    project=os.environ['GCP_PROJECT'],
    location=os.environ.get('FUNCTION_REGION', 'us-central1'),
)

# Load per-user prompt from Firestore (omitted here for brevity)

@cloud_event
def ingest_email(cloud_event):
    # 1. Decode Pub/Sub message
    data = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    msg_id = data  # assume payload is the message ID
    # 2. Fetch full email via Gmail API
    creds = service_account.Credentials.from_service_account_file(
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
        scopes=['https://www.googleapis.com/auth/gmail.readonly']
    )
    gmail = build('gmail', 'v1', credentials=creds)
    msg = gmail.users().messages().get(
        userId='me', id=msg_id, format='full'
    ).execute()
    # 3. (Optional) Pre‑filter with regex or header checks to save tokens
    #    e.g., skip "List-Unsubscribe" threads, promotions, etc.
    # 4. Construct LLM prompt
    user_prompt = os.environ.get('USER_PROMPT', 'Decide if this email is YES or NO:')
    content = msg['snippet']
    # 5. Call Vertex AI for classification
    endpoint = aiplatform.Endpoint(os.environ['LLM_ENDPOINT_ID'])
    response = endpoint.predict(instances=[{"prompt": user_prompt + "\n\n" + content}])
    decision = response.predictions[0].get('keep', False)
    # 6. Publish to post-filter topic with decision payload
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.environ['GCP_PROJECT'], 'mail-action')
    payload = f"{msg_id}|{decision}"
    publisher.publish(topic_path, payload.encode())
