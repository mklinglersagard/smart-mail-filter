
# ingest_email/main.py
import os
import base64
import json
import google.auth
from google.cloud import pubsub_v1, aiplatform
from googleapiclient.discovery import build

# Environment variables
BOX_LABEL_ID = os.environ["BOX_LABEL_ID"]
LLM_MODEL = os.environ["LLM_MODEL"]
GCP_PROJECT = os.environ.get("GCP_PROJECT") or google.auth.default()[1]
GMAIL_USER = os.environ.get("GMAIL_USER", "me")

# Initialize Vertex AI
aiplatform.init(project=GCP_PROJECT, location="us-central1")
model = aiplatform.TextGenerationModel.from_pretrained(LLM_MODEL)

def ingest_email(event, context):
    """Triggered by Pub/Sub topic 'mail-in'."""
    try:
        # Parse Pub/Sub message
        payload = base64.b64decode(event["data"]).decode("utf-8")
        data = json.loads(payload)
        msg_id = data["id"]

        # Build Gmail service
        credentials, _ = google.auth.default()
        service = build("gmail", "v1", credentials=credentials)

        # Retrieve the email message
        message = service.users().messages().get(
            userId=GMAIL_USER, id=msg_id, format="full"
        ).execute()

        # Extract plaintext body
        text_body = ""
        for part in message.get("payload", {}).get("parts", []):
            if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
                text_body += base64.urlsafe_b64decode(
                    part["body"]["data"].encode("utf-8")
                ).decode("utf-8")

        # Generate classification via LLM
        prompt = f"Classify this email: {text_body}"
        response = model.predict(
            prompt,
            temperature=0.2,
            max_output_tokens=512
        )
        classification = response.text.strip().lower()

        # Apply label if classification is 'keep'
        if classification == "keep":
            service.users().messages().modify(
                userId=GMAIL_USER,
                id=msg_id,
                body={"addLabelIds": [BOX_LABEL_ID]},
            ).execute()

        # Publish to next topic
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(GCP_PROJECT, "mail-action")
        publisher.publish(topic_path, data=json.dumps({"id": msg_id}).encode("utf-8"))

        return "OK"
    except Exception as e:
        print(f"Error in ingest_email: {e}")
        raise


# post_filter/main.py
import os
import base64
import json
import google.auth
from googleapiclient.discovery import build

# Environment variables
BOX_LABEL_ID = os.environ["BOX_LABEL_ID"]
GCP_PROJECT = os.environ.get("GCP_PROJECT") or google.auth.default()[1]
GMAIL_USER = os.environ.get("GMAIL_USER", "me")

def post_filter(event, context):
    """Triggered by Pub/Sub topic 'mail-action'."""
    try:
        # Parse Pub/Sub message
        payload = base64.b64decode(event["data"]).decode("utf-8")
        data = json.loads(payload)
        msg_id = data["id"]

        # Build Gmail service
        credentials, _ = google.auth.default()
        service = build("gmail", "v1", credentials=credentials)

        # Remove the BOX_LABEL_ID if present
        service.users().messages().modify(
            userId=GMAIL_USER,
            id=msg_id,
            body={"removeLabelIds": [BOX_LABEL_ID]},
        ).execute()

        return "OK"
    except Exception as e:
        print(f"Error in post_filter: {e}")
        raise



