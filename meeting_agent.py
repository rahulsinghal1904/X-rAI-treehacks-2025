import requests
import base64
import json

# Zoom API credentials (Replace with actual credentials)
CLIENT_ID = "4FD_KghQtOIWFxUmCpiNA"
CLIENT_SECRET = "sl5JhRfpBP8FndEDIWckxoIlGRizgqB0"
ACCOUNT_ID = "fyNkc61xSv2vvuCkxU545Q"

# OAuth Token URL
TOKEN_URL = "https://zoom.us/oauth/token"

# Zoom API URL (Use 'me' for authenticated user or specific Zoom user ID/email)
ZOOM_USER_ID = "me"
ZOOM_API_URL = f"https://api.zoom.us/v2/users/{ZOOM_USER_ID}/meetings"

def get_zoom_access_token():
    """Obtain an OAuth access token from Zoom"""
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "client_credentials"
    }

    response = requests.post(TOKEN_URL, headers=headers, data=payload)

    if response.status_code != 200:
        print("❌ Error fetching access token:", response.json())
        response.raise_for_status()

    token = response.json().get("access_token")
    print(token)
    print("✅ OAuth Access Token Retrieved:", token[:10] + "********")
    return token

def create_zoom_meeting(topic):
    """Create a Zoom meeting using OAuth authentication"""
    access_token = get_zoom_access_token()
    
    if not access_token:
        print("❌ Failed to get access token. Exiting.")
        return None

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "topic": topic,
        "type": 2,  # Scheduled meeting
        "start_time": "2025-02-16T10:00:00",  # ISO 8601 format
        "duration": 60,
        "timezone": "UTC",
        "agenda": "Meeting Agenda",
        "settings": {
            "host_video": True,
            "participant_video": True,
            "mute_upon_entry": True,
            "waiting_room": True
        }
    }

    response = requests.post(ZOOM_API_URL, json=payload, headers=headers)

    if response.status_code != 201:
        print("❌ Zoom API Error:", response.json())
        response.raise_for_status()

    meeting_info = response.json()
    print("✅ Zoom Meeting Created Successfully!")
    print("📅 Topic:", meeting_info["topic"])
    print("🔗 Join URL:", meeting_info["join_url"])
    print("🔑 Meeting ID:", meeting_info["id"])

    return meeting_info

if __name__ == "__main__":
    # Test creating a meeting
    meeting = create_zoom_meeting("Squad Readiness Review")

    if meeting:
        print("🎉 Meeting successfully created:", json.dumps(meeting, indent=4))
