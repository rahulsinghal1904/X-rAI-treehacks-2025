#!/usr/bin/env python
"""
meeting_agent_oauth.py
Simulates the Meeting Agent using Zoom API with OAuth.
For demo purposes, returns a dummy meeting confirmation.
"""

import time
import requests
from requests.auth import HTTPBasicAuth

# Zoom API credentials
CLIENT_ID = "4FD_KghQtOIWFxUmCpiNA"
CLIENT_SECRET = "sl5JhRfpBP8FndEDIWckxoIlGRizgqB0"
ACCOUNT_ID = "fyNkc61xSv2vvuCkxU545Q"
ZOOM_API_URL = "https://api.zoom.us/v2/users/me/meetings"
TOKEN_URL = f"https://zoom.us/oauth/token"

def get_zoom_access_token():
    """Obtain an OAuth access token from Zoom"""
    payload = {
        "grant_type": "account_credentials",
        "account_id": ACCOUNT_ID,
    }
    response = requests.post(TOKEN_URL, auth=HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET), data=payload)
    response.raise_for_status()
    return response.json().get("access_token")

def create_zoom_meeting(topic):
    """Create a Zoom meeting using OAuth authentication"""
    access_token = get_zoom_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "topic": topic,
        "type": 2,  # Scheduled meeting
        "start_time": "2025-02-16T10:00:00Z",
        "duration": 60,
        "timezone": "UTC",
        "settings": {
            "host_video": True,
            "participant_video": True,
        }
    }
    response = requests.post(ZOOM_API_URL, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    # Example usage
    meeting = create_zoom_meeting("Squad Readiness Review")
    print("Zoom Meeting:", meeting)
