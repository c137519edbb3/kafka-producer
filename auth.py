import requests
from config import Config

class Auth:
    def __init__(self):
        self.api_url = Config.EYECONAI_API_URL
        self.username = Config.EYECONAI_USERNAME
        self.password = Config.EYECONAI_PASSWORD
        self.token = None
        self.user = None

    def login(self):
        try:
            response = requests.post(
                f"{self.api_url}/api/auth/login",
                json={
                    "username": self.username,
                    "password": self.password
                }
            )
            response.raise_for_status()
            data = response.json()
            
            self.token = data.get('token')
            self.user = data.get('user')
            
            print("Successfully authenticated with EyeconAI")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"Authentication failed: {str(e)}")
            return False
        
    def get_online_cameras(self):
        try:
            if not self.token or not self.user:
                print("Not authenticated. Please login first.")
                return None

            org_id = self.user.get("organization", {}).get("id")
            if not org_id:
                print("Organization ID not found in user data")
                return None

            headers = {
                "Authorization": f"Bearer {self.token}"
            }

            response = requests.get(
                f"{self.api_url}/api/organization/{org_id}/cameras/online",
                headers=headers
            )
            response.raise_for_status()
            
            return response.json().get("cameras", [])
            
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch cameras: {str(e)}")
            return None