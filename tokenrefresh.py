import os
import sys
import requests
import winreg
import ctypes

API_URL = "https://cloud-api.youraspire.com/Authorization/RefreshToken"

def is_admin():
    """Check if the script is running with administrative privileges."""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False

def run_as_admin():
    """Relaunch the script with administrative privileges."""
    if is_admin():
        return
    ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)

def get_registry_env_variable(variable_name):
    """Retrieve an environment variable from the Windows registry."""
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment") as key:
            value, _ = winreg.QueryValueEx(key, variable_name)
            return value
    except FileNotFoundError:
        return None

def set_registry_env_variable(variable_name, value):
    """Set an environment variable in the Windows registry."""
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment", 0, winreg.KEY_SET_VALUE) as key:
            winreg.SetValueEx(key, variable_name, 0, winreg.REG_SZ, value)
    except Exception as e:
        print(f"Failed to set registry key {variable_name}: {e}")

def refresh_tokens():
    """Request new tokens using the refresh token."""
    refresh_token = get_registry_env_variable('REFRESH_TOKEN')
    if not refresh_token:
        print("Refresh token not found in the registry.")
        return

    payload = {
        "RefreshToken": refresh_token
    }
    headers = {
        'accept': "application/json;odata.metadata=minimal;odata.streaming=true",
        'Content-Type': "application/json;odata.metadata=minimal;odata.streaming=true"
    }

    try:
        response = requests.post(API_URL, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        new_token = data.get("Token")
        new_refresh_token = data.get("RefreshToken")

        if new_token and new_refresh_token:
            # Update the registry with new tokens
            set_registry_env_variable('AUTH_TOKEN', new_token)
            set_registry_env_variable('REFRESH_TOKEN', new_refresh_token)
        else:
            print("Failed to retrieve new tokens.")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the token refresh request: {e}")

# Main logic
if __name__ == "__main__":
    # Attempt to run with administrative privileges
    run_as_admin()

    # Refresh tokens
    refresh_tokens()
