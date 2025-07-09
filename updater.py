import requests
import zipfile
import os
import shutil

LOCAL_VERSION = "1.0"
REMOTE_VERSION_URL = "https://YOUR_GITHUB_USERNAME.github.io/metrika-logs-updater/version.txt"
UPDATE_ZIP_URL = "https://YOUR_GITHUB_USERNAME.github.io/metrika-logs-updater/MetrikaLogsDownloader_v1.0.zip"

def get_remote_version():
    try:
        response = requests.get(REMOTE_VERSION_URL)
        response.raise_for_status()
        return response.text.strip()
    except Exception as e:
        print(f"Ошибка при проверке версии: {e}")
        return None

def download_update(destination_folder):
    try:
        response = requests.get(UPDATE_ZIP_URL, stream=True)
        with open('update.zip', 'wb') as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        with zipfile.ZipFile('update.zip', 'r') as zip_ref:
            zip_ref.extractall(destination_folder)
        os.remove('update.zip')
        return True
    except Exception as e:
        print(f"Ошибка при загрузке обновления: {e}")
        return False
