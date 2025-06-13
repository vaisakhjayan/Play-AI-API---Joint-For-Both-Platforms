import os
import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import pickle
import os.path
import json
import requests
from google.auth.transport.requests import AuthorizedSession
from platformconfig import get_celebrity_vo_path

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']
FOLDER_ID = '1FvOdQjUNxOcZTpO2DnQf4JIFLeBohQiG'
LOCAL_FOLDER = get_celebrity_vo_path()
TOKEN_PATH = 'token.pickle'
CREDENTIALS_PATH = 'client_secret_410229145844-csaacms0ba2tkcgcv8efbsiojrr2app9.apps.googleusercontent.com.json'
NOTION_DATABASE_ID = "1e502cd2c14280ca81e8ff63dad7f3ae"
NOTION_API_KEY = "ntn_cC7520095381SElmcgTOADYsGnrABFn2ph1PrcaGSst2dv"

def update_notion_page(title, drive_link):
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Get all pages from database
    query_url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    response = requests.post(query_url, headers=headers, json={"page_size": 100})
    
    if response.status_code != 200:
        print(f"Failed to query database: {response.text}")
        return False
        
    # Find the page with matching title
    for page in response.json()['results']:
        try:
            # Safely get the title
            title_property = page['properties'].get('New Title', {}).get('title', [])
            if not title_property:
                print(f"Skipping page {page['id']}: No title found")
                continue
                
            page_title = title_property[0].get('text', {}).get('content', '')
            if not page_title:
                print(f"Skipping page {page['id']}: Empty title")
                continue
                
            if title in page_title:  # Using 'in' for partial match
                # Update the page
                update_url = f"https://api.notion.com/v1/pages/{page['id']}"
                update_data = {
                    "properties": {
                        "Voice Drive Link": {"url": f"https://drive.google.com/file/d/{drive_link}/view"},
                        "Voiceover": {"checkbox": True}
                    }
                }
                
                update_response = requests.patch(update_url, headers=headers, json=update_data)
                if update_response.status_code == 200:
                    print(f"Successfully updated page: {page_title}")
                    return True
                else:
                    print(f"Failed to update page {page_title}: {update_response.text}")
                    
        except Exception as e:
            print(f"Error processing page {page.get('id', 'unknown')}: {str(e)}")
            continue
                
    print(f"No matching page found for title: {title}")
    return False

def get_content_title():
    try:
        with open('JSON Files/content.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            if data.get('records') and len(data['records']) > 0:
                title = data['records'][0].get('title')
                if not title:
                    print("No title found in content.json")
                    return None
                print(f"Found title in content.json: {title}")
                return title
    except FileNotFoundError:
        print("content.json file not found")
    except json.JSONDecodeError:
        print("Error decoding content.json")
    except Exception as e:
        print(f"Error reading content.json: {e}")
    return None

def get_credentials():
    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Error refreshing credentials: {e}")
                creds = None
                if os.path.exists(TOKEN_PATH):
                    os.remove(TOKEN_PATH)
                    
        if not creds or not creds.valid:
            if not os.path.exists(CREDENTIALS_PATH):
                raise Exception(f"Credentials file not found: {CREDENTIALS_PATH}")
                
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
            
            # Save the credentials for the next run
            with open(TOKEN_PATH, 'wb') as token:
                pickle.dump(creds, token)
    
    return creds

def get_latest_audio_file(folder_path):
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) 
             if os.path.isfile(os.path.join(folder_path, f)) and f.lower().endswith('.wav')]
    
    if not files:
        return None
    
    return max(files, key=os.path.getmtime)

def upload_file(service, file_path, folder_id):
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    
    print(f'Starting upload of {file_name} ({file_size / (1024*1024):.1f} MB)')
    
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    
    # Use different upload strategies based on file size
    if file_size < 10 * 1024 * 1024:  # Less than 10MB - use simple upload
        print("Using simple upload for small file...")
        media = MediaFileUpload(
            file_path,
            mimetype='audio/wav',
            resumable=False
        )
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
    else:  # Larger files - use resumable with aggressive chunks for high-speed connections
        print("Using high-speed resumable upload optimized for 1Gbps+...")
        # Calculate optimal chunk size: larger chunks for bigger files and faster connections
        # For 1Gbps connections, use much larger chunks
        if file_size > 100 * 1024 * 1024:  # Files > 100MB
            chunk_size = 64 * 1024 * 1024  # 64MB chunks
        elif file_size > 50 * 1024 * 1024:  # Files > 50MB
            chunk_size = 32 * 1024 * 1024  # 32MB chunks
        else:
            chunk_size = 16 * 1024 * 1024  # 16MB chunks
            
        print(f"Using {chunk_size // (1024*1024)}MB chunks for maximum speed...")
        
        media = MediaFileUpload(
            file_path,
            mimetype='audio/wav',
            resumable=True,
            chunksize=chunk_size
        )
        
        request = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        )
        
        file = None
        retry_count = 0
        max_retries = 3
        last_progress = 0
        
        while file is None and retry_count < max_retries:
            try:
                status, file = request.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    # Only report progress every 5% to reduce I/O overhead
                    if progress >= last_progress + 5:
                        print(f"Upload progress: {progress}%")
                        last_progress = progress
                    
            except Exception as chunk_error:
                retry_count += 1
                print(f"Upload chunk failed (attempt {retry_count}/{max_retries}): {str(chunk_error)}")
                if retry_count >= max_retries:
                    raise chunk_error
                print("Retrying in 2 seconds...")
                import time
                time.sleep(2)
    
    print(f'Successfully uploaded file: {file_name}')
    file_id = file.get("id")
    print(f'File ID: {file_id}')
    
    # Get the title from content.json and update Notion
    content_title = get_content_title()
    if content_title:
        if update_notion_page(content_title, file_id):
            print(f"Successfully updated Notion page for: {content_title}")
        else:
            print(f"Failed to update Notion page for: {content_title}")
    else:
        print("Could not update Notion: No title found in content.json")
    
    return file_id

def main():
    try:
        # Get credentials
        creds = get_credentials()
        if not creds:
            raise Exception("Failed to obtain valid credentials")
            
        # Create the Drive API service
        service = build('drive', 'v3', credentials=creds)
        
        # Get the latest audio file
        latest_file = get_latest_audio_file(LOCAL_FOLDER)
        
        if latest_file:
            print(f'Found latest file: {os.path.basename(latest_file)}')
            file_size_mb = os.path.getsize(latest_file) / (1024 * 1024)
            print(f'File size: {file_size_mb:.1f} MB')
            
            # Set timeout for upload (10 seconds per MB, min 2 minutes)
            timeout_seconds = max(120, int(file_size_mb * 10))
            print(f'Upload timeout set to: {timeout_seconds // 60} minutes')
            
            # Use threading to implement timeout
            import threading
            import time
            
            upload_result = [None]
            upload_error = [None]
            
            def upload_thread():
                try:
                    upload_result[0] = upload_file(service, latest_file, FOLDER_ID)
                except Exception as e:
                    upload_error[0] = e
            
            thread = threading.Thread(target=upload_thread)
            thread.daemon = True
            thread.start()
            
            # Wait for upload with timeout
            thread.join(timeout=timeout_seconds)
            
            if thread.is_alive():
                print(f"Upload timed out after {timeout_seconds // 60} minutes")
                print("The upload may still be running in the background.")
                print("Try checking Google Drive to see if the file appears.")
                return False
                
            if upload_error[0]:
                raise upload_error[0]
                
            if upload_result[0]:
                print("Upload completed successfully!")
                return True
            else:
                print("Upload failed - no file ID returned")
                return False
                
        else:
            print('No audio files found in the specified directory.')
            return False
            
    except Exception as e:
        print(f'Error during upload: {str(e)}')
        print("If the upload was interrupted, you can try running the script again.")
        print("Google Drive resumable uploads can continue from where they left off.")
        return False

if __name__ == '__main__':
    main()
