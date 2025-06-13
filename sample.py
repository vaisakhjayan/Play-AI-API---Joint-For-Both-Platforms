import os
import time
import logging
import traceback
import socket
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from pyairtable import Api
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import pickle
from notion_client import Client
import re
import warnings
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
from queue import Queue, Empty

# Fix the file_cache warning by adding this import and setting
warnings.filterwarnings('ignore', message='file_cache is only supported with oauth2client<4.0.0')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
DESKTOP_DIRECTORY = os.path.expanduser("~/Desktop")
MAX_WORDS_PER_BLOCK = 250

# Notion configuration
SOURCE_NOTION_TOKEN = "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"
SOURCE_NOTION_DATABASE_ID = "0e0b82f51dc8408095bf1b0bded0f2e2"

# Second source database
SECOND_NOTION_TOKEN = "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"
SECOND_NOTION_DATABASE_ID = "196388bc362f80fda069daaf55c55a69"

# Third source database
THIRD_NOTION_TOKEN = "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"
THIRD_NOTION_DATABASE_ID = "1ed388bc362f80f9adb4f43e983573ee"

# Target Notion database
TARGET_NOTION_TOKEN = "ntn_cC7520095381SElmcgTOADYsGnrABFn2ph1PrcaGSst2dv"
TARGET_NOTION_DATABASE_ID = "1e502cd2c14280ca81e8ff63dad7f3ae"

# Voice URL mapping for different channels
VOICE_URLS = {
    "Red White & Real": "https://app.play.ht/studio/file/daOlVjalacOXwvuBCcVK?voice=s3://voice-cloning-zero-shot/5191967a-e431-4bae-a82e-ed030495963e/original/manifest.json",
    "Rachel Zegler": "https://app.play.ht/studio/file/reM1BpjhWjdq8fjePjO9?voice=s3://voice-cloning-zero-shot/541946ca-d3c9-49c7-975b-09a4e42a991f/original/manifest.json",
    "Meghan Markle": "https://app.play.ht/studio/file/reM1BpjhWjdq8fjePjO9?voice=s3://voice-cloning-zero-shot/541946ca-d3c9-49c7-975b-09a4e42a991f/original/manifest.json",
    "Knuckle Talk": "https://play.ht/studio/files/f57e51c2-bf57-4c8f-8094-a22991c8b45f?voice=s3%3A%2F%2Fvoice-cloning-zero-shot%2F54a23eaa-05de-4c08-a9dc-090513a7dc3b%2Foriginal%2Fmanifest.json",
    "Royal Family": "https://play.ht/studio/files/d121f4fc-a6ab-490b-8a3f-9c76bf6688a8?voice=s3%3A%2F%2Fvoice-cloning-zero-shot%2F904e22a9-69cf-4e1c-b6c0-eb8cbfb394ec%2Foriginal%2Fmanifest.json"
}

# Folder paths for audio files
CELEBRITY_VO_PATH = os.path.join(os.path.expanduser("~/Desktop"), "Urbanoire Voiceovers")
VOICEOVERS_MOVED_PATH = os.path.join(os.path.expanduser("~/Desktop"), "Voiceovers Moved")

# Google Drive configuration
SERVICE_ACCOUNT_FILE = 'helpful-data-459308-b9-56243ce0290d.json'
GOOGLE_DRIVE_FOLDER_ID = "1FvOdQjUNxOcZTpO2DnQf4JIFLeBohQiG"

# PlayHT configuration
PLAYHT_COOKIES_FILE = "playht_cookies_account2.pkl"
PLAYHT_LOGIN_URL = "https://app.play.ht/login"

# Create a queue for new file events
new_file_queue = Queue()

# Default voice URL as fallback
DEFAULT_VOICE_URL = "https://app.play.ht/studio/file/RUPykAWAnI5dfeJHF2rM?voice=s3://voice-cloning-zero-shot/14decc65-157c-488a-acf2-454a53b649a4/original/manifest.json"

# Add these constants
SCOPES = ['https://www.googleapis.com/auth/documents.readonly']
TOKEN_PATH = 'token.pickle'
CREDENTIALS_PATH = 'credentials.json'  # You'll need to get this from Google Cloud Console

# Notion configuration
NOTION_TOKEN = "ntn_cC7520095381SElmcgTOADYsGnrABFn2ph1PrcaGSst2dv"
NOTION_DATABASE_ID = "1a402cd2c14280909384df6c898ddcb3"  # Updated to correct database ID

# Add these constants at the top with your other constants
PLAYHT_COOKIES_FILE = "playht_cookies_account2.pkl"
PLAYHT_LOGIN_URL = "https://app.play.ht/login"

# Add these constants with your other constants
SERVICE_ACCOUNT_FILE = 'helpful-data-459308-b9-56243ce0290d.json'  # Using the correct service account credentials
GOOGLE_DRIVE_FOLDER_ID = "1FvOdQjUNxOcZTpO2DnQf4JIFLeBohQiG"  # Correct folder ID for voiceover uploads

class NotionHandler:
    def __init__(self, token, database_id):
        self.notion = Client(auth=token)
        self.database_id = database_id

    def get_done_items(self):
        """Get items from the DONE column."""
        try:
            response = self.notion.databases.query(
                database_id=self.database_id,
                filter={
                    "property": "UPDATE",
                    "status": {  # Using status type as in the original code
                        "equals": "DONE"
                    }
                }
            )
            logging.info(f"Found {len(response['results'])} items in DONE status")
            return response['results']
        except Exception as e:
            logging.error(f"Error querying Notion database: {str(e)}")
            return []

    def get_google_docs_link(self, page_id):
        """Extract Google Docs link from page comments."""
        try:
            comments = self.notion.comments.list(block_id=page_id)
            for comment in comments['results']:
                text = comment['rich_text'][0]['text']['content']
                if 'docs.google.com' in text:
                    urls = re.findall(r'https://docs.google.com/\S+', text)
                    if urls:
                        return urls[0]
            return None
        except Exception as e:
            logging.error(f"Error getting comments for page {page_id}: {str(e)}")
            return None

class TargetNotionHandler:
    def __init__(self, token, database_id):
        self.notion = Client(auth=token)
        self.database_id = database_id
        # Log database structure for debugging
        self.log_database_schema()

    def log_database_schema(self):
        """Get and log database schema to verify property names"""
        try:
            database = self.notion.databases.retrieve(self.database_id)
            logging.info("Retrieved database schema for debugging")
            if "properties" in database:
                properties = database["properties"]
                logging.info("Database properties:")
                for name, prop in properties.items():
                    prop_type = prop.get("type", "unknown")
                    logging.info(f"  - '{name}' (type: {prop_type})")
                
                # Log checkbox properties with extra detail
                logging.info("CHECKBOX PROPERTIES (detailed):")
                for name, prop in properties.items():
                    if prop.get("type") == "checkbox":
                        logging.info(f"  - Checkbox: '{name}' (ID: {prop.get('id', 'unknown')})")
                        # Dump the full property definition for detailed inspection
                        import json
                        logging.info(f"    Full definition: {json.dumps(prop)}")
                
                # Try to find the "Ready to Be Edited" property with case-insensitive matching
                ready_prop = None
                for name, prop in properties.items():
                    if "ready" in name.lower() and "edit" in name.lower():
                        logging.info(f"Found potential match for 'Ready to Be Edited': '{name}'")
                        ready_prop = name
                
                if ready_prop:
                    logging.info(f"Will use '{ready_prop}' as the property name for 'Ready to Be Edited'")
                    # Store this for later use
                    self.ready_to_be_edited_prop_name = ready_prop
                else:
                    logging.warning("Could not find a property matching 'Ready to Be Edited'")
                    self.ready_to_be_edited_prop_name = "Ready to Be Edited"  # Default fallback
            else:
                logging.warning("No properties found in database schema")
        except Exception as e:
            logging.error(f"Error retrieving database schema: {str(e)}")
            self.ready_to_be_edited_prop_name = "Ready to Be Edited"  # Default fallback

    def split_into_sentences(self, text):
        """Split text into sentences, preserving sentence boundaries"""
        # First split by obvious sentence endings
        sentences = []
        current = []
        
        # Split by words to preserve spacing
        words = text.split()
        for word in words:
            current.append(word)
            # Check for sentence endings
            if word.endswith('.') or word.endswith('!') or word.endswith('?'):
                sentences.append(' '.join(current))
                current = []
        
        # Add any remaining words as the last sentence
        if current:
            sentences.append(' '.join(current))
        
        return sentences

    def create_content_blocks(self, text):
        """Create content blocks that respect sentence boundaries and stay under 2000 chars"""
        sentences = self.split_into_sentences(text)
        blocks = []
        current_block = []
        current_length = 0
        
        for sentence in sentences:
            # +1 for the space we'll add between sentences
            sentence_length = len(sentence) + 1
            
            # If adding this sentence would exceed 2000 chars, start a new block
            if current_length + sentence_length > 2000 and current_block:
                blocks.append(' '.join(current_block))
                current_block = []
                current_length = 0
            
            current_block.append(sentence)
            current_length += sentence_length
        
        # Add the last block if it exists
        if current_block:
            blocks.append(' '.join(current_block))
        
        return blocks

    def create_record(self, docs_url, new_script="", new_title="", voiceover=False):
        """Create a new record in the target Notion database"""
        try:
            # First create the page with basic properties
            properties = {
                "Docs": {"url": docs_url},
                "New Title": {"title": [{"text": {"content": new_title}}]} if new_title else {"title": []},
                "Voiceover": {"checkbox": voiceover}
            }

            # Create the page first
            response = self.notion.pages.create(
                parent={"database_id": self.database_id},
                properties=properties
            )
            
            # If we have a script, update the page content
            if new_script:
                # Split content into blocks that respect sentence boundaries
                content_blocks = self.create_content_blocks(new_script)
                
                # Create a paragraph block for each content block
                children = [
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{"type": "text", "text": {"content": block.strip()}}]
                        }
                    }
                    for block in content_blocks
                ]
                
                self.notion.blocks.children.append(
                    response['id'],
                    children=children
                )
            
            logging.info(f"Created new Notion record with Docs URL: {docs_url}")
            return True
        except Exception as e:
            logging.error(f"Error creating Notion record: {e}")
            return False

    def update_record(self, page_id, new_script="", new_title="", voiceover=None, ready_to_be_edited=None):
        """Update an existing record in the target Notion database"""
        try:
            # Update properties except New Script
            properties = {}
            if new_title:
                properties["New Title"] = {"title": [{"text": {"content": new_title}}]}
                logging.info(f"Adding 'New Title' to update: {new_title}")
            if voiceover is not None:
                properties["Voiceover"] = {"checkbox": voiceover}
                logging.info(f"Adding 'Voiceover' checkbox to update: {voiceover}")
            if ready_to_be_edited is not None:
                ready_prop_name = getattr(self, 'ready_to_be_edited_prop_name', "Ready to Be Edited")
                properties[ready_prop_name] = {"checkbox": ready_to_be_edited}
                logging.info(f"Adding '{ready_prop_name}' checkbox to update: {ready_to_be_edited}")

            if properties:
                logging.info(f"Updating page {page_id} with properties: {properties}")
                response = self.notion.pages.update(
                    page_id=page_id,
                    properties=properties
                )
                logging.info(f"Notion API response status: success")
                # Log the actual property values from the response to verify
                if "properties" in response:
                    if voiceover is not None and "Voiceover" in response["properties"]:
                        checkbox_value = response["properties"]["Voiceover"].get("checkbox", None)
                        logging.info(f"Confirmed 'Voiceover' is now set to: {checkbox_value}")
                    if ready_to_be_edited is not None:
                        ready_prop_name = getattr(self, 'ready_to_be_edited_prop_name', "Ready to Be Edited")
                        if ready_prop_name in response["properties"]:
                            checkbox_value = response["properties"][ready_prop_name].get("checkbox", None)
                            logging.info(f"Confirmed '{ready_prop_name}' is now set to: {checkbox_value}")
            
            # If we have a script, update the page content
            if new_script:
                # First, check existing content
                existing_blocks = self.notion.blocks.children.list(page_id)
                existing_content = ""
                
                # Extract the existing content
                for block in existing_blocks.get('results', []):
                    if block['type'] == 'paragraph':
                        for text in block['paragraph'].get('rich_text', []):
                            existing_content += text.get('text', {}).get('content', '')
                
                # Only update content if it's different
                if existing_content.strip() != new_script.strip():
                    logging.info(f"Content differs - updating page {page_id}")
                    
                    # Delete existing blocks
                    for block in existing_blocks.get('results', []):
                        self.notion.blocks.delete(block['id'])
                    
                    # Split content into blocks that respect sentence boundaries
                    content_blocks = self.create_content_blocks(new_script)
                    
                    # Create a paragraph block for each content block
                    children = [
                        {
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": [{"type": "text", "text": {"content": block.strip()}}]
                            }
                        }
                        for block in content_blocks
                    ]
                    
                    # Add new content as multiple blocks
                    self.notion.blocks.children.append(
                        page_id,
                        children=children
                    )
                else:
                    logging.info(f"Content already matches - skipping update for page {page_id}")

            logging.info(f"Updated Notion record: {page_id}")
            return True
        except Exception as e:
            logging.error(f"Error updating Notion record: {str(e)}")
            # Print more details about the error
            logging.error(f"Traceback: {traceback.format_exc()}")
            return False

    def get_existing_docs_urls(self):
        """Get all existing doc URLs from the database"""
        try:
            results = self.notion.databases.query(database_id=self.database_id)
            existing_urls = set()
            for page in results.get('results', []):
                url = page['properties'].get('Docs', {}).get('url')
                if url:
                    existing_urls.add(url)
            return existing_urls
        except Exception as e:
            logging.error(f"Error getting existing URLs: {e}")
            return set()

    def get_records_for_voiceover(self):
        """Get records that need voiceover processing"""
        try:
            logging.info("Querying for records that need voiceover...")
            
            # Query for records where Voiceover is not checked and has a title
            unvoiced_records = self.notion.databases.query(
                database_id=self.database_id,
                filter={
                    "and": [
                        {"property": "Voiceover", "checkbox": {"equals": False}},
                        {"property": "New Title", "title": {"is_not_empty": True}}
                    ]
                }
            ).get('results', [])
            
            # Log raw results for debugging
            logging.info(f"Raw query returned {len(unvoiced_records)} records")
            for record in unvoiced_records:
                props = record.get('properties', {})
                title = props.get('New Title', {}).get('title', [{}])[0].get('text', {}).get('content', 'No Title')
                vo_checked = props.get('Voiceover', {}).get('checkbox', False)
                docs_url = props.get('Docs', {}).get('url', None)
                logging.info(f"Record: {title}")
                logging.info(f"  - Voiceover checkbox: {vo_checked}")
                logging.info(f"  - Has Docs URL: {docs_url is not None}")
            
            # Process records that have content
            processed_records = []
            for record in unvoiced_records:
                # Verify we have a title
                title_prop = record['properties'].get('New Title', {}).get('title', [])
                if title_prop:
                    title = title_prop[0].get('text', {}).get('content', '')
                    if title:
                        processed_records.append(record)
                        logging.info(f"Record '{title}' is ready for voiceover processing")

            logging.info(f"Found {len(processed_records)} records ready for voiceover processing")
            return processed_records
            
        except Exception as e:
            logging.error(f"Error getting records for voiceover: {e}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            return []

    def update_notion_checkboxes(self, page_id, voiceover=None, ready_to_be_edited=None):
        """Specific method just for updating checkboxes to ensure it works correctly"""
        try:
            properties = {}
            if voiceover is not None:
                properties["Voiceover"] = {"checkbox": voiceover}
                logging.info(f"Setting Voiceover checkbox to: {voiceover}")
            
            if ready_to_be_edited is not None:
                ready_prop_name = getattr(self, 'ready_to_be_edited_prop_name', "Ready to Be Edited")
                properties[ready_prop_name] = {"checkbox": ready_to_be_edited}
                logging.info(f"Adding '{ready_prop_name}' checkbox to update: {ready_to_be_edited}")

            if properties:
                logging.info(f"CHECKBOX UPDATE: Updating page {page_id} with checkboxes: {properties}")
                response = self.notion.pages.update(
                    page_id=page_id,
                    properties=properties
                )
                
                # Verify checkbox was updated
                if "properties" in response:
                    if voiceover is not None and "Voiceover" in response["properties"]:
                        value = response["properties"]["Voiceover"].get("checkbox", None)
                        logging.info(f"CHECKBOX UPDATE: Voiceover checkbox is now: {value}")
                        if value == voiceover:
                            logging.info("Voiceover checkbox was successfully updated")
                            return True
                        else:
                            logging.error(f"Voiceover checkbox value mismatch. Expected: {voiceover}, Got: {value}")
                            return False
                    else:
                        logging.error("Voiceover property not found in response")
                        return False
                else:
                    logging.error("No properties found in response")
                    return False
            
        except Exception as e:
            logging.error(f"Error updating Voiceover checkbox: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            return False

    def check_page_properties(self, page_id):
        """Retrieve and log a page's current properties for debugging"""
        try:
            page = self.notion.pages.retrieve(page_id)
            logging.info(f"Retrieved page {page_id} to check properties")
            if "properties" in page:
                properties = page["properties"]
                logging.info("Current page properties:")
                # Check for checkboxes specifically
                for name, prop in properties.items():
                    if prop.get("type") == "checkbox":
                        value = prop.get("checkbox", None)
                        logging.info(f"  - Checkbox '{name}': {value}")
            else:
                logging.warning("No properties found in page data")
                
            return True
        except Exception as e:
            logging.error(f"Error retrieving page properties: {str(e)}")
            return False

    def update_notion_with_drive_link(self, page_id, drive_url):
        """Update the Voice Drive Link column in Notion."""
        try:
            properties = {
                "Voice Drive Link": {"url": drive_url}
            }
            
            response = self.notion.pages.update(
                page_id=page_id,
                properties=properties
            )
            
            logging.info(f"Updated Notion with Drive URL: {drive_url}")
            return True
        except Exception as e:
            logging.error(f"Error updating Notion with Drive URL: {e}")
            return False

    def get_unprocessed_records(self):
        """Get records that have Google Docs but haven't been processed"""
        try:
            return self.notion.databases.query(
                database_id=self.database_id,
                filter={
                    "and": [
                        {"property": "Voiceover", "checkbox": {"equals": False}},
                        {"property": "Docs", "url": {"is_not_empty": True}}  # Changed from Docs to Docs
                    ]
                }
            ).get('results', [])
        except Exception as e:
            logging.error(f"Error getting unprocessed records: {e}")
            return []

def remove_whitespace(text):
    return ' '.join(text.split())

def split_text(text, max_words=150):
    # Split text into paragraphs first (preserve original paragraph breaks)
    paragraphs = text.split('\n')
    chunks = []
    
    for paragraph in paragraphs:
        # Skip empty paragraphs
        if not paragraph.strip():
            continue
            
        # Clean the paragraph
        cleaned_para = remove_whitespace(paragraph)
        
        # Split into sentences (looking for ., !, ?)
        sentences = []
        current_sentence = []
        words = cleaned_para.split()
        
        for word in words:
            current_sentence.append(word)
            if word.endswith('.') or word.endswith('!') or word.endswith('?'):
                sentences.append(' '.join(current_sentence))
                current_sentence = []
        
        # Add any remaining words as a sentence
        if current_sentence:
            sentences.append(' '.join(current_sentence))
        
        # Group sentences into chunks
        current_chunk = []
        word_count = 0
        
        for sentence in sentences:
            sentence_words = len(sentence.split())
            
            # If adding this sentence would exceed limit, save current chunk and start new one
            if word_count + sentence_words > max_words and current_chunk:
                chunks.append(' '.join(current_chunk))
                current_chunk = [sentence]
                word_count = sentence_words
            else:
                current_chunk.append(sentence)
                word_count += sentence_words
        
        # Add the last chunk if it exists
        if current_chunk:
            chunks.append(' '.join(current_chunk))
    
    # Log chunk information
    for i, chunk in enumerate(chunks):
        word_count = len(chunk.split())
        logging.info(f"Chunk {i+1}: {word_count} words, {len(chunk)} characters")
        if word_count > 150:
            logging.warning(f"Chunk {i+1} has {word_count} words - check for very long sentences")

    return chunks

def find_available_port(start_port=9222, max_port=9299):
    """Find an available port for Chrome DevTools Protocol."""
    for port in range(start_port, max_port + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(('127.0.0.1', port))
                return port
            except socket.error:
                continue
    return None

def cleanup_chrome_processes():
    """Kill any existing Chrome processes that might interfere with automation"""
    try:
        if sys.platform == "win32":  # Windows
            os.system("taskkill /f /im chrome.exe")
            os.system("taskkill /f /im chromedriver.exe")
        elif sys.platform == "darwin":  # macOS
            os.system("pkill -f 'Google Chrome'")
            os.system("pkill -f 'chromedriver'")
        time.sleep(2)  # Give processes time to close
        logging.info("Cleaned up existing Chrome processes")
    except Exception as e:
        logging.warning(f"Error during Chrome cleanup: {e}")

def setup_chrome_driver():
    try:
        # First cleanup any existing Chrome processes
        cleanup_chrome_processes()
        
        # Find an available port for DevTools
        debug_port = find_available_port()
        if not debug_port:
            raise Exception("Could not find an available port for Chrome DevTools")
        logging.info(f"Using port {debug_port} for Chrome DevTools")
        
        chrome_options = Options()
        
        # Remove automation control bar and other automation indicators
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Set download preferences to save directly to Celebrity Voice Overs
        chrome_options.add_experimental_option('prefs', {
            'download.default_directory': CELEBRITY_VO_PATH,
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True
        })
        
        # Basic Chrome configurations - minimal setup
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--log-level=3")  # Only show fatal errors
        
        # Set debugging port to the available port we found
        chrome_options.add_argument(f"--remote-debugging-port={debug_port}")
        chrome_options.add_argument("--remote-allow-origins=*")
        
        # Set user profile directory - use a completely separate profile for this script
        temp_profile_dir = os.path.join(os.path.expanduser("~/Desktop"), "urbanoire_chrome_profile")
        if not os.path.exists(temp_profile_dir):
            os.makedirs(temp_profile_dir)
        logging.info(f"Using temporary Chrome profile at: {temp_profile_dir}")
        chrome_options.add_argument(f"--user-data-dir={temp_profile_dir}")
        
        # Set more realistic user agent
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        
        try:
            service = Service(
                ChromeDriverManager().install(),
                log_path=os.devnull  # Suppress chromedriver logs
            )
            logging.info("ChromeDriverManager setup successful")
        except Exception as e:
            logging.error(f"Error setting up ChromeDriverManager: {e}")
            service = Service()
        
        logging.info("Attempting to start Chrome...")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)
        logging.info(f"Chrome process started with pid: {driver.service.process.pid}")
        
        # Load cookies if they exist
        if os.path.exists(PLAYHT_COOKIES_FILE):
            try:
                # First navigate to the domain
                driver.get("https://play.ht")
                time.sleep(2)  # Wait for page to load
                
                # Now load the cookies
                with open(PLAYHT_COOKIES_FILE, 'rb') as f:
                    cookies = pickle.load(f)
                    for cookie in cookies:
                        try:
                            # Remove problematic attributes
                            if 'expiry' in cookie:
                                del cookie['expiry']
                            if 'sameSite' in cookie:
                                del cookie['sameSite']
                            driver.add_cookie(cookie)
                        except Exception as e:
                            logging.warning(f"Error adding cookie: {e}")
                
                logging.info("Cookies loaded successfully")
                
                # Now navigate to the voice URL
                driver.get(DEFAULT_VOICE_URL)
                time.sleep(3)  # Wait for page to load
                
                # Check if we're logged in
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@role='textbox']"))
                    )
                    logging.info("Successfully logged in with cookies")
                    return driver
                except:
                    logging.warning("Cookie login failed, will need manual login")
            except Exception as e:
                logging.error(f"Error loading cookies: {e}")
        else:
            logging.info("No cookie file found, will need manual login")
            driver.get(DEFAULT_VOICE_URL)
        
        return driver
        
    except Exception as e:
        logging.error(f"Failed to start Chrome: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        # Try to cleanup before raising the error
        try:
            cleanup_chrome_processes()
        except:
            pass
        raise

def wait_for_element(driver, by, value, timeout=30):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))

def wait_and_click(driver, by, value, timeout=30):
    element = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, value)))
    driver.execute_script("arguments[0].click();", element)

def is_audio_ready(driver):
    try:
        # Check for the presence of the audio player and absence of loading indicators
        audio_player = driver.find_element(By.XPATH, "//div[contains(@class, 'audio-player')]")
        loading_indicators = driver.find_elements(By.XPATH, "//div[contains(@class, 'loading')]")
        
        # Check if the Export button is present and enabled
        export_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Export')]")
        
        is_ready = (
            audio_player.is_displayed() and 
            len(loading_indicators) == 0 and 
            export_button.is_enabled()
        )
        
        if is_ready:
            logging.info("Audio is ready for export")
        return is_ready
        
    except (NoSuchElementException, StaleElementReferenceException) as e:
        logging.debug(f"Audio not ready yet: {str(e)}")
        return False

def wait_for_audio_generation(driver, timeout=300):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_audio_ready(driver):
            logging.info("Audio generation completed")
            return True
        time.sleep(2)
        logging.info("Waiting for audio generation to complete...")
    
    logging.error("Timeout waiting for audio generation")
    return False

def try_export(driver):
    try:
        # Try to click Export button
        logging.info("Looking for Export button...")
        export_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Export')]")
        if not export_buttons:
            logging.info("Export button not found")
            return False
            
        export_button = export_buttons[0]
        if not export_button.is_enabled():
            logging.info("Export button found but not enabled")
            return False

        logging.info("Found enabled Export button, attempting to click...")
        driver.execute_script("arguments[0].click();", export_button)
        logging.info("Clicked Export button")
        return True

    except Exception as e:
        logging.error(f"Error in export process: {str(e)}")
        if hasattr(e, 'msg'):
            logging.error(f"Error message: {e.msg}")
        return False

    return False

def try_generate(driver):
    try:
        generate_button = driver.find_element(
            By.XPATH,
            "//button[contains(text(), 'Generate')]"
        )
        if generate_button.is_enabled():
            driver.execute_script("arguments[0].click();", generate_button)
            logging.info("Clicked Generate button")
            return True
    except Exception as e:
        logging.debug(f"Generate button not clickable: {e}")
        return False

def get_recent_download(driver, desktop_dir, text_snippet, doc_title, notion_handler, record_id, timeout=60):
    """Check for a recent file and rename it."""
    default_download_path = os.path.join(os.path.expanduser("~"), "Downloads")
    celebrity_vo_path = "E:\\Celebrity Voice Overs"
    
    # Record the start time to only look for files created after this point
    start_time = time.time()
    end_time = start_time + timeout
    last_check_files = set()
    
    while time.time() < end_time:
        try:
            # First check Downloads folder
            if os.path.exists(default_download_path):
                current_files = set(os.listdir(default_download_path))
                new_files = current_files - last_check_files
                
                for filename in new_files:
                    if filename.startswith("PlayAI_") and filename.endswith(".wav"):
                        source_path = os.path.join(default_download_path, filename)
                        
                        # Wait for file to be completely downloaded
                        file_size = -1
                        for _ in range(10):  # Check file size stability
                            try:
                                current_size = os.path.getsize(source_path)
                                if current_size == file_size and current_size > 0:  # File size hasn't changed and is not empty
                                    break
                                file_size = current_size
                                time.sleep(1)
                            except Exception as e:
                                logging.debug(f"Error checking file size: {e}")
                                time.sleep(1)
                        
                        # Get file extension
                        _, ext = os.path.splitext(filename)
                        # Create new filename with doc title
                        new_filename = f"{doc_title}{ext}"
                        target_path = os.path.join(celebrity_vo_path, new_filename)
                        
                        try:
                            # If a file with the same name exists in target directory, add a number
                            counter = 1
                            while os.path.exists(target_path):
                                new_filename = f"{doc_title}_{counter}{ext}"
                                target_path = os.path.join(celebrity_vo_path, new_filename)
                                counter += 1
                            
                            # Try to move the file
                            max_retries = 3
                            for retry in range(max_retries):
                                try:
                                    # First try to copy the file
                                    import shutil
                                    shutil.copy2(source_path, target_path)
                                    logging.info(f"Copied file to: {target_path}")
                                    
                                    # If copy successful, try to remove the original
                                    try:
                                        os.remove(source_path)
                                        logging.info("Removed original file from Downloads folder")
                                    except Exception as e:
                                        logging.warning(f"Could not remove original file: {e}")
                                    
                                    break
                                except Exception as e:
                                    if retry == max_retries - 1:
                                        raise
                                    logging.warning(f"Move attempt {retry + 1} failed: {e}")
                                    time.sleep(2)
                            
                            # Mark Notion record as complete
                            try:
                                notion_handler.update_notion_checkboxes(record_id, voiceover=True, ready_to_be_edited=True)
                                logging.info("Marked Notion record as complete")
                            except Exception as e:
                                logging.error(f"Error updating Notion: {e}")
                                return False
                            
                            return True
                                
                        except Exception as e:
                            logging.error(f"Error processing file: {str(e)}")
                            return False
                
                # Update last checked files
                last_check_files = current_files
            
            # Log progress
            remaining = end_time - time.time()
            if remaining > 0:
                logging.info(f"Waiting for download... {int(remaining)}s remaining")
            
            time.sleep(1)
            
        except Exception as e:
            logging.error(f"Error checking downloads: {e}")
            time.sleep(1)
    
    logging.error("Download timeout reached")
    return False

def save_cookies(driver, cookie_file):
    """Save current browser cookies to a file"""
    try:
        # Wait a moment to ensure we're fully logged in
        time.sleep(2)
        
        # Get all cookies
        cookies = driver.get_cookies()
        
        # Filter cookies for play.ht domain
        playht_cookies = [cookie for cookie in cookies if '.play.ht' in cookie.get('domain', '')]
        
        if playht_cookies:
            # Create directory if it doesn't exist
            cookie_dir = os.path.dirname(cookie_file)
            if cookie_dir and not os.path.exists(cookie_dir):
                os.makedirs(cookie_dir)
            
            # Save cookies
            with open(cookie_file, 'wb') as f:
                pickle.dump(playht_cookies, f)
            logging.info(f"Saved {len(playht_cookies)} cookies to {cookie_file}")
            return True
        else:
            logging.warning("No Play.ht cookies found to save")
            return False
    except Exception as e:
        logging.error(f"Error saving cookies: {e}")
        return False

def load_cookies(driver, cookie_file, domain=None):
    """Load cookies from file"""
    if not os.path.exists(cookie_file):
        logging.warning(f"Cookie file {cookie_file} not found")
        return
        
    try:
        with open(cookie_file, 'rb') as f:
            cookies = pickle.load(f)
            
        # Load only domain-specific cookies if domain is specified
        if domain:
            cookies = [cookie for cookie in cookies if domain in cookie.get('domain', '')]
            
        for cookie in cookies:
            try:
                # Remove problematic attributes that might cause issues
                if 'expiry' in cookie:
                    del cookie['expiry']
                driver.add_cookie(cookie)
            except Exception as e:
                logging.warning(f"Error adding cookie: {e}")
                
        logging.info(f"Loaded {len(cookies)} cookies from {cookie_file}")
    except Exception as e:
        logging.error(f"Error loading cookies from {cookie_file}: {e}")
        return False
    
    return True

def handle_playht_login(driver):
    """Handle PlayHT login if needed"""
    try:
        # Log current URL for debugging
        current_url = driver.current_url
        logging.info(f"Current URL before login check: {current_url}")
        
        # Take a screenshot of initial state
        try:
            initial_screenshot = os.path.join(os.path.expanduser("~/Desktop"), "playht_initial.png")
            driver.save_screenshot(initial_screenshot)
            logging.info(f"Saved initial state screenshot to {initial_screenshot}")
        except Exception as e:
            logging.warning(f"Could not save initial screenshot: {e}")
        
        # First check if already logged in by looking for the editor
        try:
            # Try multiple element indicators for being logged in
            for xpath in [
                "//div[@role='textbox']",                      # Editor textbox
                "//button[contains(text(), 'Generate')]",      # Generate button
                "//div[contains(@class, 'editor')]",           # Editor container
                "//button[contains(text(), 'New')]"            # New project button
            ]:
                try:
                    element = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.XPATH, xpath))
                    )
                    logging.info(f"Already logged in - found element: {xpath}")
                    return True
                except:
                    continue
                    
            logging.info("No logged-in indicators found, checking if login page is shown")
        except TimeoutException:
            logging.info("Editor not found, checking if login page is shown")
        
        # Check page content to determine state
        try:
            page_source = driver.page_source.lower()
            
            # If we're not on a PlayHT page, navigate directly to the voice URL
            if "play.ht" not in current_url:
                logging.info(f"Not on PlayHT domain: {current_url}")
                
                # Navigate directly to the voice URL
                logging.info("Navigating directly to voice URL...")
                driver.get(DEFAULT_VOICE_URL)
                time.sleep(5)  # Wait for page to load
                
                # Check if we're logged in after navigation
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@role='textbox']"))
                    )
                    logging.info("Successfully loaded voice URL and found editor")
                    return True
                except TimeoutException:
                    logging.warning("Could not find editor after navigation")
            
            # If we see login elements, we need manual login
            if "log in" in page_source or "sign in" in page_source or "login" in page_source:
                logging.warning("*** MANUAL LOGIN REQUIRED ***")
                logging.warning("Please log in manually in the browser window")
                logging.warning("The script will continue once logged in")
                
                # Wait longer for manual login
                try:
                    WebDriverWait(driver, 120).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@role='textbox']"))
                    )
                    
                    logging.info("Login successful - editor found after manual login")
                    # Save cookies after successful login
                    save_cookies(driver, PLAYHT_COOKIES_FILE)
                    return True
                except TimeoutException:
                    logging.error("Timeout waiting for manual login")
                    return False
            
            # Take a final screenshot of the current state
            try:
                final_screenshot = os.path.join(os.path.expanduser("~/Desktop"), "playht_current.png")
                driver.save_screenshot(final_screenshot)
                logging.info(f"Saved current state screenshot to {final_screenshot}")
            except Exception as e:
                logging.warning(f"Could not save current state screenshot: {e}")
            
            return False
                
        except Exception as e:
            logging.error(f"Error checking page state: {e}")
            return False
            
    except Exception as e:
        logging.error(f"Error in login handling: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return False

def get_audio_files():
    """Get list of audio files in the directory"""
    try:
        if os.path.exists(CELEBRITY_VO_PATH):
            return [f for f in os.listdir(CELEBRITY_VO_PATH) 
                    if f.endswith(('.wav', '.mp3'))]
        return []
    except Exception as e:
        logging.error(f"Error getting audio files: {e}")
        return []

def wait_for_new_audio_file(timeout=None):
    """Wait for a new audio file by checking directory contents. If timeout is None, wait indefinitely."""
    try:
        start_time = time.time()
        initial_files = set(get_audio_files())
        initial_count = len(initial_files)
        logging.info(f"Starting with {initial_count} audio files")

        while True:
            if timeout and time.time() - start_time > timeout:
                logging.error("Timeout waiting for new audio file")
                return None

            current_files = set(get_audio_files())
            new_files = current_files - initial_files
            
            if new_files:
                # Get the newest file based on creation time
                new_file = max(new_files, key=lambda f: os.path.getctime(os.path.join(CELEBRITY_VO_PATH, f)))
                new_file_path = os.path.join(CELEBRITY_VO_PATH, new_file)
                logging.info(f"Found new audio file: {new_file_path}")
                return new_file_path

            # Log progress every 10 seconds
            elapsed = int(time.time() - start_time)
            if elapsed % 10 == 0:
                logging.info(f"Waiting for new file... (elapsed: {elapsed}s)")
            time.sleep(0.5)

    except Exception as e:
        logging.error(f"Error waiting for new audio file: {e}")
        return None

def process_voiceover(driver, chunks, doc_title, record, target_notion, channel=None):
    try:
        last_generate_click = 0
        refresh_done = False
        export_clicked = False  # Track if we've successfully clicked export
        
        # Clear the new file queue before starting
        while not new_file_queue.empty():
            new_file_queue.get()
        
        # Determine which voice URL to use
        voice_url = DEFAULT_VOICE_URL
        if channel and channel in VOICE_URLS:
            voice_url = VOICE_URLS[channel]
            logging.info(f"Using voice URL for channel: {channel}")
        else:
            logging.info("Using default voice URL")
        
        # Check if we need to handle first-time login
        if not os.path.exists(PLAYHT_COOKIES_FILE):
            logging.info("No cookie file found - initiating first-time login process")
            # First navigate to login page
            driver.get(PLAYHT_LOGIN_URL)
            logging.info("Navigating to login page for first-time setup")
            
            # Wait for manual login and save cookies
            logging.info("Please log in manually with your second Play.ht account...")
            try:
                WebDriverWait(driver, 120).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@role='textbox']"))
                )
                logging.info("Login successful - saving cookies")
                save_cookies(driver, PLAYHT_COOKIES_FILE)
            except Exception as e:
                logging.error(f"Login failed: {e}")
                return False
        else:
            # Load existing cookies
            try:
                load_cookies(driver, PLAYHT_COOKIES_FILE)
                logging.info("Loaded existing cookies for Play.ht")
            except Exception as e:
                logging.warning(f"Error loading cookies: {e}")
        
        # Navigate directly to the voice URL
        logging.info(f"Navigating directly to voice URL: {voice_url}")
        try:
            driver.get(voice_url)
            # Wait for the page to be fully loaded
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Verify we're on the correct page
            if "play.ht" not in driver.current_url:
                raise Exception("Failed to navigate to Play.ht")
            
            logging.info(f"Successfully loaded voice URL: {driver.current_url}")
        except Exception as e:
            logging.error(f"Navigation error: {e}")
            return False
        
        # Wait for editor to be fully loaded and interactive
        try:
            # Wait for editor container
            editor_container = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'editor')]"))
            )
            logging.info("Editor container found")
            
            # Wait for actual editor textbox
            editor = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='textbox']"))
            )
            logging.info("Editor textbox found")
            
            # Wait for editor to be clickable
            WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@role='textbox']"))
            )
            logging.info("Editor is clickable")
            
            # Clear the editor using keyboard shortcuts
            driver.execute_script("arguments[0].focus();", editor)
            time.sleep(0.5)
            actions = ActionChains(driver)
            if sys.platform == "darwin":
                actions.key_down(Keys.COMMAND).send_keys('a').key_up(Keys.COMMAND)
            else:
                actions.key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL)
            actions.send_keys(Keys.DELETE).perform()
            time.sleep(0.5)
            
            # Input text chunks with clipboard
            for i, chunk in enumerate(chunks):
                try:
                    # Focus the editor
                    driver.execute_script("arguments[0].focus();", editor)
                    
                    if i > 0:
                        actions = ActionChains(driver)
                        actions.send_keys(Keys.RETURN).perform()
                        time.sleep(0.2)
                    
                    # Use JavaScript to paste the chunk
                    script = """
                        var textarea = arguments[0];
                        var text = arguments[1];
                        var dataTransfer = new DataTransfer();
                        dataTransfer.setData('text', text);
                        textarea.dispatchEvent(new ClipboardEvent('paste', {
                            clipboardData: dataTransfer,
                            bubbles: true,
                            cancelable: true
                        }));
                    """
                    driver.execute_script(script, editor, chunk)
                    time.sleep(0.2)
                    
                    # Verify using the editor's text content
                    current_text = editor.text
                    if not chunk.strip() in current_text:
                        # Try alternative paste method if first attempt failed
                        actions = ActionChains(driver)
                        if sys.platform == "darwin":
                            actions.key_down(Keys.COMMAND).send_keys('v').key_up(Keys.COMMAND)
                        else:
                            actions.key_down(Keys.CONTROL).send_keys('v').key_up(Keys.CONTROL)
                        actions.perform()
                        time.sleep(0.2)
                        
                        # Verify again
                        current_text = editor.text
                        if not chunk.strip() in current_text:
                            logging.error(f"Failed to verify chunk {i+1}")
                            return False
                    
                    logging.info(f"Successfully added chunk {i+1}")
                    
                except Exception as e:
                    logging.error(f"Error adding chunk {i+1}: {str(e)}")
                    return False
            
            # Wait for Generate button to be clickable
            generate_button = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Generate')]"))
            )
            logging.info("Generate button is ready")
            
            # Initial Generate click
            driver.execute_script("arguments[0].click();", generate_button)
            last_generate_click = time.time()
            logging.info("Clicked Generate button initially")
            
            # Wait for audio generation and try exporting
            while True:
                try:
                    current_time = time.time()

                    # Handle one refresh after 1 minute
                    if not refresh_done and current_time - last_generate_click >= 60:
                        logging.info("Refreshing page (one time)")
                        driver.refresh()
                        refresh_done = True  # Set this immediately to prevent infinite refreshes
                        time.sleep(5)  # Wait for page to load
                        
                        # Try to click generate after refresh, with multiple attempts
                        generate_attempts = 0
                        max_attempts = 3
                        while generate_attempts < max_attempts:
                            if try_generate(driver):
                                last_generate_click = current_time
                                logging.info("Clicked Generate after refresh")
                                break
                            else:
                                generate_attempts += 1
                                logging.info(f"Generate button not ready, attempt {generate_attempts}/{max_attempts}")
                                time.sleep(2)  # Wait before retry
                        
                        if generate_attempts >= max_attempts:
                            logging.warning("Could not click Generate button after refresh, continuing...")
                        continue

                    # Only proceed with export if:
                    # 1. Refresh has happened
                    # 2. Exactly 3 minutes have passed since last generate
                    # 3. We haven't clicked export yet
                    if (not export_clicked and 
                        refresh_done and 
                        current_time - last_generate_click >= 180):  # 3 minutes = 180 seconds
                        
                        # Try to export
                        if try_export(driver):
                            logging.info("Export clicked successfully, monitoring for new files...")
                            export_clicked = True
                            
                            # Start monitoring for new files indefinitely
                            while True:
                                # Check for new files without timeout
                                new_file = wait_for_new_audio_file()  # No timeout parameter
                                if new_file:
                                    logging.info(f"New audio file detected: {new_file}")
                                    
                                    # Wait a brief moment for file to be fully written
                                    time.sleep(1)
                                    
                                    # Immediately try to rename the file
                                    try:
                                        if not os.path.exists(new_file):
                                            logging.error(f"File no longer exists: {new_file}")
                                            continue
                                        
                                        # Try to open the file to verify it's not locked
                                        with open(new_file, 'rb') as f:
                                            # Just read a byte to verify access
                                            f.read(1)
                                        
                                        # If we can read the file, proceed with rename
                                        base_name = os.path.basename(new_file)
                                        _, ext = os.path.splitext(base_name)
                                        new_name = f"{doc_title}{ext}"
                                        new_path = os.path.join(CELEBRITY_VO_PATH, new_name)
                                        
                                        # If file with same name exists, add timestamp
                                        if os.path.exists(new_path):
                                            base, ext = os.path.splitext(new_name)
                                            timestamp = time.strftime("_%Y%m%d_%H%M%S")
                                            new_path = os.path.join(CELEBRITY_VO_PATH, f"{base}{timestamp}{ext}")
                                        
                                        os.rename(new_file, new_path)
                                        logging.info(f"Successfully renamed file to: {new_path}")
                                        
                                        # Now close the browser
                                        try:
                                            driver.quit()
                                            driver = None
                                            logging.info("Closed browser after successful rename")
                                        except Exception as e:
                                            logging.error(f"Error closing browser: {e}")
                                        
                                        # Upload to Google Drive
                                        drive_url = upload_to_drive(new_path, os.path.basename(new_path))
                                        if drive_url != "skipped":
                                            target_notion.update_notion_with_drive_link(record['id'], drive_url)
                                            logging.info("Updated Notion with Drive link")
                                        
                                        # Update Notion record
                                        target_notion.update_notion_checkboxes(
                                            record['id'],
                                            voiceover=True,
                                            ready_to_be_edited=True
                                        )
                                        logging.info("Updated Notion record")
                                        return True
                                        
                                    except (PermissionError, OSError) as e:
                                        logging.error(f"Error processing file: {e}")
                                        # If we failed to process the file, continue monitoring
                                        continue
                                
                                # Log monitoring status periodically
                                time.sleep(5)  # Wait before next check
                    
                    # Log remaining time until export
                    elif not export_clicked and refresh_done:
                        remaining = int(180 - (current_time - last_generate_click))
                        if remaining > 0 and remaining % 10 == 0:  # Log every 10 seconds
                            logging.info(f"Waiting {remaining} seconds before export...")

                    time.sleep(1)
                    
                except Exception as e:
                    logging.error(f"Error during attempt: {e}")
                    if driver is None:  # If browser is already closed, stop retrying
                        return False
                    time.sleep(1)
                    
        except Exception as e:
            logging.error(f"Error during text preparation: {e}")
            return False
            
    except Exception as e:
        logging.error(f"Error in process_voiceover: {e}")
        return False
    finally:
        # Make sure to quit the browser if something goes wrong
        try:
            if driver:
                driver.quit()
                logging.info("Cleaned up browser in finally block")
        except:
            pass

def mark_as_processed(table, record_id):
    table.update(record_id, {"Voiceover": True})
    logging.info(f"Marked record {record_id} as processed")

def clean_script(script):
    # List of variations to remove (case-insensitive)
    variations = [
        "Real Sound",
        "real sound",
        "Real Sound Clip",
        "real sound clip"
    ]
    
    # Clean the text
    cleaned_text = script
    for variation in variations:
        cleaned_text = cleaned_text.replace(variation, "")
    
    # Remove any double spaces or extra whitespace that might be left
    cleaned_text = ' '.join(cleaned_text.split())
    
    logging.info("Removed 'Real Sound' variations from script")
    return cleaned_text

def preprocess_text(text):
    """Preprocess text to ensure chunks of ~150 words ending with full stops"""
    # First clean the text
    text = remove_whitespace(text)
    
    # Split into sentences
    sentences = []
    current = []
    
    # Split by potential sentence endings
    for word in text.split():
        current.append(word)
        if word.endswith('.') or word.endswith('!') or word.endswith('?'):
            sentences.append(' '.join(current))
            current = []
    
    # Add any remaining text as a sentence
    if current:
        sentences.append(' '.join(current))
    
    # Group sentences into chunks of ~150 words
    chunks = []
    current_chunk = []
    current_word_count = 0
    
    for sentence in sentences:
        sentence_words = len(sentence.split())
        
        # If adding this sentence would exceed limit, save chunk and start new one
        if current_word_count + sentence_words > 150:
            if current_chunk:  # Only save if we have content
                chunk_text = ' '.join(current_chunk)
                chunks.append(chunk_text)
                # Log the chunk details
                logging.info(f"Created chunk with {len(chunk_text.split())} words")
            current_chunk = [sentence]
            current_word_count = sentence_words
        else:
            current_chunk.append(sentence)
            current_word_count += sentence_words
    
    # Add the last chunk if it exists
    if current_chunk:
        chunk_text = ' '.join(current_chunk)
        chunks.append(chunk_text)
        logging.info(f"Created final chunk with {len(chunk_text.split())} words")
    
    # Verify all chunks
    for i, chunk in enumerate(chunks, 1):
        word_count = len(chunk.split())
        logging.info(f"Chunk {i}: {word_count} words")
        if word_count > 150:
            logging.warning(f"Chunk {i} exceeds 150 words: {word_count} words")
    
    return chunks

def get_google_creds():
    """Get or refresh Google API credentials"""
    creds = None
    
    # Delete the existing token file if it exists and is invalid
    if os.path.exists(TOKEN_PATH):
        try:
            with open(TOKEN_PATH, 'rb') as token:
                creds = pickle.load(token)
            
            # If credentials are invalid and can't be refreshed, delete the token file
            if not creds or not creds.valid:
                if not creds or not creds.refresh_token or not creds.expired:
                    logging.info("Removing invalid token file")
                    os.remove(TOKEN_PATH)
                    creds = None
        except Exception as e:
            logging.error(f"Error loading credentials, removing token file: {e}")
            os.remove(TOKEN_PATH)
            creds = None
    
    # If no valid credentials available, run the OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logging.error(f"Error refreshing credentials: {e}")
                os.remove(TOKEN_PATH)
                creds = None
        
        if not creds:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
                creds = flow.run_local_server(port=0)
                # Save the new credentials
                with open(TOKEN_PATH, 'wb') as token:
                    pickle.dump(creds, token)
                logging.info("New credentials obtained and saved")
            except Exception as e:
                logging.error(f"Error running OAuth flow: {e}")
                raise
    
    return creds

def get_doc_content(doc_url):
    """Fetch content and title from a document URL"""
    try:
        parsed_url = urlparse(doc_url)
        
        if "docs.google.com" in parsed_url.netloc:
            # Extract document ID from Google Docs URL, handling both u/0 and direct paths
            path_parts = parsed_url.path.split('/')
            doc_id = None
            for part in path_parts:
                if len(part) > 25:  # Google Doc IDs are typically long strings
                    doc_id = part
                    break
            
            if not doc_id:
                raise ValueError(f"Could not extract document ID from URL: {doc_url}")
            
            logging.info(f"Extracted document ID: {doc_id}")
            
            creds = get_google_creds()
            service = build('docs', 'v1', credentials=creds)
            document = service.documents().get(documentId=doc_id).execute()
            doc_title = document.get('title', '')
            
            content = []
            skip_next = False
            
            real_sound_variants = [
                "REAL SOUND",
                "Real Sound",
                "real sound",
                "REAL SOUND CLIP",
                "Real Sound Clip",
                "real sound clip",
                "[Real Sound]",
                "(Real Sound)",
                "Real Sound:",
                "real sound:",
                "REAL SOUND:",
                "Real sound clip",
                "Real Sound Clip:",
                "REAL SOUND CLIP:"
            ]

            for element in document.get('body').get('content'):
                if 'paragraph' in element:
                    paragraph = element['paragraph']
                    para_segments = []
                    
                    for para_element in paragraph['elements']:
                        if 'textRun' in para_element:
                            text = para_element['textRun']['content']
                            text_style = para_element['textRun'].get('textStyle', {})
                            font_size = text_style.get('fontSize', {}).get('magnitude', 11)
                            
                            # Only skip this specific text segment if it's a headline
                            if font_size <= 13:
                                para_segments.append(text)
                    
                    # Combine all non-headline segments
                    para_text = ''.join(para_segments).strip()
                    
                    # Skip if empty
                    if not para_text:
                        continue
                    
                    # Only skip the "Real Sound" marker itself, not the content after it
                    if any(variant in para_text for variant in real_sound_variants):
                        continue
                    
                    # Add all other content
                    content.append(para_text)
            
            final_content = ' '.join(content)
            final_content = ' '.join(final_content.split())
            
            logging.info(f"Processed content length: {len(final_content)} characters")
            
            return {
                'content': final_content.strip(),
                'title': doc_title
            }
        else:
            response = requests.get(doc_url)
            response.raise_for_status()
            return {
                'content': response.text.strip(),
                'title': ''
            }
            
    except Exception as e:
        logging.error(f"Error fetching doc content: {e}")
        return None

def update_new_script(table, record_id, content, title):
    """Update the New Script and New Title fields with content from doc"""
    try:
        update_fields = {
            "New Script": content,
            "Name": title
        }
        table.update(record_id, update_fields)
        logging.info(f"Updated New Script and Name for record {record_id}")
        return True
    except Exception as e:
        logging.error(f"Error updating fields: {e}")
        return False

def update_airtable_docs(table, docs_url):
    """Create new record in Airtable with the Google Docs link if it doesn't exist."""
    try:
        # Check if this URL already exists in Airtable
        existing_records = table.all(
            formula=f"{{Docs}} = '{docs_url}'"
        )
        
        if existing_records:
            logging.info(f"URL already exists in Airtable, skipping: {docs_url}")
            return True
            
        # If URL doesn't exist, create new record
        new_record = {
            "Docs": docs_url,
        }
        table.create(new_record)
        logging.info(f"Created new Airtable record with Docs URL: {docs_url}")
        return True
    except Exception as e:
        logging.error(f"Error creating Airtable record: {e}")
        return False

def get_existing_docs_urls(table):
    """Get all existing doc URLs from Airtable to avoid duplicates."""
    try:
        records = table.all()
        existing_urls = set()
        for record in records:
            url = record['fields'].get('Docs', '').strip()
            if url:
                existing_urls.add(url)
        return existing_urls
    except Exception as e:
        logging.error(f"Error getting existing URLs: {e}")
        return set()

class AudioFileHandler(FileSystemEventHandler):
    def __init__(self, initial_count):
        self.initial_count = initial_count
        self.last_created_time = time.time()
        
    def on_created(self, event):
        if not event.is_directory:
            current_time = time.time()
            # Prevent duplicate events by checking time delta
            if current_time - self.last_created_time > 1:
                self.last_created_time = current_time
                file_path = event.src_path
                # Check if it's a wav or mp3 file and exists
                if file_path.endswith(('.wav', '.mp3')) and os.path.exists(file_path):
                    try:
                        # Count current files
                        current_files = len([f for f in os.listdir(CELEBRITY_VO_PATH) 
                                          if f.endswith(('.wav', '.mp3'))])
                        
                        # If we have more files than we started with
                        if current_files > self.initial_count:
                            logging.info(f"New audio file detected by watchdog: {file_path}")
                            new_file_queue.put(file_path)
                    except Exception as e:
                        logging.error(f"Error processing new file in watchdog: {e}")

def get_initial_file_count():
    """Get the initial count of audio files in the directory"""
    try:
        if os.path.exists(CELEBRITY_VO_PATH):
            count = len([f for f in os.listdir(CELEBRITY_VO_PATH) 
                        if f.endswith(('.wav', '.mp3'))])
            logging.info(f"Initial audio file count: {count}")
            return count
        else:
            os.makedirs(CELEBRITY_VO_PATH)
            logging.info("Created Celebrity Voice Overs directory")
            return 0
    except Exception as e:
        logging.error(f"Error getting initial file count: {e}")
        return 0

def setup_watchdog():
    """Set up watchdog observer for the Celebrity Voice Overs folder"""
    try:
        # Get initial file count
        initial_count = get_initial_file_count()
        
        event_handler = AudioFileHandler(initial_count)
        observer = Observer()
        observer.schedule(event_handler, CELEBRITY_VO_PATH, recursive=False)
        observer.start()
        logging.info(f"Started watchdog observer for {CELEBRITY_VO_PATH}")
        
        # Verify the observer is running
        if not observer.is_alive():
            logging.error("Observer failed to start")
            return None
            
        return observer
    except Exception as e:
        logging.error(f"Error setting up watchdog: {e}")
        return None

def upload_to_drive(file_path, filename):
    """Upload a file to Google Drive using service account and return the sharing URL."""
    try:
        # Check if service account file exists
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            logging.warning(f"Google Drive service account file not found: {SERVICE_ACCOUNT_FILE}")
            logging.info("Skipping Google Drive upload but marking task as complete")
            return "skipped"  # Return special value to indicate skipped upload
        
        # Create credentials from service account file
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=['https://www.googleapis.com/auth/drive.file']
        )
        
        # Build the Drive API service
        service = build('drive', 'v3', credentials=credentials)
        
        # File metadata
        file_metadata = {
            'name': filename,
            'parents': [GOOGLE_DRIVE_FOLDER_ID]
        }
        
        # Create media
        media = MediaFileUpload(
            file_path,
            resumable=True
        )
        
        # Upload file
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        file_id = file.get('id')
        
        # Create sharing permission (anyone with link can view)
        service.permissions().create(
            fileId=file_id,
            body={
                'type': 'anyone',
                'role': 'reader'
            }
        ).execute()
        
        # Get the sharing URL
        sharing_url = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
        
        logging.info(f"File uploaded successfully to Google Drive with URL: {sharing_url}")
        return sharing_url
        
    except Exception as e:
        logging.error(f"Error uploading to Google Drive: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return "skipped"  # Return special value to indicate skipped upload

def main():
    # Initialize Notion handlers
    source_notion = NotionHandler(SOURCE_NOTION_TOKEN, SOURCE_NOTION_DATABASE_ID)
    second_notion = NotionHandler(SECOND_NOTION_TOKEN, SECOND_NOTION_DATABASE_ID)
    third_notion = TargetNotionHandler(THIRD_NOTION_TOKEN, THIRD_NOTION_DATABASE_ID)
    target_notion = TargetNotionHandler(TARGET_NOTION_TOKEN, TARGET_NOTION_DATABASE_ID)
    driver = None
    max_retries = 3

    while True:
        try:
            # Set up watchdog observer
            observer = setup_watchdog()
            if not observer:
                logging.error("Failed to set up watchdog observer")
                time.sleep(60)
                continue

            # First check source Notion databases for new content
            for notion_handler in [source_notion, second_notion]:
                done_items = notion_handler.get_done_items()
                for item in done_items:
                    try:
                        # Get Google Docs link
                        docs_url = notion_handler.get_google_docs_link(item['id'])
                        if not docs_url:
                            continue

                        # Check if URL already exists in target database
                        existing_urls = target_notion.get_existing_docs_urls()
                        if docs_url in existing_urls:
                            logging.info(f"URL already exists in target database: {docs_url}")
                            continue

                        # Get content from Google Docs
                        doc_content = get_doc_content(docs_url)
                        if not doc_content:
                            continue

                        # Create new record in target database
                        target_notion.create_record(
                            docs_url=docs_url,
                            new_script=doc_content['content'],
                            new_title=doc_content['title']
                        )
                        logging.info(f"Created new record for: {doc_content['title']}")

                    except Exception as e:
                        logging.error(f"Error processing item: {e}")
                        continue

            # Check third database for items with Script checked and Voiceover not checked
            try:
                third_db_items = third_notion.notion.databases.query(
                    database_id=THIRD_NOTION_DATABASE_ID,
                    filter={
                        "and": [
                            {"property": "Script", "checkbox": {"equals": True}},
                            {"property": "Voiceover", "checkbox": {"equals": False}},
                            {"property": "New Title", "title": {"is_not_empty": True}}
                        ]
                    }
                ).get('results', [])

                logging.info(f"Found {len(third_db_items)} items in third database that need voiceover")
                
                for item in third_db_items:
                    try:
                        # Get title from the page
                        title_prop = item['properties'].get('New Title', {}).get('title', [])
                        doc_title = title_prop[0].get('text', {}).get('content', '').strip() if title_prop else ''
                        
                        logging.info(f"Processing item with title: {doc_title}")

                        # Get content directly from the page
                        blocks = third_notion.notion.blocks.children.list(item['id'])
                        script = ''
                        for block in blocks.get('results', []):
                            if block['type'] == 'paragraph':
                                for text in block['paragraph']['rich_text']:
                                    script += text.get('text', {}).get('content', '')

                        if script and doc_title:
                            logging.info(f"Processing voiceover for third database item: {doc_title}")

                            # Initialize Chrome if not already running
                            if not driver:
                                try:
                                    driver = setup_chrome_driver()
                                    if not driver:
                                        logging.error("Failed to initialize Chrome driver")
                                        time.sleep(60)
                                        continue

                                    # Load cookies and handle login
                                    if driver:
                                        load_cookies(driver, PLAYHT_COOKIES_FILE)
                                        if not handle_playht_login(driver):
                                            logging.error("Failed to log in")
                                            cleanup_chrome_processes()
                                            driver = None
                                            time.sleep(60)
                                            continue
                                except Exception as e:
                                    logging.error(f"Error setting up Chrome: {e}")
                                    if driver:
                                        cleanup_chrome_processes()
                                        driver = None
                                    time.sleep(60)
                                    continue

                            # Get channel from properties if available
                            channel = None
                            if "Channel" in item['properties']:
                                channel_prop = item['properties']["Channel"].get("select", {})
                                if channel_prop:
                                    channel = channel_prop.get("name")

                            # Process the voiceover
                            chunks = split_text(script)
                            if not chunks:
                                logging.warning(f"No valid chunks found for {doc_title}")
                                continue

                            start_time = time.time()
                            success = process_voiceover(driver, chunks, doc_title, item, third_notion, channel=channel)

                            if success:
                                # Upload to Google Drive
                                new_file_path = os.path.join(CELEBRITY_VO_PATH, f"{doc_title}.wav")
                                if os.path.exists(new_file_path):
                                    drive_url = upload_to_drive(new_file_path, os.path.basename(new_file_path))
                                    if drive_url != "skipped":
                                        third_notion.update_notion_with_drive_link(item['id'], drive_url)

                                # Update Voiceover checkbox - using the correct method
                                try:
                                    third_notion.notion.pages.update(
                                        page_id=item['id'],
                                        properties={
                                            "Voiceover": {"checkbox": True}
                                        }
                                    )
                                    logging.info(f"Successfully updated Voiceover checkbox for: {doc_title}")
                                except Exception as e:
                                    logging.error(f"Error updating Voiceover checkbox: {e}")

                                logging.info(f"Successfully processed third database item: {doc_title}")

                                processing_time = time.time() - start_time
                                logging.info(f"Processing time for {doc_title}: {processing_time:.2f} seconds")
                            else:
                                logging.error(f"Failed to process voiceover for: {doc_title}")
                                cleanup_chrome_processes()
                                driver = None
                                time.sleep(60)

                    except Exception as e:
                        logging.error(f"Error processing third database item: {e}")
                        continue

            except Exception as e:
                logging.error(f"Error querying third database: {e}")

            # Now check for voiceovers to process
            voiceover_records = target_notion.get_records_for_voiceover()
            logging.info(f"Found {len(voiceover_records)} records that need voiceover")
            
            if voiceover_records:
                # Process all voiceover records
                for record in voiceover_records:
                    title_prop = record['properties'].get('New Title', {}).get('title', [])
                    doc_title = title_prop[0].get('text', {}).get('content', '').strip() if title_prop else ''
                    
                    # Get content from the page body
                    blocks = target_notion.notion.blocks.children.list(record['id'])
                    script = ''
                    for block in blocks.get('results', []):
                        if block['type'] == 'paragraph':
                            for text in block['paragraph']['rich_text']:
                                script += text.get('text', {}).get('content', '')
                    
                    if script and doc_title:
                        logging.info(f"Processing voiceover for: {doc_title}")
                        
                        # Initialize Chrome if not already running
                        if not driver:
                            try:
                                driver = setup_chrome_driver()
                                if not driver:
                                    logging.error("Failed to initialize Chrome driver")
                                    time.sleep(60)  # Wait before retrying
                                    continue
                                
                                # Load cookies and handle login
                                if driver:  # Double check driver is valid
                                    load_cookies(driver, PLAYHT_COOKIES_FILE)
                                    if not handle_playht_login(driver):
                                        logging.error("Failed to log in")
                                        cleanup_chrome_processes()
                                        driver = None
                                        time.sleep(60)  # Wait before retrying
                                        continue
                            except Exception as e:
                                logging.error(f"Error setting up Chrome: {e}")
                                if driver:
                                    cleanup_chrome_processes()
                                    driver = None
                                time.sleep(60)  # Wait before retrying
                                continue

                        # Get channel from properties if available
                        channel = None
                        if "Channel" in record['properties']:
                            channel_prop = record['properties']["Channel"].get("select", {})
                            if channel_prop:
                                channel = channel_prop.get("name")
                        
                        # Process the voiceover
                        chunks = split_text(script)
                        if not chunks:
                            logging.warning(f"No valid chunks found for {doc_title}")
                            continue
                            
                        start_time = time.time()
                        success = process_voiceover(driver, chunks, doc_title, record, target_notion, channel=channel)
                        
                        if success:
                            # Upload to Google Drive
                            new_file_path = os.path.join(CELEBRITY_VO_PATH, f"{doc_title}.wav")
                            if os.path.exists(new_file_path):
                                drive_url = upload_to_drive(new_file_path, os.path.basename(new_file_path))
                                if drive_url != "skipped":
                                    target_notion.update_notion_with_drive_link(record['id'], drive_url)
                            
                            # Update both Voiceover and Ready to Be Edited checkboxes
                            checkbox_update_success = target_notion.update_notion_checkboxes(
                                record['id'], 
                                voiceover=True,
                                ready_to_be_edited=True
                            )
                            
                            if checkbox_update_success:
                                logging.info(f"Successfully processed and updated record: {doc_title}")
                            else:
                                logging.warning(f"Failed to update checkboxes for: {doc_title}")
                            
                            processing_time = time.time() - start_time
                            logging.info(f"Processing time for {doc_title}: {processing_time:.2f} seconds")
                        else:
                            logging.error(f"Failed to process voiceover for: {doc_title}")
                            cleanup_chrome_processes()
                            driver = None
                            time.sleep(60)  # Wait before retrying
                    else:
                        logging.warning(f"Skipping record {record['id']} - missing script or title")
            
            # Stop the watchdog observer
            if observer:
                observer.stop()
                observer.join()
            
            time.sleep(10)  # Short sleep between checks
            
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")
            if driver:
                cleanup_chrome_processes()
                driver = None
            time.sleep(60)  # Wait before retrying

if __name__ == "__main__":
    main() 