import time
import logging
from notion_client import Client
from datetime import datetime, timedelta
import urllib3
import requests
import sys
import json
import os
import re
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
from urllib.parse import urlparse

# Add these constants
SCOPES = ['https://www.googleapis.com/auth/documents.readonly']
TOKEN_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'docs_token.pickle')
CREDENTIALS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'credentials.json')

# Debug prints
print(f"Script directory: {os.path.dirname(os.path.abspath(__file__))}")
print(f"Looking for credentials at: {CREDENTIALS_PATH}")
print(f"File exists: {os.path.exists(CREDENTIALS_PATH)}")

# Completely suppress all logging except our own
logging.getLogger().setLevel(logging.CRITICAL)
for log_name, logger in logging.Logger.manager.loggerDict.items():
    if isinstance(logger, logging.Logger):
        logger.setLevel(logging.CRITICAL)

# Now set up our own clean logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    force=True
)

# Suppress all HTTP-related warnings
urllib3.disable_warnings()

# Path to content.json file
CONTENT_JSON_PATH = os.path.join("JSON Files", "content.json")

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
            
            # Get credentials and build service
            creds = None
            if os.path.exists(TOKEN_PATH):
                try:
                    with open(TOKEN_PATH, 'rb') as token:
                        creds = pickle.load(token)
                    
                    if not creds or not creds.valid:
                        if not creds or not creds.refresh_token or not creds.expired:
                            logging.info("Removing invalid token file")
                            os.remove(TOKEN_PATH)
                            creds = None
                except Exception as e:
                    logging.error(f"Error loading credentials, removing token file: {e}")
                    os.remove(TOKEN_PATH)
                    creds = None
            
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
                        with open(TOKEN_PATH, 'wb') as token:
                            pickle.dump(creds, token)
                        logging.info("New credentials obtained and saved")
                    except Exception as e:
                        logging.error(f"Error running OAuth flow: {e}")
                        raise
            
            service = build('docs', 'v1', credentials=creds)
            document = service.documents().get(documentId=doc_id).execute()
            doc_title = document.get('title', '')
            
            content = []
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
                            
                            if font_size <= 13:
                                para_segments.append(text)
                    
                    para_text = ''.join(para_segments).strip()
                    
                    if not para_text:
                        continue
                    
                    if any(variant in para_text for variant in real_sound_variants):
                        continue
                    
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

def store_content_in_json(content_data):
    """Store content data in the JSON file"""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(CONTENT_JSON_PATH), exist_ok=True)
        
        # Write content to JSON file
        with open(CONTENT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(content_data, f, indent=4, ensure_ascii=False)
        logging.info(f"Content stored in {CONTENT_JSON_PATH}")
        return True
    except Exception as e:
        logging.error(f"Error storing content in JSON: {e}")
        return False

# ANSI colors for terminal output
class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    DIM = "\033[2m"

def log(message, level="info", newline=True):
    """Print a nicely formatted log message with timestamp."""
    timestamp = datetime.now().strftime("%I:%M:%S %p")
    
    if level == "info":
        prefix = f"{Colors.BLUE}ℹ{Colors.RESET}"
        color = Colors.RESET
    elif level == "success":
        prefix = f"{Colors.GREEN}✓{Colors.RESET}"
        color = Colors.GREEN
    elif level == "warn":
        prefix = f"{Colors.YELLOW}⚠{Colors.RESET}"
        color = Colors.YELLOW
    elif level == "error":
        prefix = f"{Colors.RED}✗{Colors.RESET}"
        color = Colors.RED
    elif level == "wait":
        prefix = f"{Colors.CYAN}◔{Colors.RESET}"
        color = Colors.CYAN
    elif level == "header":
        prefix = f"{Colors.MAGENTA}▶{Colors.RESET}"
        color = Colors.MAGENTA + Colors.BOLD
    else:
        prefix = " "
        color = Colors.RESET
    
    log_msg = f"{Colors.DIM}[{timestamp}]{Colors.RESET} {prefix} {color}{message}{Colors.RESET}"
    
    if newline:
        print(log_msg)
    else:
        print(log_msg, end="", flush=True)

# Notion configuration
# Red White & Real Database
RWR_NOTION_TOKEN = "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"
RWR_NOTION_DATABASE_ID = "0e0b82f51dc8408095bf1b0bded0f2e2"

# Celebrity Database (Rachel Zegler, Meghan Markle)
CELEBRITY_NOTION_TOKEN = "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"
CELEBRITY_NOTION_DATABASE_ID = "196388bc362f80fda069daaf55c55a69"

# Royal Family & Knuckle Talk Database
ROYAL_KNUCKLE_NOTION_TOKEN = "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"
ROYAL_KNUCKLE_NOTION_DATABASE_ID = "1ed388bc362f80f9adb4f43e983573ee"

# Voiceover Processing Database
VOICEOVER_NOTION_TOKEN = "ntn_cC7520095381SElmcgTOADYsGnrABFn2ph1PrcaGSst2dv"
VOICEOVER_NOTION_DATABASE_ID = "1e502cd2c14280ca81e8ff63dad7f3ae"

class NotionHandler:
    def __init__(self, token, database_id):
        self.notion = Client(auth=token)
        self.database_id = database_id

    def get_google_creds(self):
        """Get or refresh Google API credentials"""
        creds = None
        
        if os.path.exists(TOKEN_PATH):
            try:
                with open(TOKEN_PATH, 'rb') as token:
                    creds = pickle.load(token)
                
                if not creds or not creds.valid:
                    if not creds or not creds.refresh_token or not creds.expired:
                        logging.info("Removing invalid token file")
                        os.remove(TOKEN_PATH)
                        creds = None
            except Exception as e:
                logging.error(f"Error loading credentials, removing token file: {e}")
                os.remove(TOKEN_PATH)
                creds = None
        
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
                    with open(TOKEN_PATH, 'wb') as token:
                        pickle.dump(creds, token)
                    logging.info("New credentials obtained and saved")
                except Exception as e:
                    logging.error(f"Error running OAuth flow: {e}")
                    raise
        
        return creds

    def get_doc_content(self, doc_url):
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
                
                creds = self.get_google_creds()
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
                                
                                if font_size <= 13:
                                    para_segments.append(text)
                        
                        para_text = ''.join(para_segments).strip()
                        
                        if not para_text:
                            continue
                        
                        if any(variant in para_text for variant in real_sound_variants):
                            continue
                        
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

    def get_done_items(self):
        """Get items from the DONE column."""
        try:
            response = self.notion.databases.query(
                database_id=self.database_id,
                filter={
                    "property": "UPDATE",
                    "status": {
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
        try:
            log("Validating Notion token...", "info")
            self.notion = Client(auth=token, log_level=logging.CRITICAL)
            # Test the connection by trying to access the database
            self.notion.databases.retrieve(database_id)
            log("Notion connection successful", "success")
        except Exception as e:
            log(f"Failed to initialize Notion client: {str(e)}", "error")
            raise Exception(f"Notion initialization failed: {str(e)}")
            
        self.database_id = database_id
        self.pending_voiceovers = []

    def create_record(self, docs_url, new_script="", new_title="", voiceover=False):
        """Create a new record in the target Notion database"""
        try:
            # First create the page with basic properties
            properties = {
                "Docs": {"url": docs_url},
                "New Title": {"title": [{"text": {"content": new_title}}]} if new_title else {"title": []},
                "Voiceover": {"checkbox": voiceover},
                "Script": {"checkbox": True}  # Set Script checkbox to True when creating record
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

    def get_existing_docs_urls(self):
        """Get all existing doc URLs from the database"""
        try:
            results = self.notion.databases.query(
                database_id=self.database_id,
                filter={
                    "property": "Docs",
                    "url": {
                        "is_not_empty": True
                    }
                }
            )
            
            # Extract URLs from the response
            existing_urls = set()
            for page in results.get('results', []):
                url = page['properties'].get('Docs', {}).get('url')
                if url:
                    existing_urls.add(url)
            return existing_urls
        except Exception as e:
            logging.error(f"Error getting existing URLs: {e}")
            return set()

    def get_block_content(self, block_id):
        """Recursively get content from a block and its children"""
        try:
            content = []
            
            # First try to get content from page properties
            try:
                page = self.notion.pages.retrieve(block_id)
                if "properties" in page:
                    # Try to get content from rich text property if it exists
                    rich_text = page.get("properties", {}).get("Content", {}).get("rich_text", [])
                    if rich_text:
                        content.extend([text.get("text", {}).get("content", "") for text in rich_text])
            except Exception as e:
                log(f"Error getting page properties: {str(e)}", "error")
            
            # Then get content from blocks
            try:
                blocks = self.notion.blocks.children.list(block_id).get('results', [])
                
                for block in blocks:
                    block_type = block.get('type')
                    if not block_type:
                        continue
                        
                    if block_type == 'paragraph':
                        text = ''.join([
                            rt.get('text', {}).get('content', '')
                            for rt in block.get(block_type, {}).get('rich_text', [])
                        ])
                        if text.strip():
                            content.append(text)
                    
                    # Handle other block types that might contain text
                    elif block_type in ['heading_1', 'heading_2', 'heading_3', 'bulleted_list_item', 'numbered_list_item']:
                        text = ''.join([
                            rt.get('text', {}).get('content', '')
                            for rt in block.get(block_type, {}).get('rich_text', [])
                        ])
                        if text.strip():
                            content.append(text)
                    
                    # Recursively get content from child blocks if they exist
                    if block.get('has_children', False):
                        child_content = self.get_block_content(block['id'])
                        content.extend(child_content)
                        
            except Exception as e:
                log(f"Error getting block content: {str(e)}", "error")
            
            return content
            
        except Exception as e:
            log(f"Error in get_block_content: {str(e)}", "error")
            return []

    def get_records_for_voiceover(self):
        """Get records that have content but haven't been processed for voiceover"""
        try:
            # Query only for records that match our criteria
            unvoiced_records = self.notion.databases.query(
                database_id=self.database_id,
                filter={
                    "and": [
                        {"property": "Voiceover", "checkbox": {"equals": False}},
                        {"property": "New Title", "title": {"is_not_empty": True}}
                    ]
                }
            ).get('results', [])
            
            log(f"Found {len(unvoiced_records)} unvoiced records", "info")
            
            # Process records that have content
            processed_records = []
            content_data = {
                "records": []
            }
            
            for record in unvoiced_records:
                try:
                    # Get the title - safely handle None values
                    properties = record.get('properties', {})
                    new_title = properties.get('New Title', {})
                    title_array = new_title.get('title', [])
                    if title_array and len(title_array) > 0:
                        title = title_array[0].get('text', {}).get('content', 'Untitled')
                    else:
                        title = 'Untitled'
                    
                    log(f"Processing record: {title}", "info")
                    
                    # Get all content recursively
                    content = []
                    try:
                        # First try to get content from page properties
                        page = self.notion.pages.retrieve(record.get('id'))
                        if not page:
                            log(f"Could not retrieve page for {title}", "error")
                            continue
                            
                        # Then get content from blocks
                        blocks_response = self.notion.blocks.children.list(record.get('id'))
                        if not blocks_response:
                            log(f"Could not retrieve blocks for {title}", "error")
                            continue
                            
                        blocks = blocks_response.get('results', [])
                        log(f"Found {len(blocks)} blocks", "info")
                        
                        for block in blocks:
                            if not block:
                                continue
                                
                            block_type = block.get('type')
                            if not block_type:
                                continue
                                
                            log(f"Processing block type: {block_type}", "info")
                            if block_type == 'paragraph':
                                rich_text = block.get(block_type, {}).get('rich_text', [])
                                text = ''.join([
                                    rt.get('text', {}).get('content', '')
                                    for rt in rich_text
                                ])
                                if text.strip():
                                    content.append(text)
                                    log(f"Added paragraph content: {text[:50]}...", "info")
                            
                            # Handle other block types that might contain text
                            elif block_type in ['heading_1', 'heading_2', 'heading_3', 'bulleted_list_item', 'numbered_list_item']:
                                rich_text = block.get(block_type, {}).get('rich_text', [])
                                text = ''.join([
                                    rt.get('text', {}).get('content', '')
                                    for rt in rich_text
                                ])
                                if text.strip():
                                    content.append(text)
                                    log(f"Added {block_type} content: {text[:50]}...", "info")
                    except Exception as e:
                        log(f"Error getting content for {title}: {str(e)}", "error")
                        continue
                    
                    script = ' '.join(content).strip()
                    
                    if script:  # Only add if there's actual content
                        # Add to processed records
                        processed_records.append(record)
                        
                        # Get channel safely
                        channel = ""
                        try:
                            channel_prop = properties.get("Channel")
                            if channel_prop:
                                select_prop = channel_prop.get("select")
                                if select_prop:
                                    channel = select_prop.get("name", "")
                        except Exception as e:
                            logging.warning(f"Error getting channel for {title}: {e}")
                        
                        # Create the record data
                        record_data = {
                            "id": record.get('id'),
                            "title": title,
                            "content": script,
                            "channel": channel
                        }
                        
                        # Verify record data before adding
                        if len(record_data["content"]) > 0:
                            content_data["records"].append(record_data)
                            log(f"Found content for: {title}", "success")
                            log(f"Content length: {len(script)} characters", "info")
                            log(f"Content preview: {script[:100]}...", "info")
                        else:
                            log(f"Warning: Empty content for {title}", "warn")
                            
                except Exception as e:
                    import traceback
                    log(f"Error processing record: {str(e)}", "error")
                    log(f"Traceback: {traceback.format_exc()}", "error")
                    continue

            # Store content in JSON file if we found any records
            if content_data["records"]:
                try:
                    # Ensure directory exists
                    os.makedirs(os.path.dirname(CONTENT_JSON_PATH), exist_ok=True)
                    
                    # Write to file
                    with open(CONTENT_JSON_PATH, 'w', encoding='utf-8') as f:
                        json.dump(content_data, f, indent=4, ensure_ascii=False)
                    log(f"Successfully saved content to {CONTENT_JSON_PATH}", "success")
                            
                except Exception as e:
                    log(f"Error saving content: {str(e)}", "error")
                    return []
            
            return processed_records
            
        except Exception as e:
            log(f"Error getting records for voiceover: {str(e)}", "error")
            return []

    def update_notion_checkboxes(self, page_id, voiceover=None, ready_to_be_edited=None):
        """Update the Voiceover and Ready to Be Edited checkboxes for a record"""
        try:
            properties = {}
            if voiceover is not None:
                properties["Voiceover"] = {"checkbox": voiceover}
                log(f"Setting Voiceover checkbox to: {voiceover}", "info")
            
            if ready_to_be_edited is not None:
                properties["Ready to Be Edited"] = {"checkbox": ready_to_be_edited}
                log(f"Setting Ready to Be Edited checkbox to: {ready_to_be_edited}", "info")

            if properties:
                log(f"Updating checkboxes for page {page_id}", "info")
                response = self.notion.pages.update(
                    page_id=page_id,
                    properties=properties
                )
                
                # Verify the update
                if "properties" in response:
                    success = True
                    if voiceover is not None and "Voiceover" in response["properties"]:
                        actual_value = response["properties"]["Voiceover"].get("checkbox", None)
                        if actual_value != voiceover:
                            log(f"Voiceover checkbox value mismatch. Expected: {voiceover}, Got: {actual_value}", "error")
                            success = False
                    
                    if ready_to_be_edited is not None and "Ready to Be Edited" in response["properties"]:
                        actual_value = response["properties"]["Ready to Be Edited"].get("checkbox", None)
                        if actual_value != ready_to_be_edited:
                            log(f"Ready to Be Edited checkbox value mismatch. Expected: {ready_to_be_edited}, Got: {actual_value}", "error")
                            success = False
                    
                    return success
                else:
                    log("No properties found in response", "error")
                    return False
            
            return True
            
        except Exception as e:
            log(f"Error updating checkboxes: {str(e)}", "error")
            return False

    def check_existing_docs_for_voiceover(self):
        """Check existing records that have docs links but haven't been processed for voiceover"""
        try:
            log("Checking existing records with docs links for voiceover processing...", "info")
            
            # Query for records that have docs URLs but Voiceover is not checked
            existing_docs_records = self.notion.databases.query(
                database_id=self.database_id,
                filter={
                    "and": [
                        {"property": "Docs", "url": {"is_not_empty": True}},
                        {"property": "Voiceover", "checkbox": {"equals": False}}
                    ]
                }
            ).get('results', [])
            
            log(f"Found {len(existing_docs_records)} existing records with docs links that need voiceover", "info")
            
            processed_count = 0
            for record in existing_docs_records:
                try:
                    # Get the docs URL
                    docs_url = record['properties'].get('Docs', {}).get('url')
                    if not docs_url:
                        continue
                    
                    # Get current title
                    title_prop = record['properties'].get('New Title', {}).get('title', [])
                    current_title = title_prop[0].get('text', {}).get('content', '').strip() if title_prop else ''
                    
                    log(f"Processing existing record with docs URL: {docs_url}", "info")
                    
                    # Fetch content from Google Docs
                    doc_content = get_doc_content(docs_url)
                    if not doc_content:
                        log(f"Could not fetch content from: {docs_url}", "error")
                        continue
                    
                    # Update the record with fetched content
                    # Split content into blocks that respect sentence boundaries
                    content_blocks = self.create_content_blocks(doc_content['content'])
                    
                    # Clear existing content blocks and add new ones
                    try:
                        # Get current blocks
                        current_blocks = self.notion.blocks.children.list(record['id']).get('results', [])
                        
                        # Delete existing content blocks
                        for block in current_blocks:
                            try:
                                self.notion.blocks.delete(block['id'])
                            except Exception as e:
                                log(f"Could not delete block {block['id']}: {e}", "warn")
                        
                        # Add new content blocks
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
                            record['id'],
                            children=children
                        )
                        
                        # Update title if it's different or empty
                        if not current_title or current_title != doc_content['title']:
                            self.notion.pages.update(
                                page_id=record['id'],
                                properties={
                                    "New Title": {"title": [{"text": {"content": doc_content['title']}}]},
                                    "Script": {"checkbox": True}  # Mark script as ready
                                }
                            )
                            log(f"Updated title to: {doc_content['title']}", "success")
                        else:
                            # Just update the Script checkbox
                            self.notion.pages.update(
                                page_id=record['id'],
                                properties={
                                    "Script": {"checkbox": True}
                                }
                            )
                        
                        log(f"Successfully updated existing record: {doc_content['title']}", "success")
                        processed_count += 1
                        
                    except Exception as e:
                        log(f"Error updating record content: {e}", "error")
                        continue
                    
                except Exception as e:
                    log(f"Error processing existing record: {e}", "error")
                    continue
            
            log(f"Successfully processed {processed_count} existing records with docs links", "success")
            return processed_count > 0
            
        except Exception as e:
            log(f"Error checking existing docs for voiceover: {e}", "error")
            return False

def monitor_notion_database():
    # Initialize Notion handlers - using the same names as sample.py
    source_notion = NotionHandler(RWR_NOTION_TOKEN, RWR_NOTION_DATABASE_ID)
    second_notion = NotionHandler(CELEBRITY_NOTION_TOKEN, CELEBRITY_NOTION_DATABASE_ID)
    third_notion = TargetNotionHandler(ROYAL_KNUCKLE_NOTION_TOKEN, ROYAL_KNUCKLE_NOTION_DATABASE_ID)
    target_notion = TargetNotionHandler(VOICEOVER_NOTION_TOKEN, VOICEOVER_NOTION_DATABASE_ID)
    
    # Clear screen and print header
    print("\033[H\033[J", end="")
    log("✨ VOICEOVER MONITOR", "header")
    
    while True:
        try:
            # 1. Check source Notion databases for new content (RWR and Celebrity)
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
                            log(f"URL already exists in target database: {docs_url}", "info")
                            continue

                        # Get content from Google Docs using the class method
                        doc_content = notion_handler.get_doc_content(docs_url)
                        if not doc_content:
                            continue

                        # Create new record in target database
                        target_notion.create_record(
                            docs_url=docs_url,
                            new_script=doc_content['content'],
                            new_title=doc_content['title']
                        )
                        log(f"Created new record for: {doc_content['title']}", "success")

                    except Exception as e:
                        log(f"Error processing item: {e}", "error")
                        continue

            # 2. Check third database (Royal Knuckle) for items with Script checked and Voiceover not checked
            try:
                third_db_items = third_notion.notion.databases.query(
                    database_id=ROYAL_KNUCKLE_NOTION_DATABASE_ID,
                    filter={
                        "and": [
                            {"property": "Script", "checkbox": {"equals": True}},
                            {"property": "Voiceover", "checkbox": {"equals": False}},
                            {"property": "New Title", "title": {"is_not_empty": True}}
                        ]
                    }
                ).get('results', [])

                log(f"Found {len(third_db_items)} items in third database that need voiceover", "info")
                
                for item in third_db_items:
                    try:
                        # Get title from the page
                        title_prop = item['properties'].get('New Title', {}).get('title', [])
                        doc_title = title_prop[0].get('text', {}).get('content', '').strip() if title_prop else ''
                        
                        log(f"Processing third database item with title: {doc_title}", "info")

                        # Get content directly from the page
                        blocks = third_notion.notion.blocks.children.list(item['id'])
                        script = ''
                        for block in blocks.get('results', []):
                            if block['type'] == 'paragraph':
                                for text in block['paragraph']['rich_text']:
                                    script += text.get('text', {}).get('content', '')

                        if script and doc_title:
                            log(f"Found content for processing in third database: {doc_title}", "success")
                            # Note: The actual voiceover processing will be handled by main.py

                    except Exception as e:
                        log(f"Error processing third database item: {e}", "error")
                        continue

            except Exception as e:
                log(f"Error querying third database: {e}", "error")

            # 3. Check target database for voiceover records
            voiceover_records = target_notion.get_records_for_voiceover()
            log(f"Found {len(voiceover_records)} records in target database that need voiceover", "info")
            
            if voiceover_records:
                for record in voiceover_records:
                    try:
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
                            log(f"Found content for processing in target database: {doc_title}", "success")
                            # Note: The actual voiceover processing will be handled by main.py
                            
                    except Exception as e:
                        log(f"Error processing target database record: {e}", "error")
                        continue

            # Wait before next check
            next_check = datetime.now() + timedelta(seconds=15)
            log(f"Next check at {next_check.strftime('%I:%M:%S %p')}", "wait")
            time.sleep(15)
            
        except KeyboardInterrupt:
            log("Stopping monitor...", "info")
            break
            
        except Exception as e:
            log(f"Error: {str(e)}", "error")
            time.sleep(30)

if __name__ == "__main__":
    try:
        monitor_notion_database()
    except KeyboardInterrupt:
        print("\n")
        log("Monitor stopped", "info")
        log("Goodbye! ✌️", "header")
