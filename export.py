from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import logging
import time
import os
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from queue import Queue, Empty
from platformconfig import get_celebrity_vo_path

# Create a logger specific to this module
logger = logging.getLogger(__name__)

# Configurable paths and delays
CONTENT_JSON_PATH = "JSON Files/content.json"
DELAY_BEFORE_EXPORT = 160  # Delay before clicking Export
EXPORT_TIMEOUT = 6000  # Maximum time to wait for export file

# Get the platform-specific export path
CELEBRITY_VO_PATH = get_celebrity_vo_path()

# Queue for new file events
new_file_queue = Queue()

class AudioFileHandler(FileSystemEventHandler):
    def __init__(self, initial_count):
        self.initial_count = initial_count
        self.last_created_time = time.time()
        logger.info(f"Initialized AudioFileHandler with initial count: {initial_count}")
        
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
                            logger.info(f"New audio file detected: {file_path}")
                            new_file_queue.put(file_path)
                    except Exception as e:
                        logger.error(f"Error processing new file: {e}")

def get_initial_file_count():
    """Get the initial count of audio files in the directory"""
    try:
        if os.path.exists(CELEBRITY_VO_PATH):
            count = len([f for f in os.listdir(CELEBRITY_VO_PATH) 
                        if f.endswith(('.wav', '.mp3'))])
            logger.info(f"Initial audio file count: {count}")
            return count
        else:
            os.makedirs(CELEBRITY_VO_PATH)
            logger.info("Created Celebrity Voice Overs directory")
            return 0
    except Exception as e:
        logger.error(f"Error getting initial file count: {e}")
        return 0

def setup_watchdog():
    """Set up watchdog observer for the Celebrity Voice Overs folder"""
    try:
        logger.info("Setting up watchdog observer...")
        initial_count = get_initial_file_count()
        
        event_handler = AudioFileHandler(initial_count)
        observer = Observer()
        observer.schedule(event_handler, CELEBRITY_VO_PATH, recursive=False)
        observer.start()
        logger.info(f"Started watchdog observer for {CELEBRITY_VO_PATH}")
        
        if not observer.is_alive():
            logger.error("Observer failed to start")
            return None
            
        return observer
    except Exception as e:
        logger.error(f"Error setting up watchdog: {e}")
        return None

def handle_error_dialogs(driver):
    """Handle any error dialogs that pop up by clicking OK or Cancel"""
    try:
        buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'OK') or contains(text(), 'Cancel') or contains(text(), 'Dismiss')]")
        for button in buttons:
            if button.is_displayed():
                try:
                    driver.execute_script("arguments[0].click();", button)
                    logger.info(f"Clicked dialog button: {button.text}")
                    time.sleep(0.5)
                except:
                    pass
    except Exception as e:
        logger.debug(f"No error dialogs found or error handling them: {e}")

def is_driver_alive(driver):
    """Check if the WebDriver session is still alive and responsive."""
    if driver is None:
        logger.warning("Driver is None")
        return False
    
    try:
        driver.current_url
        return True
    except Exception as e:
        logger.warning(f"Driver session appears to be dead: {e}")
        return False

def check_for_error_dialog(driver):
    """Check specifically for audio not ready dialog and return True if found"""
    try:
        error_messages = driver.find_elements(By.XPATH, 
            "//div[contains(text(), 'not ready') or contains(text(), 'still processing') or contains(text(), 'please wait')]"
        )
        error_buttons = driver.find_elements(By.XPATH, 
            "//button[contains(text(), 'OK') or contains(text(), 'Cancel') or contains(text(), 'Dismiss')]"
        )
        
        if error_messages or error_buttons:
            logger.info("Found audio not ready dialog")
            for button in error_buttons:
                if button.is_displayed():
                    driver.execute_script("arguments[0].click();", button)
                    logger.info("Dismissed error dialog")
            return True
        return False
    except Exception as e:
        logger.debug(f"Error checking for dialog: {e}")
        return False

def try_export(driver):
    """Try to click the Export button with multiple fallback methods"""
    try:
        if not is_driver_alive(driver):
            logger.warning("Driver session is dead in try_export")
            return False
            
        # Initial 120-second delay before first attempt
        logger.info("Waiting 120 seconds before attempting export...")
        for remaining in range(160, 0, -5):
            logger.info(f"Export will begin in {remaining} seconds...")
            time.sleep(5)
        
        # Reload the page after the initial delay
        logger.info("Reloading page before export...")
        driver.refresh()
        
        # Wait for page to reload and stabilize
        logger.info("Waiting 20 seconds after reload...")
        time.sleep(20)
        
        max_retries = 20
        retry_count = 0
        
        while retry_count < max_retries:
            logger.info("Looking for Export button...")
            export_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Export')]")
            
            if not export_buttons:
                logger.info("Export button not found")
                return False
            
            export_button = export_buttons[0]
            if not export_button.is_enabled():
                logger.info("Export button found but not enabled")
                time.sleep(15)
                retry_count += 1
                continue

            logger.info("Found enabled Export button, attempting to click...")
            driver.execute_script("arguments[0].click();", export_button)
            logger.info("Clicked Export button")
            
            time.sleep(2)
            
            if check_for_error_dialog(driver):
                logger.info(f"Audio not ready, waiting 15 seconds before retry {retry_count + 1}/{max_retries}")
                time.sleep(15)
                retry_count += 1
                continue
            else:
                logger.info("No error dialog found, export appears successful")
                return True

        logger.error("Maximum retry attempts reached, export failed")
        return False

    except Exception as e:
        error_msg = str(e).lower()
        if any(error_text in error_msg for error_text in ["connection", "session", "refused", "10061"]):
            logger.error(f"Connection error in export process: {str(e)}")
            logger.error("This indicates the WebDriver session has been lost")
        else:
            logger.error(f"Error in export process: {str(e)}")
        return False

def get_title_from_json():
    """Get the title from content.json"""
    try:
        with open(CONTENT_JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            title = data['records'][0]['title']
            logger.info(f"Found title from JSON: {title}")
            return title
    except Exception as e:
        logger.error(f"Error reading title from JSON: {e}")
        return None

def rename_new_file(file_path):
    """Rename the new file with the title from content.json"""
    try:
        title = get_title_from_json()
        if not title:
            logger.error("Could not get title from JSON, keeping original filename")
            return file_path
            
        # Clean the title to make it filesystem-safe
        title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
        if not title:
            logger.error("Title became empty after cleaning, keeping original filename")
            return file_path
            
        _, ext = os.path.splitext(file_path)
        new_filename = f"{title}{ext}"
        new_path = os.path.join(CELEBRITY_VO_PATH, new_filename)
        
        counter = 1
        while os.path.exists(new_path):
            new_filename = f"{title} ({counter}){ext}"
            new_path = os.path.join(CELEBRITY_VO_PATH, new_filename)
            counter += 1
            
        os.rename(file_path, new_path)
        logger.info(f"Renamed file to: {new_filename}")
        return new_path
    except Exception as e:
        logger.error(f"Error renaming file: {e}")
        return file_path

def wait_for_export_complete():
    """Wait for new export file to appear in the directory"""
    try:
        logger.info(f"Waiting up to {EXPORT_TIMEOUT} seconds for export to complete...")
        start_time = time.time()
        initial_files = get_audio_files()
        logger.info(f"Starting with {len(initial_files)} files")

        while time.time() - start_time < EXPORT_TIMEOUT:
            current_files = get_audio_files()
            new_files = current_files - initial_files
            
            if new_files:
                newest_file = max(new_files, key=lambda f: os.path.getctime(os.path.join(CELEBRITY_VO_PATH, f)))
                newest_path = os.path.join(CELEBRITY_VO_PATH, newest_file)
                
                if os.path.exists(newest_path) and os.path.getsize(newest_path) > 0:
                    logger.info(f"Export completed successfully: {newest_file}")
                    renamed_path = rename_new_file(newest_path)
                    return True
            
            try:
                new_file = new_file_queue.get_nowait()
                if new_file and os.path.exists(new_file) and os.path.getsize(new_file) > 0:
                    logger.info(f"Export completed successfully (via watchdog): {new_file}")
                    renamed_path = rename_new_file(new_file)
                    return True
            except Empty:
                pass
                
            remaining = int(EXPORT_TIMEOUT - (time.time() - start_time))
            if remaining % 5 == 0:
                current_count = len(current_files)
                logger.info(f"Still waiting for export... {remaining}s remaining (Files: {current_count}, New: {len(new_files)})")
            
            time.sleep(1)
                
        logger.error("Export timeout reached - no file detected")
        return False
        
    except Exception as e:
        logger.error(f"Error waiting for export: {e}")
        return False

def get_audio_files():
    """Get current list of audio files in the directory"""
    try:
        if os.path.exists(CELEBRITY_VO_PATH):
            return set(f for f in os.listdir(CELEBRITY_VO_PATH) 
                      if f.endswith(('.wav', '.mp3')))
        return set()
    except Exception as e:
        logger.error(f"Error getting audio files: {e}")
        return set()

def export_audio(driver):
    """Main export function that coordinates the export process"""
    try:
        logger.info("Starting export process...")
        
        initial_files = get_audio_files()
        logger.info(f"Initial file count: {len(initial_files)}")
        
        observer = setup_watchdog()
        if not observer:
            logger.error("Failed to set up file monitoring")
            return False
            
        try:
            if try_export(driver):
                time.sleep(2)
                current_files = get_audio_files()
                new_files = current_files - initial_files
                
                if new_files:
                    new_file = max(new_files, key=lambda f: os.path.getctime(os.path.join(CELEBRITY_VO_PATH, f)))
                    logger.info(f"New file detected immediately: {new_file}")
                    renamed_path = rename_new_file(os.path.join(CELEBRITY_VO_PATH, new_file))
                    return True
                    
                if wait_for_export_complete():
                    logger.info("Export process completed successfully")
                    return True
                else:
                    current_files = get_audio_files()
                    new_files = current_files - initial_files
                    if new_files:
                        new_file = max(new_files, key=lambda f: os.path.getctime(os.path.join(CELEBRITY_VO_PATH, f)))
                        logger.info(f"New file detected in final check: {new_file}")
                        renamed_path = rename_new_file(os.path.join(CELEBRITY_VO_PATH, new_file))
                        return True
                    
                    logger.error("Export file not detected")
                    return False
            else:
                logger.error("Failed to click export button")
                return False
                
        finally:
            if observer:
                logger.info("Stopping watchdog observer...")
                observer.stop()
                observer.join()
                logger.info("Watchdog observer stopped")
                
    except Exception as e:
        logger.error(f"Error in export process: {e}")
        return False
