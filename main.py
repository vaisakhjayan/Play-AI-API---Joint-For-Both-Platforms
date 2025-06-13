import time
import os
import json
from notion import (
    NotionHandler, TargetNotionHandler,
    RWR_NOTION_TOKEN, RWR_NOTION_DATABASE_ID,
    CELEBRITY_NOTION_TOKEN, CELEBRITY_NOTION_DATABASE_ID,
    ROYAL_KNUCKLE_NOTION_TOKEN, ROYAL_KNUCKLE_NOTION_DATABASE_ID,
    VOICEOVER_NOTION_TOKEN, VOICEOVER_NOTION_DATABASE_ID,
    log
)
from platformconfig import get_platform, get_chrome_profile_path, get_celebrity_vo_path
from chrome import setup_chrome, cleanup_chrome
from contentpaster import start_content_pasting
from generationlogic import verify_and_generate
from export import export_audio
from gdrive import main as gdrive_main

def check_and_create_content():
    """Run notion.py logic once to check for content and create JSON"""
    try:
        # Initialize handlers
        source_notion = NotionHandler(RWR_NOTION_TOKEN, RWR_NOTION_DATABASE_ID)
        second_notion = NotionHandler(CELEBRITY_NOTION_TOKEN, CELEBRITY_NOTION_DATABASE_ID)
        third_notion = TargetNotionHandler(ROYAL_KNUCKLE_NOTION_TOKEN, ROYAL_KNUCKLE_NOTION_DATABASE_ID)
        target_notion = TargetNotionHandler(VOICEOVER_NOTION_TOKEN, VOICEOVER_NOTION_DATABASE_ID)
        
        # First check existing records in target database with docs links that need voiceover
        log("Checking existing records with docs links...", "info")
        existing_docs_processed = target_notion.check_existing_docs_for_voiceover()
        
        # Check source databases for new content
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
                        continue

                    # Get content from Google Docs
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

        # Check for voiceover records and create content.json
        voiceover_records = target_notion.get_records_for_voiceover()
        if voiceover_records:
            log(f"Found {len(voiceover_records)} records ready for voiceover", "success")
            return True, target_notion
        elif existing_docs_processed:
            # If we processed existing docs, check again for voiceover records
            voiceover_records = target_notion.get_records_for_voiceover()
            if voiceover_records:
                log(f"Found {len(voiceover_records)} records ready for voiceover after processing existing docs", "success")
                return True, target_notion
        
        return False, None
        
    except Exception as e:
        log(f"Error in notion check: {str(e)}", "error")
        return False, None

def main():
    try:
        # Clear screen and print header
        print("\033[H\033[J", end="")
        log("✨ VOICEOVER AUTOMATION - CONTINUOUS MONITORING", "header")
        log("Press Ctrl+C to stop monitoring", "info")
        
        while True:
            try:
                # Step 1: Run notion.py logic to check and create content
                log("Checking for new content...", "wait")
                has_content, target_notion = check_and_create_content()
                if not has_content or not target_notion:
                    log("No content found, waiting 5 minutes...", "info")
                    time.sleep(300)  # Wait 5 minutes
                    continue
                
                # Check if content.json was created
                content_file = 'JSON Files/content.json'
                if not os.path.exists(content_file):
                    log("No content.json found, waiting 5 minutes...", "info")
                    time.sleep(300)  # Wait 5 minutes
                    continue
                
                log("Content found! Starting processing...", "success")
                
                # Step 2: Setup Chrome
                log("Step 2: Setting up Chrome...", "wait")
                driver = setup_chrome()
                if not driver:
                    log("Failed to setup Chrome, waiting 5 minutes...", "error")
                    time.sleep(300)  # Wait 5 minutes
                    continue
                
                try:
                    # Step 3: Paste content
                    log("Step 3: Pasting content...", "wait")
                    if not start_content_pasting(driver):
                        log("Failed to paste content", "error")
                        continue
                    
                    # Step 4: Generate
                    log("Step 4: Starting generation...", "wait")
                    if not verify_and_generate(driver):
                        log("Failed to generate", "error")
                        continue
                    
                    # Step 5: Export
                    log("Step 5: Exporting audio...", "wait")
                    if not export_audio(driver):
                        log("Failed to export audio", "error")
                        continue
                    
                    # Get the current record ID from content.json
                    try:
                        with open(content_file, 'r', encoding='utf-8') as f:
                            content_data = json.load(f)
                            if content_data.get('records') and len(content_data['records']) > 0:
                                record_id = content_data['records'][0].get('id')
                                if record_id:
                                    # Update Notion checkboxes immediately after successful export
                                    target_notion.update_notion_checkboxes(
                                        record_id,
                                        voiceover=True,
                                        ready_to_be_edited=True
                                    )
                                    log("Updated Notion record - Voiceover completed ✓", "success")
                    except Exception as e:
                        log(f"Error updating Notion after export: {str(e)}", "error")
                    
                    # Step 6: Upload to Google Drive (optional)
                    log("Step 6: Uploading to Google Drive...", "wait")
                    try:
                        if gdrive_main():
                            log("Successfully uploaded to Google Drive ✓", "success")
                        else:
                            log("Google Drive upload failed - file can be uploaded manually later", "error")
                    except Exception as e:
                        log(f"Failed to upload to Google Drive: {str(e)}", "error")
                        log("Continuing workflow - file can be uploaded manually later", "info")
                    
                    log("Workflow completed successfully! ✨", "success")
                    log("Continuing to monitor for new content...", "info")
                    
                finally:
                    # Cleanup
                    cleanup_chrome(driver)
                    
                    # Remove content.json after processing
                    if os.path.exists(content_file):
                        os.remove(content_file)
                        
            except Exception as e:
                log(f"Error in processing cycle: {str(e)}", "error")
                log("Waiting 5 minutes before next check...", "info")
                time.sleep(300)  # Wait 5 minutes on error
                continue
                
    except KeyboardInterrupt:
        log("Stopping automation...", "info")
        log("Goodbye! ✌️", "header")
        
    except Exception as e:
        log(f"Fatal error: {str(e)}", "error")

if __name__ == "__main__":
    main()
