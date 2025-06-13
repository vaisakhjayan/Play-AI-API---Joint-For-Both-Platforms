from notion_client import Client

# First client for first three databases
client1 = Client(auth="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")

# Second client for target database
client2 = Client(auth="ntn_cC7520095381SElmcgTOADYsGnrABFn2ph1PrcaGSst2dv")

# Database IDs
db_ids = {
    "First DB": "0e0b82f51dc8408095bf1b0bded0f2e2",
    "Second DB": "196388bc362f80fda069daaf55c55a69",
    "Third DB": "1ed388bc362f80f9adb4f43e983573ee",
    "Target DB": "1e502cd2c14280ca81e8ff63dad7f3ae"
}

# Check each database
for label, db_id in db_ids.items():
    try:
        client = client1 if label != "Target DB" else client2
        db = client.databases.retrieve(db_id)
        title = db['title'][0]['text']['content']
        print(f"{label}: {title}")
    except Exception as e:
        print(f"Error getting {label}: {str(e)}") 