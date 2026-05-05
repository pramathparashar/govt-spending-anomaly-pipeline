import boto3
import json
import time
import requests
from datetime import datetime, timedelta

# Configuration
STREAM_NAME = "govt-spending-stream"
REGION = "us-east-2"
API_BASE_URL = "https://api.usaspending.gov/api/v2"

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name=REGION)

def fetch_contract_awards(page=1, limit=100):
    url = f"{API_BASE_URL}/search/spending_by_award/"
    
    payload = {
        "filters": {
            "time_period": [
                {
                    "start_date": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
                    "end_date": datetime.now().strftime("%Y-%m-%d")
                }
            ],
            "award_type_codes": ["A", "B", "C", "D"]
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",
            "Awarding Agency",
            "Award Date",
            "Contract Award Type",
            "NAICS Code"
        ],
        "page": page,
        "limit": limit,
        "sort": "Award Amount",
        "order": "desc"
    }
    
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
        return None

def transform_record(award):
    # Try multiple possible date field names
    award_date = (
        award.get("Award Date") or
        award.get("period_of_performance_start_date") or
        award.get("action_date") or
        datetime.now().strftime("%Y-%m-%d")  # fallback to today
    )
    
    return {
        "contract_id": award.get("Award ID", ""),
        "vendor_name": award.get("Recipient Name", ""),
        "award_amount": float(award.get("Award Amount", 0) or 0),
        "agency_name": award.get("Awarding Agency", ""),
        "award_date": award_date,
        "contract_type": award.get("Contract Award Type", ""),
        "naics_code": str(award.get("NAICS Code", "")),
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "source": "USASpending.gov"
    }

def send_to_kinesis(record):
    try:
        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(record).encode('utf-8'),
            PartitionKey=record.get("agency_name", "default")
        )
        return response
    except Exception as e:
        print(f"Kinesis Error: {e}")
        return None

def run_producer():
    print("Starting Government Contract Spending Producer...")
    print(f"Stream: {STREAM_NAME}")
    print(f"Region: {REGION}")
    print("-" * 50)
    
    page = 1
    total_sent = 0
    
    while True:
        print(f"\nFetching page {page} from USASpending.gov API...")
        
        data = fetch_contract_awards(page=page)
        
        if not data or "results" not in data:
            print("No more data or API error. Restarting from page 1...")
            page = 1
            time.sleep(60)
            continue
        
        awards = data.get("results", [])
        
        if not awards:
            print("No results on this page. Restarting...")
            page = 1
            time.sleep(60)
            continue
        
        print(f"Fetched {len(awards)} records. Sending to Kinesis...")
        
        for award in awards:
            record = transform_record(award)
            
            if record["contract_id"] and record["award_amount"] > 0:
                response = send_to_kinesis(record)
                
                if response:
                    total_sent += 1
                    print(f"Sent: {record['agency_name']} | "
                          f"${record['award_amount']:,.2f} | "
                          f"Shard: {response.get('ShardId', 'N/A')}")
                
                time.sleep(0.1)
        
        print(f"\nTotal records sent so far: {total_sent}")
        
        total_pages = data.get("page_metadata", {}).get("total", 1)
        
        if page >= total_pages:
            print("All pages fetched. Restarting from page 1 in 5 minutes...")
            page = 1
            time.sleep(300)
        else:
            page += 1
            time.sleep(2)

if __name__ == "__main__":
    run_producer()