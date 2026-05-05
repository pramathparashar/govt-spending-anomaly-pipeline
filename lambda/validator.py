import json
import boto3
import base64
from datetime import datetime

# Configuration
S3_BUCKET = "govt-spending-pipeline-raw-pramath-352017689721-us-east-2-an"
REGION = "us-east-2"

# Initialize S3 client
s3_client = boto3.client('s3', region_name=REGION)

# Required fields for validation
REQUIRED_FIELDS = [
    "contract_id",
    "vendor_name", 
    "award_amount",
    "agency_name",
    "award_date",
    "ingestion_timestamp"
]

def validate_record(record):
    """
    Validates a contract record against required fields and data types.
    Returns (is_valid, error_message)
    """
    # Check required fields exist and are not empty
    for field in REQUIRED_FIELDS:
        if field not in record:
            return False, f"Missing required field: {field}"
        if record[field] is None or record[field] == "":
            return False, f"Empty value for required field: {field}"
    
    # Validate award_amount is positive number
    try:
        amount = float(record["award_amount"])
        if amount <= 0:
            return False, f"Invalid award_amount: {amount}"
    except (ValueError, TypeError):
        return False, f"award_amount is not a number: {record['award_amount']}"
    
    # Validate award_date format
    try:
        datetime.strptime(record["award_date"], "%Y-%m-%d")
    except ValueError:
        return False, f"Invalid award_date format: {record['award_date']}"
    
    return True, "Valid"

def tag_record(record):
    """
    Adds pipeline metadata tags to the record
    """
    record["pipeline_version"] = "1.0"
    record["validation_status"] = "passed"
    record["processing_timestamp"] = datetime.utcnow().isoformat()
    return record

def write_to_s3(record, shard_id, sequence_number):
    """
    Writes validated record to S3 Raw layer
    Partitioned by year/month/day for efficient querying
    """
    now = datetime.utcnow()
    
    # Build S3 key with partitioning
    s3_key = (
        f"raw/contracts/"
        f"year={now.year}/"
        f"month={str(now.month).zfill(2)}/"
        f"day={str(now.day).zfill(2)}/"
        f"shard={shard_id}/"
        f"seq={sequence_number}.json"
    )
    
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(record).encode('utf-8'),
            ContentType='application/json'
        )
        return True, s3_key
    except Exception as e:
        print(f"S3 write error: {e}")
        return False, str(e)

def lambda_handler(event, context):
    """
    Main Lambda handler - triggered by Kinesis stream
    Processes each record in the batch
    """
    print(f"Processing {len(event['Records'])} records from Kinesis")
    
    success_count = 0
    error_count = 0
    
    for kinesis_record in event['Records']:
        try:
            # Decode base64 encoded Kinesis data
            encoded_data = kinesis_record['kinesis']['data']
            decoded_data = base64.b64decode(encoded_data).decode('utf-8')
            record = json.loads(decoded_data)
            
            # Get Kinesis metadata
            shard_id = kinesis_record['eventID'].split(':')[0]
            sequence_number = kinesis_record['kinesis']['sequenceNumber']
            
            print(f"Processing record: {record.get('contract_id', 'unknown')}")
            
            # Validate record
            is_valid, message = validate_record(record)
            
            if not is_valid:
                print(f"Validation failed: {message}")
                error_count += 1
                continue
            
            # Tag record with pipeline metadata
            record = tag_record(record)
            
            # Write to S3
            success, result = write_to_s3(record, shard_id, sequence_number)
            
            if success:
                print(f"Successfully written to S3: {result}")
                success_count += 1
            else:
                print(f"S3 write failed: {result}")
                error_count += 1
                
        except Exception as e:
            print(f"Error processing record: {e}")
            error_count += 1
    
    print(f"Batch complete. Success: {success_count}, Errors: {error_count}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'success_count': success_count,
            'error_count': error_count
        })
    }