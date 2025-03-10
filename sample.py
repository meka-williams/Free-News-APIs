import boto3
import json
import os
from botocore.config import Config
import uuid  # For generating unique filenames

def upload_articles_to_s3(articles, team_prefix, bucket_name):
    """
    Upload articles to S3 bucket as individual JSON files
    
    Args:
        articles (list): List of processed article dictionaries
        team_prefix (str): Team prefix for S3 path (e.g., "TEAM_4/")
        bucket_name (str): S3 bucket name
        
    Returns:
        int: Number of successfully uploaded articles
    """
    # Create S3 client with unsigned signature (for public bucket)
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    # Create a temporary directory to store files
    temp_dir = "/tmp/articles/"
    os.makedirs(temp_dir, exist_ok=True)
    
    uploaded_count = 0
    
    for i, article in enumerate(articles):
        # Create a unique filename for each article
        # Use article ID if available, otherwise generate a unique ID
        article_id = article.get('id', str(uuid.uuid4())[:8])
        safe_title = ''.join(c if c.isalnum() else '_' for c in article.get('title', '')[:30])
        
        # Combine to make a descriptive filename
        filename = f"{article_id}_{safe_title}.json"
        filepath = os.path.join(temp_dir, filename)
        
        # Save article to temporary file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(article, f, ensure_ascii=False, indent=2)
        
        # Upload to S3
        object_name = team_prefix + "articles/" + filename
        try:
            s3.upload_file(filepath, bucket_name, object_name)
            print(f"Uploaded article {i+1}/{len(articles)}: {filename}")
            uploaded_count += 1
        except Exception as e:
            print(f"Error uploading {filename}: {e}")
    
    # List objects in the bucket to verify upload
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=team_prefix)
        if "Contents" in response:
            print(f"\nFiles in S3 Bucket under {team_prefix}:")
            for obj in response["Contents"][:10]:  # Show first 10 files
                print(f" - {obj['Key']}")
            
            if len(response["Contents"]) > 10:
                print(f" ... and {len(response['Contents']) - 10} more files")
        else:
            print("No files found in the bucket under your team prefix.")
    except Exception as e:
        print(f"Error listing bucket contents: {e}")
    
    return uploaded_count

# Example usage:
TEAM = "TEAM_4/"
BUCKET_NAME = "cus635-spring2025"

# Assuming you already have news_data and processed_data from earlier code
news_data = get_news_articles(news_key)
processed_data = preprocess_articles(news_data.get('news', []))

# Upload the processed articles to S3
uploaded_count = upload_articles_to_s3(processed_data, TEAM, BUCKET_NAME)
print(f"\nSuccessfully uploaded {uploaded_count} articles to S3")
