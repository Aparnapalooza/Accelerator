import pandas as pd
import os
import zstandard as zstd
import json
import io


# read a single zst file
def read_zst_to_json(file_path):
    """
    Read a zst compressed file and return a list of JSON objects.
    
    Args:
        file_path (str): Path to .zst file
        
    Returns:
        list: List of parsed JSON objects
    """
    data = []
    print(f"Processing {file_path}...")
    
    try:
        with open(file_path, 'rb') as fh:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(fh) as reader:
                text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                for line_count, line in enumerate(text_stream, 1):
                    try:
                        json_obj = json.loads(line.strip())
                        data.append(json_obj)
                        
                        # Print progress every 1000 records
                        if len(data) % 1000 == 0:
                            print(f"Processed {len(data)} records...")
                            
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON at line {line_count}: {e}")
                        continue
                        
        print(f"Total records processed: {len(data)}")
        return data
        
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return []


def process_reddit_data(submissions_file, comments_file):
    """
    Process Reddit submissions and comments files to extract Essure-related discussions.
    
    Args:
        submissions_file (str): Path to submissions zst file
        comments_file (str): Path to comments zst file
        
    Returns:
        pandas.DataFrame: Combined dataframe of Essure-related discussions
    """
    # Read files
    print(f"Processing submissions from: {submissions_file}")
    submissions_data = read_zst_to_json(submissions_file)
    print(f"Processing comments from: {comments_file}")
    comments_data = read_zst_to_json(comments_file)
    
    # Create DataFrames with essential columns
    submissions_cols = ['id', 'name', 'created_utc', 'title', 'selftext', 'subreddit', 'author']
    comments_cols = ['id', 'subreddit', 'created_utc', 'body', 'link_id', 'parent_id', 'author']
    
    submissions_df = pd.DataFrame(submissions_data)[submissions_cols]
    comments_df = pd.DataFrame(comments_data)[comments_cols]
    
    # Convert timestamps
    submissions_df['created_utc'] = pd.to_datetime(submissions_df['created_utc'], unit='s')
    comments_df['created_utc'] = pd.to_datetime(comments_df['created_utc'], unit='s')
    
    # Filter submissions containing "Essure"

    essure_pattern = r'\bEssure\b|\bessure\b'
    essure_submissions = submissions_df[
        submissions_df['title'].str.contains(essure_pattern, regex=True, case=True, na=False) |
        submissions_df['selftext'].str.contains(essure_pattern, regex=True, case=True, na=False)
    ]
    
    # Filter comments containing "Essure"
    essure_comments = comments_df[comments_df['body'].str.contains(essure_pattern, regex=True, case=True, na=False)]
    
    # Merge submissions with their comments
    merged_df_1 = pd.merge(
        essure_submissions,
        comments_df,
        left_on='name',
        right_on='link_id',
        how='left',
        suffixes=('_submission', '_comment')
    )
    
    # Merge comments with their submissions
    merged_df_2 = pd.merge(
        essure_comments,
        submissions_df,
        left_on='link_id',
        right_on='name',
        how='left',
        suffixes=('_comment', '_submission')
    )
    
    # Combine results and remove duplicates
    full_df = pd.concat([merged_df_1, merged_df_2], ignore_index=True)
    full_df = full_df.drop_duplicates()
    
    print(f"\nResults:")
    print(f"Found {len(full_df)} unique Essure-related discussions")
    print(f"From {full_df['id_submission'].nunique()} unique submissions")
    print(f"With {full_df['id_comment'].nunique()} unique comments")

    print(f"Across {full_df['subreddit_submission'].nunique()} subreddits:")
    
    return full_df

# Process all zst files in submissions and comments folders

submissions_dir = "../../Data/Input/Reddit/Submissions"
comments_dir = "../../Data/Input/Reddit/Comments"


# Get list of all zst files
submission_files = [f for f in os.listdir(submissions_dir) if f.endswith('.zst')]
comment_files = [f for f in os.listdir(comments_dir) if f.endswith('.zst')]

output_dir = "../../Data/Output/Reddit"
os.makedirs(output_dir, exist_ok=True)

# Match submission and comment files by subreddit name
output_dir = "../../data/Output/Reddit"
os.makedirs(output_dir, exist_ok=True)

for submission_file in submission_files:
    # Extract subreddit name from filename
    subreddit = submission_file.replace('_submissions.zst', '')
    comment_file = f"{subreddit}_comments.zst"
    
    # Check if matching comment file exists
    if comment_file in comment_files:
        submissions_path = os.path.join(submissions_dir, submission_file)
        comments_path = os.path.join(comments_dir, comment_file)
        output_file = os.path.join(output_dir, f"{subreddit}_essure_discussions.csv")
        
        print(f"\n{'='*50}")

        print(f"Processing subreddit: r/{subreddit}")
        print(f"{'='*50}")
        
        # Process one subreddit and save immediately

        discussions = process_reddit_data(submissions_path, comments_path)
        if len(discussions) > 0:
            discussions.to_csv(output_file, index=False)
            print(f"\nResults for r/{subreddit}:")

            print(f"- Saved {len(discussions)} discussions")
            print(f"- From {discussions['id_submission'].nunique()} submissions")
            print(f"- With {discussions['id_comment'].nunique()} comments")
            print(f"- Output saved to: {output_file}")
        
        # Clear memory
        del discussions
        

