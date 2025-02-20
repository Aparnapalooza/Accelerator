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
    submissions_cols = ['id', 'name', 'created_utc', 'title', 'selftext', 'subreddit']
    comments_cols = ['id', 'subreddit', 'created_utc', 'body', 'link_id', 'parent_id']
    
    submissions_df = pd.DataFrame(submissions_data)[submissions_cols]
    comments_df = pd.DataFrame(comments_data)[comments_cols]
    
    # Convert timestamps
    submissions_df['created_utc'] = pd.to_datetime(submissions_df['created_utc'], unit='s')
    comments_df['created_utc'] = pd.to_datetime(comments_df['created_utc'], unit='s')
    
    # Filter submissions containing "Essure"
    essure_submissions = submissions_df[
        submissions_df['title'].str.contains('Essure', case=True, na=False) |
        submissions_df['selftext'].str.contains('Essure', case=True, na=False)
    ]
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
        how='left'
    )
    
    # Merge comments with their submissions
    merged_df_2 = pd.merge(
        essure_comments,
        submissions_df,
        left_on='link_id',
        right_on='name',
        how='left'
    )
    
    # Rename columns for merged_df_1 (submissions left, comments right)
    column_renames_1 = {
        'id_x': 'submission_base_id',
        'id_y': 'comment_base_id',
        'name': 'submission_id',
        'link_id': 'comment_link_id',
        'body': 'comment_body',
        'created_utc_x': 'submission_created_utc',
        'created_utc_y': 'comment_created_utc',
        'subreddit_x': 'submission_subreddit',
        'parent_id': 'comment_parent_id',
        'title': 'submission_title',
        'selftext': 'submission_selftext',
        'subreddit_y': 'comment_subreddit'
    }
    
    # Rename columns for merged_df_2 (comments left, submissions right)
    column_renames_2 = {
        'id_x': 'comment_base_id',
        'id_y': 'submission_base_id',
        'name': 'submission_id',
        'link_id': 'comment_link_id',
        'body': 'comment_body',
        'created_utc_x': 'comment_created_utc',
        'created_utc_y': 'submission_created_utc',
        'subreddit_x': 'comment_subreddit',
        'parent_id': 'comment_parent_id',
        'title': 'submission_title',
        'selftext': 'submission_selftext',
        'subreddit_y': 'submission_subreddit'
    }
    
    merged_df_1.rename(columns=column_renames_1, inplace=True)
    merged_df_2.rename(columns=column_renames_2, inplace=True)
    
    # Combine results and remove duplicates
    full_df = pd.concat([merged_df_1, merged_df_2], ignore_index=True)
    full_df = full_df.drop_duplicates()
    
    print(f"\nResults:")
    print(f"Found {len(full_df)} unique Essure-related discussions")
    print(f"From {full_df['submission_base_id'].nunique()} unique submissions")
    print(f"With {full_df['comment_base_id'].nunique()} unique comments")
    
    return full_df

# Process all zst files in submissions and comments folders
submissions_dir = "../../data/Input/Reddit/submissions"
comments_dir = "../../data/Input/Reddit/comments"
all_discussions = []

# Get list of all zst files
submission_files = [f for f in os.listdir(submissions_dir) if f.endswith('.zst')]
comment_files = [f for f in os.listdir(comments_dir) if f.endswith('.zst')]

# Match submission and comment files by subreddit name
for submission_file in submission_files:
    # Extract subreddit name from filename (assuming format: subreddit_submissions.zst)
    subreddit = submission_file.replace('_submissions.zst', '')
    comment_file = f"{subreddit}_comments.zst"
    
    # Check if matching comment file exists
    if comment_file in comment_files:
        submissions_path = os.path.join(submissions_dir, submission_file)
        comments_path = os.path.join(comments_dir, comment_file)
        
        print(f"\nProcessing subreddit: {subreddit}")
        discussions = process_reddit_data(submissions_path, comments_path)
        all_discussions.append(discussions)

# Combine all results
if all_discussions:
    final_df = pd.concat(all_discussions, ignore_index=True)
    final_df = final_df.drop_duplicates()
    
    print("\nFinal Results:")
    print(f"Total unique Essure-related discussions: {len(final_df)}")
    print(f"Across {final_df['submission_base_id'].nunique()} submissions")
    print(f"With {final_df['comment_base_id'].nunique()} comments")
    
    # Save results if needed
    #final_df.to_csv('essure_discussions.csv', index=False)