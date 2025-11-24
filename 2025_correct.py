import pandas as pd
import os
import glob

# Configuration
TARGET_YEAR_DIR = 'shot_data_with_urls/2025'
BACKUP_FILE = '../formatted_videos_backup.csv' # Using the specific filename you provided

def generate_video_url(row):
    """
    Helper function to generate URL strings.
    """
    # Check if UUID is valid
    if pd.isna(row['uuid']) or row['uuid'] == 'NO_VIDEO':
        return None
    
    try:
        # Ensure date parts are integers before formatting
        year_str = str(int(row['year']))
        month_str = str(int(row['month'])).zfill(2)
        day_str = str(int(row['day'])).zfill(2)
        game_id = f"00{int(row['GAME_ID'])}"
        event_id = int(row['GAME_EVENT_ID'])
        
        return (
            f"https://videos.nba.com/nba/pbp/media/"
            f"{year_str}/{month_str}/{day_str}/{game_id}/"
            f"{event_id}/{row['uuid']}_1280x720.mp4"
        )
    except (ValueError, TypeError):
        return None

def patch_2025_directory():
    print(f"--- Starting Patch Process for {TARGET_YEAR_DIR} ---")
    
    # 1. Load the Backup/Fixer Data
    if not os.path.exists(BACKUP_FILE):
        print(f"Error: Backup file '{BACKUP_FILE}' not found.")
        return

    print(f"Loading backup data from {BACKUP_FILE}...")
    backup_df = pd.read_csv(BACKUP_FILE)
    
    # Standardize backup columns for merging
    # We assume the backup file has: game_id, action_number, uuid, year, month, day
    backup_df['game_id'] = backup_df['game_id'].astype(str)
    backup_df['action_number'] = backup_df['action_number'].astype(int)
    
    # Remove duplicates in backup to prevent exploding the merge
    backup_df = backup_df.drop_duplicates(subset=['game_id', 'action_number'])

    # 2. Get list of team files in the 2025 directory
    team_files = glob.glob(os.path.join(TARGET_YEAR_DIR, '*.csv'))
    print(f"Found {len(team_files)} team files to check.")

    total_fixed_count = 0

    # 3. Iterate through each team file
    for file_path in team_files:
        try:
            team_df = pd.read_csv(file_path)
            original_len = len(team_df)
            
            # Ensure join keys match types
            team_df['GAME_ID'] = team_df['GAME_ID'].astype(str)
            team_df['GAME_EVENT_ID'] = team_df['GAME_EVENT_ID'].astype(int)

            # Perform a Left Merge to bring in the "Fixer" data
            # We use suffixes to distinguish between existing data and new data
            merged_df = pd.merge(
                team_df,
                backup_df[['game_id', 'action_number', 'uuid', 'year', 'month', 'day']],
                left_on=['GAME_ID', 'GAME_EVENT_ID'],
                right_on=['game_id', 'action_number'],
                how='left',
                suffixes=('', '_fix')
            )

            # 4. Apply the fixes
            # If the original 'uuid' is NaN, fill it with 'uuid_fix'
            # If the original 'video_url' is missing, we likely need the date columns from the fix too
            
            mask_missing_uuid = team_df['uuid'].isna() | (team_df['uuid'] == 'NO_VIDEO')
            
            # Update UUIDs
            merged_df['uuid'] = merged_df['uuid'].fillna(merged_df['uuid_fix'])
            
            # Update Date columns (Year/Month/Day) if they were missing/NaN in original
            # (These are required to generate the URL)
            for col in ['year', 'month', 'day']:
                if col in merged_df.columns and f'{col}_fix' in merged_df.columns:
                    merged_df[col] = merged_df[col].fillna(merged_df[f'{col}_fix'])
                elif f'{col}_fix' in merged_df.columns:
                    # If column didn't exist in original, just take the fix column
                    merged_df[col] = merged_df[f'{col}_fix']

            # 5. Regenerate Video URLs
            # We only really need to regenerate where it was missing, but doing all ensures consistency
            merged_df['video_url'] = merged_df.apply(generate_video_url, axis=1)

            # 6. Cleanup
            # Select only the columns that belong in the final file
            # (excluding the _fix columns and the merge keys from backup)
            cols_to_keep = team_df.columns.tolist()
            
            # If 'video_url' or 'uuid' weren't in the original columns for some reason, add them
            if 'video_url' not in cols_to_keep: cols_to_keep.insert(1, 'video_url')
            if 'uuid' not in cols_to_keep: cols_to_keep.append('uuid')
            
            final_df = merged_df[cols_to_keep]

            # Calculate stats
            fixed_in_this_file = final_df['video_url'].notna().sum() - team_df['video_url'].notna().sum()
            if fixed_in_this_file > 0:
                total_fixed_count += fixed_in_this_file
                # Overwrite the file
                final_df.to_csv(file_path, index=False)
                print(f"  -> Fixed {file_path}: +{fixed_in_this_file} URLs restored.")
            
        except Exception as e:
            print(f"  Error processing {file_path}: {e}")

    print(f"\n--- Patch Complete. Total URLs restored: {total_fixed_count} ---")

if __name__ == "__main__":
    patch_2025_directory()