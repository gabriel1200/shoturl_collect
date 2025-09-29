import pandas as pd
import os

# Load the game dates to get team IDs
GAME_DATES = pd.read_csv('https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv')

def load_all_shot_data_for_year(year, base_path='../../shot_data/team'):
    """
    Loads regular season and post-season shot data for all teams in a given year.

    Args:
        year (int): The year to load shot data for.
        base_path (str): The base path to the shot data directory.

    Returns:
        pd.DataFrame: A single DataFrame containing all shot data for the year.
    """
    # Get all unique team IDs from game_dates
    unique_teams = GAME_DATES['TEAM_ID'].drop_duplicates()
    all_shot_data = []
    
    for team_id in unique_teams:
        season_dfs = []

        # Load Regular Season Data
        reg_season_path = os.path.join(base_path, str(year), f'{team_id}.csv')
        try:
            reg_df = pd.read_csv(reg_season_path)
            reg_df['season_type'] = 'REG'
            season_dfs.append(reg_df)
        except FileNotFoundError:
            pass

        # Load Postseason Data
        post_season_path = os.path.join(base_path, f'{year}ps', f'{team_id}.csv')
        try:
            post_df = pd.read_csv(post_season_path)
            post_df['season_type'] = 'PS'
            season_dfs.append(post_df)
        except FileNotFoundError:
            pass

        # Combine regular and postseason data for this team
        if season_dfs:
            combined_df = pd.concat(season_dfs, ignore_index=True)
            combined_df['year_source'] = year # Keep original year for saving structure
            combined_df['team_id'] = team_id
            all_shot_data.append(combined_df)
            
    if not all_shot_data:
        return pd.DataFrame()
        
    return pd.concat(all_shot_data, ignore_index=True)


def load_all_shot_data(years=range(2014, 2026), base_path='../shot_data/team'):
    """
    Loads shot data for all teams across multiple years (2014-2025 by default).

    Args:
        years (range or list): Years to load data for. Defaults to 2014-2025.
        base_path (str): The base path to the shot data directory.

    Returns:
        pd.DataFrame: A single DataFrame containing all shot data across all years.
    """
    print(f"Loading shot data for years: {list(years)}")
    all_years_data = []
    
    for year in years:
        print(f"Processing {year}... ", end="")
        year_data = load_all_shot_data_for_year(year, base_path)
        
        if not year_data.empty:
            all_years_data.append(year_data)
            rs_count = len(year_data[year_data['season_type'] == 'REG'])
            ps_count = len(year_data[year_data['season_type'] == 'PS'])
            print(f"Complete (RS: {rs_count:,}, PS: {ps_count:,})")
        else:
            print("No data found")
    
    if not all_years_data:
        print("No data loaded for any year.")
        return pd.DataFrame()
    
    final_df = pd.concat(all_years_data, ignore_index=True)
    print(f"\nTotal shots loaded: {len(final_df):,} across {len(years)} years")
    return final_df

def merge_with_uuid_data(shot_data_df, backup_file_path='../data_backup.csv'):
    """
    Merges the shot data with UUID data from the backup file and generates video URLs.
    
    Args:
        shot_data_df (pd.DataFrame): The loaded shot data
        backup_file_path (str): Path to the data_backup.csv file
    
    Returns:
        pd.DataFrame: Shot data merged with UUID information and video URLs
    """
    print("Loading UUID backup data...")
    backup_df = pd.read_csv(backup_file_path)
    
    # NOTE: The 'year' column from the shot data is dropped here, 
    # but a 'year' column is re-added from the backup_df during the merge.
    # We will use the `year_source` column created earlier for saving.
    if 'year' in shot_data_df.columns:
        shot_data_df.drop(columns='year', inplace=True)

    # Ensure proper data types for the merge
    shot_data_df['GAME_ID'] = shot_data_df['GAME_ID'].astype(str)
    shot_data_df['GAME_EVENT_ID'] = shot_data_df['GAME_EVENT_ID'].astype(int)
    backup_df['game_id'] = backup_df['game_id'].astype(str)
    backup_df['action_number'] = backup_df['action_number'].astype(int)
    
    print(f"Merging shot data ({len(shot_data_df):,} rows) with UUID data ({len(backup_df):,} rows)...")
    
    merged_df = pd.merge(
        shot_data_df,
        backup_df[['game_id', 'action_number', 'year', 'month', 'day', 'api_game_id', 'uuid']],
        left_on=['GAME_ID', 'GAME_EVENT_ID'],
        right_on=['game_id', 'action_number'],
        how='left'
    )
    
    # Clean up redundant columns from the merge
    merged_df.drop(columns=['game_id', 'action_number'], inplace=True)
    
    # Generate video URLs
    print("Generating video URLs...")
    def generate_video_url(row):
        if pd.isna(row['uuid']) or row['uuid'] == 'NO_VIDEO':
            return None
        return f"https://videos.nba.com/nba/pbp/media/{int(row['year'])}/{int(row['month'])}/{int(row['day'])}/{row['api_game_id']}/{int(row['GAME_EVENT_ID'])}/{row['uuid']}_1280x720.mp4"
    
    merged_df['video_url'] = merged_df.apply(generate_video_url, axis=1)
    
    # Report merge statistics
    uuid_matches = merged_df['uuid'].notna().sum()
    video_urls = merged_df['video_url'].notna().sum()
    no_video_count = (merged_df['uuid'] == 'NO_VIDEO').sum()
    
    print(f"Merge complete: {uuid_matches:,} shots matched with UUIDs ({uuid_matches/len(merged_df)*100:.1f}%)")
    print(f"Generated {video_urls:,} video URLs, {no_video_count:,} marked as NO_VIDEO")
    
    return merged_df


def save_data_by_year_and_team(merged_df, base_output_dir='shot_data_with_urls'):
    """
    Saves the final dataset into a directory structure by year and team_id.
    
    Args:
        merged_df (pd.DataFrame): The merged shot data with UUIDs and URLs.
                                  Must contain 'year_source' and 'team_id' columns.
        base_output_dir (str): The base directory to save the structured data.
    """
    print(f"\nSaving final dataset into directory structure: {base_output_dir}/")
    
    # Ensure necessary columns exist for structuring the output
    if 'year_source' not in merged_df.columns or 'team_id' not in merged_df.columns:
        print("Error: DataFrame must contain 'year_source' and 'team_id' columns to save.")
        return

    # Ensure data types are correct for file operations
    merged_df['year_source'] = merged_df['year_source'].astype(int)
    merged_df['team_id'] = merged_df['team_id'].astype(int)

    # Group data by the source year and team to save into individual files
    grouped = merged_df.groupby(['year_source', 'team_id'])
    
    for (year, team_id), group_df in grouped:
        # Create the directory for the year if it doesn't exist
        year_dir = os.path.join(base_output_dir, str(year))
        os.makedirs(year_dir, exist_ok=True)
        
        # Define the full path for the team's CSV file
        output_path = os.path.join(year_dir, f'{team_id}.csv')
        
        # Reorder columns to have SHOT_ID and video_url first for consistency
        final_columns = ['SHOT_ID', 'video_url'] + [col for col in group_df.columns if col not in ['SHOT_ID', 'video_url']]
        final_group_df = group_df[final_columns].copy()

        # Save the filtered DataFrame to a CSV
        final_group_df.to_csv(output_path, index=False)
        
    print(f"Successfully saved {len(merged_df):,} shots into {len(grouped)} year/team files.")


# --- Main Execution ---

# Complete workflow usage:
all_shot_data = load_all_shot_data()  # Loads 2014-2025
if not all_shot_data.empty:
    shot_data_with_uuid = merge_with_uuid_data(all_shot_data)
    save_data_by_year_and_team(shot_data_with_uuid)

# or specify custom years:
# print("\n--- Running for custom years (2020-2025) ---")
# recent_data = load_all_shot_data(range(2020, 2025))
# if not recent_data.empty:
#    recent_with_uuid = merge_with_uuid_data(recent_data)
#    save_data_by_year_and_team(recent_with_uuid, 'recent_shots_with_urls')