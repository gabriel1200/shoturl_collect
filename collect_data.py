import pandas as pd
import os
import glob

def combine_shot_data(base_dir='shot_data_with_urls', output_file='all_shot_data_combined.csv'):
    """
    Walks through a directory structure of year/team_id.csv files,
    combines them into a single pandas DataFrame, and saves it to a CSV file.

    Args:
        base_dir (str): The root directory containing the year folders.
        output_file (str): The name of the final combined CSV file.
    """
    # Check if the base directory exists
    if not os.path.isdir(base_dir):
        print(f"Error: The directory '{base_dir}' was not found.")
        print("Please make sure you have run the collection script first or that the directory is in the correct path.")
        return

    # Use glob to find all CSV files within the year subdirectories
    search_path = os.path.join(base_dir, '**', '*.csv')
    all_csv_files = glob.glob(search_path, recursive=True)

    if not all_csv_files:
        print(f"No CSV files were found in '{base_dir}'.")
        return

    print(f"Found {len(all_csv_files)} CSV files to combine. Reading and concatenating...")

    # List to hold each DataFrame
    df_list = []

    # Loop through the files, read them, and append to the list
    for filename in all_csv_files:
        try:
            df = pd.read_csv(filename)
            df_list.append(df)
        except Exception as e:
            print(f"Could not read file {filename}: {e}")

    # Concatenate all DataFrames in the list into a single DataFrame
    if not df_list:
        print("No data was loaded. The output file will not be created.")
        return
        
    print("Combining all dataframes...")
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.sort_values(by=['year','month','day','SHOT_ID'],inplace=True)

    # Save the final combined DataFrame to a CSV file
    print(f"Saving combined data to '{output_file}'...")
    combined_df.to_csv(output_file, index=False)

    print("\nProcess complete!")
    print(f"Total shots combined: {len(combined_df):,}")
    print(f"Data saved successfully to '{output_file}'.")

if __name__ == "__main__":
    # This will run when the script is executed directly
    combine_shot_data()
