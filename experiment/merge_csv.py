import os
import pandas as pd

# Directory containing folders with CSV files
root_directory = '..\data\input'

# List to store all DataFrames
all_dataframes = []

# Loop through each folder
for folder_name in os.listdir(root_directory):
    folder_path = os.path.join(root_directory, folder_name)

    print("folder path is ",folder_path)
    
    # Loop through each CSV file in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            
            # Read CSV file into DataFrame
            df = pd.read_csv(file_path,delimiter='|')
            
            # Append DataFrame to the list
            all_dataframes.append(df)

# Concatenate all DataFrames into a single DataFrame
combined_df = pd.concat(all_dataframes, ignore_index=True)

print(combined_df.shape)
# Write the combined DataFrame to a new CSV file
combined_df.to_csv('..\data\combined_data.csv', index=False)
