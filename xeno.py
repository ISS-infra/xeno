import os
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

iri = r"D:\Xenomatix\output\survey_data_20240726\Output\20240726RUN02"
pic = r"D:\Xenomatix\output\survey_data_20240726\PAVE\20240726_2\PAVE-0"
# iri = r'D:\Xenomatix\20240726\data\20240726RUN02\Log\xw_iri_qgis_20240726RUN02.csv'
# rut = r'D:\Xenomatix\20240726\data\20240726RUN02\Log\xw_rutting_20240726RUN02.csv'
# pic = r'D:\Xenomatix\20240726\data\20240726RUN02\Camera_GeoTagged'

# Plot the data
# rut_df.plot(marker='o', color='red', markersize=5, figsize=(10, 10))
# plt.title(f"Geographical Plot for Survey Code: {survey_code}")
# plt.xlabel("Longitude")
# plt.ylabel("Latitude")
# plt.grid(True)
# plt.show()


def get_jpg_filenames(directory):
    jpg_filenames = [filename for filename in os.listdir(directory) if filename.endswith(".jpg")]
    return jpg_filenames


def dataframe(path):
    
    directory = r"D:\Xenomatix\output\survey_data_20240726\PAVE\20240726_2\PAVE-0"
    directory_path = get_jpg_filenames(directory)
    # Loop through each file in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory_path, filename)
            # Read the CSV file into a DataFrame with semicolon delimiter
            df = pd.read_csv(file_path, delimiter=';')
            
            # Determine the increment values for event_start and event_end
            if fnmatch.fnmatch(filename, '*xw_iri_qgis*'):
                increment = 20
            else:
                increment = 5
            
            # Initialize event_start and event_end columns
            df['event_start'] = 0
            df['event_end'] = increment

            # Set the initial values
            event_start = 0
            event_end = increment

            # Loop through the DataFrame and update event_start and event_end
            for i in range(len(df)):
                df.at[i, 'event_start'] = event_start
                df.at[i, 'event_end'] = event_end
                event_start = event_end
                event_end += increment

            # Remove or replace newlines in 'geometry' column if it exists
            if 'geometry' in df.columns:
                df['geometry'] = df['geometry'].str.replace('\n', ' ', regex=False)

            # Create new rows based on intervals
            new_df = create_interval_rows(df, interval_size=5)
            
            # Store the new DataFrame in the dictionary with the filename as the key
            dataframes[filename] = new_df

            return new_df

def load_and_prepare_rut_data(rut):
    """Load and prepare IRI data from a CSV file."""
    rut_df = pd.read_csv(rut, sep=';')
    rut_df.columns = rut_df.columns.str.strip()
    rut_df = rut_df.drop(columns=['Unnamed: 5'])
    survey_code = ((os.path.basename(rut)).split('_')[2]).split('.')[0]
    for index, row in rut_df.iterrows():
        rut_df.at[index, 'rut_index'] = int(index * 25 / 5)
    rut_df.set_index('rut_index', inplace=True)
    rut_df['survey_code'] = survey_code
    rut_df['rut_point_x'] = rut_df['qgis_shape'].apply(lambda x: float(x.split('(')[1].split(')')[0].split(',')[0].split(' ')[1]))
    rut_df['rut_point_y'] = rut_df['qgis_shape'].apply(lambda x: float(x.split('(')[1].split(')')[0].split(',')[0].split(' ')[0]))
    rut_df['left_rutting'] = rut_df['left rutting height']
    rut_df['right_rutting'] = rut_df['right rutting height']
    rut_df['avg_rutting'] = rut_df['average height']
    rut_df.drop(columns=['qgis_shape', 'left rutting height', 'right rutting height', 'average height'], inplace=True)
    rut_df = gpd.GeoDataFrame(rut_df, geometry=gpd.points_from_xy(rut_df['rut_point_y'], rut_df['rut_point_x']))
    max_chainage = rut_df.index.max()
    
    return rut_df, max_chainage

# แก้ให้avgค่าที่แยกออกมาแล้วได้เท่าค่าเดิม เช่น 5 แยกออกมา 4 เป็นค่าที่รวมกลับมาแล้วได้เท่ากับ 20 ex. 18+22+17+23(80) / 4 = 20
def split_and_randomize(value, parts=4): 
    """Split and randomize a value into a specified number of parts."""
    random_parts = np.random.rand(parts)
    random_parts /= random_parts.sum()  # Normalize to ensure sum is equal to 1
    split_values = random_parts * value  # Scale fractions to match the original value
    
    return split_values

def load_and_prepare_iri_data(iri_df):
    """Load and prepare IRI data from a CSV file."""
    iri_df = pd.read_csv(iri, sep=';')
    iri_df.columns = iri_df.columns.str.strip()
    survey_code = os.path.basename(iri).split('_')[3].split('.')[0]

    # Create 'iri_lane', 'index', 'survey_code' columns
    iri_df['iri_lane'] = (iri_df['iri left (m/km)'] + iri_df['iri right (m/km)']) / 2
    iri_df['index'] = range(1, len(iri_df) + 1)
    iri_df['survey_code'] = survey_code

    # Create 'iri_index' column with intervals of 5, starting from 0 and Set 'index'
    iri_df['iri_index'] = (iri_df['index'] - 1) * 5    
    iri_df.set_index('index', inplace=True)
    
    expanded = []

    for _, row in iri_df.iterrows():
        split_values = split_and_randomize(row['iri_lane'])
        for value in split_values:
            expanded.append({
                "index": row.name,
                "Date": row['Date'],
                "iri": value,
                "iri_left_(m/km)": row['iri left (m/km)'],
                "iri_right_(m/km)": row['iri right (m/km)'],
                "iri_lane": row['iri_lane'],
                "iri_Std_left_(m/km)": row['iri Std left (m/km)'],
                "iri_Std_right_(m/km)": row['iri Std right (m/km)'],
                "worst_iri_(m/km)": row['worst iri (m/km)'],
                "iri_difference_(m/km)": row['iri difference (m/km)'],
                "survey_code": row['survey_code'],
                "iri_index": row['iri_index']
            })

    # Create expanded DataFrame
    expanded_df = pd.DataFrame(expanded)

    # Correct the 'iri_index' in the expanded DataFrame to have increments of 5
    expanded_df['iri_index'] = range(0, len(expanded_df) * 5, 5)
    expanded_df['event_str'] = [i * 5 for i in range(len(expanded_df))]
    expanded_df['event_end'] = [i * 5 + 5 for i in range(len(expanded_df))]
    
    return expanded_df

def load_and_prepare_pic_data(pic, max_chainage):
    frame_list = [f for f in os.listdir(pic) if f.endswith('.jpg')]
    fra_df = pd.DataFrame({'frame': frame_list})
    for index, row in fra_df.iterrows():
        fra_df.at[index, 'index'] = int(index + 1)
    fra_df.set_index('index', inplace=True)
    max_frame = fra_df.index.max()
    fra_df['chainage_pic'] = fra_df.index * (max_chainage / max_frame)
    fra_df['chainage_pic'] = fra_df['chainage_pic'].round(0)
    
    return fra_df


a = dataframe(iri)
b = get_jpg_filenames(pic)

rut_df, max_chainage = load_and_prepare_rut_data(rut)
iri_df = load_and_prepare_iri_data(iri)
pic_df = load_and_prepare_pic_data(pic, max_chainage)


meg_df = pd.merge(rut_df, iri_df, left_on='rut_index', right_on='iri_index', how='left')

# Domez

import pandas as pd
import os
import fnmatch

# Define the path to your directories
directory_path = r"D:\test_xeno\123\xenomatix\output\survey_data_20240726\Output\20240726RUN02"
jpg_directory_path = r"D:\test_xeno\123\xenomatix\output\survey_data_20240726\PAVE\20240726_2\PAVE-0"

# Dictionary to store DataFrames
dataframes = {}

# Function to create new rows based on intervals
def create_interval_rows(df, start_col='event_start', end_col='event_end', interval_size=5):
    new_rows = []
    
    for _, row in df.iterrows():
        start = row[start_col]
        end = row[end_col]
        
        while start < end:
            new_row = row.copy()
            new_row[start_col] = start
            new_row[end_col] = start + interval_size
            new_rows.append(new_row)
            start += interval_size
    
    return pd.DataFrame(new_rows)

# Function to get .jpg file names
def get_jpg_filenames(directory):
    jpg_filenames = [filename for filename in os.listdir(directory) if filename.endswith(".jpg")]
    return jpg_filenames

# Function to extract numeric part from .jpg filenames
def extract_numeric_from_filenames(filenames):
    numeric_values = [int(fname.split('-')[-1].split('.jpg')[0]) for fname in filenames]
    return numeric_values

# Get the .jpg file names
jpg_filenames = get_jpg_filenames(jpg_directory_path)
frame_numbers = extract_numeric_from_filenames(jpg_filenames)

# Loop through each file in the directory
for filename in os.listdir(directory_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(directory_path, filename)
        # Read the CSV file into a DataFrame with semicolon delimiter
        df = pd.read_csv(file_path, delimiter=';')
        
        # Determine the increment values for event_start and event_end
        if fnmatch.fnmatch(filename, '*xw_iri_qgis*'):
            increment = 20
        else:
            increment = 5
        
        # Initialize event_start and event_end columns
        df['event_start'] = 0
        df['event_end'] = increment

        # Set the initial values
        event_start = 0
        event_end = increment

        # Loop through the DataFrame and update event_start and event_end
        for i in range(len(df)):
            df.at[i, 'event_start'] = event_start
            df.at[i, 'event_end'] = event_end
            event_start = event_end
            event_end += increment

        # Remove or replace newlines in 'geometry' column if it exists
        if 'geometry' in df.columns:
            df['geometry'] = df['geometry'].str.replace('\n', ' ', regex=False)

        # Create new rows based on intervals
        new_df = create_interval_rows(df, interval_size=5)
        
        # Store the new DataFrame in the dictionary with the filename as the key
        dataframes[filename] = new_df

        print(f"Processed and updated {filename} into DataFrame.")

# Perform the left join on xw_rutting and xw_iri_qgis
def left_join_dataframes(df_rutting, df_iri):
    joined_df = pd.merge(df_rutting, df_iri, how='left', on=['event_start', 'event_end'], suffixes=('_rutting', '_iri'))
    return joined_df

def add_frame_num_to_joined_df(joined_df, derived_values, frame_numbers):
    # Create new columns 'frame_num_ch' and 'frame_num' initialized with NaN
    joined_df['frame_num_ch'] = pd.NA
    joined_df['frame_num'] = pd.NA

    # Create a DataFrame to map derived values to frame numbers
    derived_to_frame_mapping = pd.DataFrame({
        'frame_num_ch': derived_values,
        'frame_num': frame_numbers
    })

    # Additional logic to update frame_num_ch and frame_num based on derived_values
    for i, frame_num_ch in enumerate(derived_values):
        mask = (joined_df['event_start'] <= frame_num_ch) & (joined_df['event_end'] > frame_num_ch)
        joined_df.loc[mask, 'frame_num_ch'] = frame_num_ch
        joined_df.loc[mask, 'frame_num'] = frame_numbers[i]

    return joined_df


# Perform the left join and store the result
joined_dataframes = {}
for rutting_file in dataframes:
    if fnmatch.fnmatch(rutting_file, '*xw_rutting*'):
        for iri_file in dataframes:
            if fnmatch.fnmatch(iri_file, '*xw_iri_qgis*'):
                joined_df = left_join_dataframes(dataframes[rutting_file], dataframes[iri_file])
                
                # Get the maximum event_end
                max_event_start = joined_df['event_start'].max()
                
                # Process the .jpg filenames to extract the derived values
                derived_values = [round((max_event_start * num) / max(frame_numbers)) for num in frame_numbers]
                print('max_event_end = ', max_event_end)
                print('frame_numbers = ', max(frame_numbers))
                print("Derived Values:", derived_values)
                print("Count of derived_values:", len(derived_values))
                print("Frame Numbers:", frame_numbers)
                print("Count of frame_numbers:", len(frame_numbers))
                
                # Add frame_num and frame_num_ch to the joined DataFrame
                joined_df = add_frame_num_to_joined_df(joined_df, derived_values, frame_numbers)
               
                # Store the joined DataFrame in the dictionary
                joined_dataframes[f"{rutting_file}_{iri_file}"] = joined_df

# Print the joined DataFrames with frame numbers
for key in joined_dataframes:
    print(f"\nJoined DataFrame with frame numbers for {key}:")
    print(joined_dataframes[key].to_string(index=False))
    


import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

def copy_files(src_dst_pairs):
    for src_file, dst_file in src_dst_pairs:
        shutil.copy2(src_file, dst_file)

def rename_files(file_pairs):
    for src_file, new_file_path in file_pairs:
        os.rename(src_file, new_file_path)

def process_date_folder(date_folder_name, source_directory, destination_parent_directory):
    # Create the main folder name
    main_folder_name = f"survey_data_{date_folder_name}"

    # Create the full path for the new directory
    survey_data_path = os.path.join(destination_parent_directory, main_folder_name)

    # Create the new directory
    os.makedirs(survey_data_path, exist_ok=True)

    # Create subdirectories
    subdirectories = ['Data', 'Output', 'PAVE', 'ROW']
    for subdirectory in subdirectories:
        subdirectory_path = os.path.join(survey_data_path, subdirectory)
        os.makedirs(subdirectory_path, exist_ok=True)

    # Paths for source directories
    data_folder_path = os.path.join(source_directory, date_folder_name, 'data')
    output_path = os.path.join(survey_data_path, 'Output')
    photo_directory = os.path.join(source_directory, date_folder_name, 'photo', date_folder_name)

    # Check if the Data directory exists for the current date
    if os.path.exists(data_folder_path):
        for folder_name in os.listdir(data_folder_path):
            folder_path = os.path.join(data_folder_path, folder_name)
            if os.path.isdir(folder_path):
                # Create each folder found in the Data directory inside the Output directory
                output_folder_path = os.path.join(output_path, folder_name)
                os.makedirs(output_folder_path, exist_ok=True)

    # Copy .xlsx files from the source directory to the Output subdirectory
    xlsx_files = []
    for root, dirs, files in os.walk(source_directory):
        for file_name in files:
            if file_name.endswith('.xlsx'):
                src_file = os.path.join(root, file_name)
                # Check if the .xlsx file belongs to the current date folder
                if date_folder_name in root:
                    dst_file = os.path.join(output_path, file_name)
                    xlsx_files.append((src_file, dst_file))
    copy_files(xlsx_files)

    # Process Camera_GeoTagged and Log directories for the current date folder
    for run_folder in os.listdir(data_folder_path):
        run_folder_path = os.path.join(data_folder_path, run_folder)
        # print(run_folder)
        if os.path.isdir(run_folder_path):
            # Process Camera_GeoTagged
            camera_geotagged_path = os.path.join(run_folder_path, 'Camera_GeoTagged')
            if os.path.exists(camera_geotagged_path):
                run_number = run_folder.replace(date_folder_name, "").replace("RUN", "").lstrip("0")
                new_folder_name = f"{date_folder_name}_{run_number}"
                new_folder_path = os.path.join(survey_data_path, 'PAVE', new_folder_name, 'PAVE-0')
                os.makedirs(new_folder_path, exist_ok=True)

                # Copy .jpg files to the new folder and rename them
                jpg_files = []
                renamed_files = []
                jpg_counter = 1
                for file_name in os.listdir(camera_geotagged_path):
                    if file_name.endswith('.jpg'):
                        src_file = os.path.join(camera_geotagged_path, file_name)
                        dst_file = os.path.join(new_folder_path, file_name)
                        jpg_files.append((src_file, dst_file))

                        # Rename the file
                        new_file_name = f"{date_folder_name}_{run_number}_PAVE-0-{jpg_counter:05d}.jpg"
                        new_file_path = os.path.join(new_folder_path, new_file_name)
                        renamed_files.append((dst_file, new_file_path))
                        jpg_counter += 1

                copy_files(jpg_files)
                rename_files(renamed_files)

            # Process Log
            log_path = os.path.join(run_folder_path, 'Log')
            if os.path.exists(log_path):
                for file_name in os.listdir(log_path):
                    if file_name.endswith(f'{run_folder}.csv'):
                        csv_path = os.path.join(log_path, file_name)
                        destination_subfolder_path = os.path.join(output_path, run_folder)
                        os.makedirs(destination_subfolder_path, exist_ok=True)
                        shutil.copy2(csv_path, destination_subfolder_path)

    # Process ROW directory within photo directory
    if os.path.exists(photo_directory):
        for photo_run_folder in os.listdir(photo_directory):
            photo_run_folder_path = os.path.join(photo_directory, photo_run_folder)
            if os.path.isdir(photo_run_folder_path):
                # Process ROW
                run_number = photo_run_folder.replace(date_folder_name, "").replace("RUN", "").lstrip("0")
                new_folder_name = f"{date_folder_name}_{run_number}"
                new_folder_path = os.path.join(survey_data_path, 'ROW', new_folder_name, 'ROW-0')
                os.makedirs(new_folder_path, exist_ok=True)

                # Copy .jpg files to the new folder and rename them
                jpg_files = []
                renamed_files = []
                jpg_counter = 1
                for file_name in os.listdir(photo_run_folder_path):
                    if file_name.endswith('.jpg'):
                        src_file = os.path.join(photo_run_folder_path, file_name)
                        dst_file = os.path.join(new_folder_path, file_name)
                        jpg_files.append((src_file, dst_file))

                        # Ensure unique file name
                        new_file_name = f"{date_folder_name}_{run_number}_ROW-0-{jpg_counter:05d}.jpg"
                        new_file_path = os.path.join(new_folder_path, new_file_name)
                        renamed_files.append((dst_file, new_file_path))
                        jpg_counter += 1

                copy_files(jpg_files)
                rename_files(renamed_files)

def copy_and_organize_files(source_directory, destination_parent_directory):
    # Create the destination parent directory if it doesn't exist
    os.makedirs(destination_parent_directory, exist_ok=True)

    # Find all date folders in the source directory
    date_folders = [folder_name for folder_name in os.listdir(source_directory) if re.match(r'^\d{8}$', folder_name)]

    if not date_folders:
        print("No date folders found in the source directory.")
    else:
        with ThreadPoolExecutor(max_workers=100) as executor:
            future_to_date_folder = {executor.submit(process_date_folder, date_folder_name, source_directory, destination_parent_directory): date_folder_name for date_folder_name in date_folders}

            for future in as_completed(future_to_date_folder):
                date_folder_name = future_to_date_folder[future]
                try:
                    future.result()
                    print(f"✅ Processed folder: {date_folder_name} Successfully")
                except Exception as exc:
                    print(f"{date_folder_name} generated an exception: {exc}")

if __name__ == "__main__":
    source_directory = r"D:\Xenomatix\input"
    destination_parent_directory = r"D:\Xenomatix\output"
    copy_and_organize_files(source_directory, destination_parent_directory)