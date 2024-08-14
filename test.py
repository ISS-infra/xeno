import os
import fnmatch
import numpy as np
import pandas as pd

# pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Define the path to your directories
path = r"D:\xenomatixs\output\survey_data_20240726\Output"
pic = r"D:\xenomatixs\output\survey_data_20240726\PAVE\20240726_2\PAVE-0"

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

# Function to get .jpg file names and numeric values
def get_jpg_names_and_nums(directory):
    jpg_filenames = [filename for filename in os.listdir(directory) if filename.endswith(".jpg")]
    numeric_values = [int(fname.split('-')[-1].split('.jpg')[0]) for fname in jpg_filenames]
    return jpg_filenames, numeric_values

def split_and_randomize(value, parts=4):
    random_parts = np.random.rand(parts)
    random_parts /= random_parts.sum()  # Normalize to ensure sum is equal to 1
    split_values = random_parts * value  # Scale fractions to match the original value
    return split_values

# Find all relevant CSV files and process them
def process_csv_files(path):
    iri_dataframes = {}
    rutting_dataframes = {}

    for root, dirs, files in os.walk(path):
        # Find files
        iri_files = [f for f in files if f.endswith('.csv') and 'xw_iri_qgis' in f]
        rutting_files = [f for f in files if f.endswith('.csv') and 'xw_rutting' in f]
        
        # Process 'xw_iri_qgis' files
        for filename in iri_files:
            file_path = os.path.join(root, filename)
            iri_df = pd.read_csv(file_path, delimiter=';')
            iri_df.columns = iri_df.columns.str.strip()
            survey_code = filename.split('_')[3].split('.')[0]
            iri_df['survey_code'] = survey_code
            iri_df['iri_lane'] = (iri_df['iri left (m/km)'] + iri_df['iri right (m/km)']) / 2
            iri_df['index'] = iri_df.groupby('survey_code').cumcount() + 1
            iri_df.drop(columns=['geometry'], errors='ignore', inplace=True)
            
            iri_df['iri'] = iri_df['iri_lane'].apply(lambda x: split_and_randomize(x, parts=4))
            
            # Set initial event columns
            increment = 20 if fnmatch.fnmatch(filename, '*xw_iri_qgis*') else 5
            iri_df['event_start'] = range(0, len(iri_df) * increment, increment)
            iri_df['event_end'] = iri_df['event_start'] + increment

            iri_df = create_interval_rows(iri_df, interval_size=5)
            iri_dataframes[filename] = iri_df

            print(f"Updated {filename} into IRI DataFrame.")
        
        # Process 'xw_rutting' files
        for filename in rutting_files:
            file_path = os.path.join(root, filename)
            rut_df = pd.read_csv(file_path, delimiter=';')
            rut_df.columns = rut_df.columns.str.strip()
            rut_df.drop(columns=['Unnamed: 5'], inplace=True, errors='ignore')
            increment = 20 if fnmatch.fnmatch(filename, '*xw_rutting*') else 5
            
            rut_df['event_start'] = range(0, len(rut_df) * increment, increment)
            rut_df['event_end'] = rut_df['event_start'] + increment
            rut_df = create_interval_rows(rut_df, interval_size=5)
            
            survey_code = filename.split('_')[2].split('.')[0]
            rut_df['index'] = rut_df.index * 25 // 5
            rut_df.set_index('index', inplace=True)
            rut_df['survey_code'] = survey_code
            rut_df['rut_point_x'] = rut_df['qgis_shape'].apply(lambda x: float(x.split('(')[1].split(')')[0].split(',')[0].split(' ')[1]))
            rut_df['rut_point_y'] = rut_df['qgis_shape'].apply(lambda x: float(x.split('(')[1].split(')')[0].split(',')[0].split(' ')[0]))
            rut_df.rename(columns={'left rutting height': 'left_rutting', 'right rutting height': 'right_rutting', 'average height': 'avg_rutting'}, inplace=True)
            rut_df.drop(columns=['qgis_shape'], inplace=True)
            rutting_dataframes[filename] = rut_df

            print(f"Updated {filename} into Rutting DataFrame.")

    return iri_dataframes, rutting_dataframes

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

# Get JPG names, frame values and Process CSV to DataFrames
jpg_filenames, frame_numbers = get_jpg_names_and_nums(pic)
iri_dataframes, rutting_dataframes = process_csv_files(path)

# Perform the left join and store the result
joined_dataframes = {}
for rutting_file in rutting_dataframes:
    for iri_file in iri_dataframes:
        if 'xw_rutting' in rutting_file and 'xw_iri_qgis' in iri_file:
            joined_df = left_join_dataframes(iri_dataframes[iri_file], rutting_dataframes[rutting_file])
            max_event_start = joined_df['event_start'].max()
            joined_df['chainage'] = joined_df.index * 25 // 5
                  
            # Process the .jpg filenames to extract the derived values
            derived_values = [round((max_event_start * num) / max(frame_numbers)) for num in frame_numbers]   
            joined_df = add_frame_num_to_joined_df(joined_df, derived_values, frame_numbers)
            # Store the joined DataFrame in the dictionary
            joined_dataframes[f"{rutting_file}_{iri_file}"] = joined_df

# Combine all joined DataFrames into one final DataFrame
final_df = pd.concat(joined_dataframes.values(), ignore_index=True)
final_df.to_csv('final_df_test.csv', index=False)
print("Final DataFrame:")
print(final_df)



