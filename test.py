import os
import re
import time
import shutil
import pyodbc
import fnmatch
import numpy as np
import pandas as pd
import win32com.client
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

pd.options.mode.chained_assignment = None  # default='warn'

def copy_files(src_dst_pairs):
    for src_file, dst_file in src_dst_pairs:
        try:
            shutil.copy2(src_file, dst_file)
            # print(f"Copied {src_file} to {dst_file}")
        except Exception as e:
            print(f"file already exists {dst_file}:{e}")

def rename_files(file_pairs):
    for src_file, new_file_path in file_pairs:
        try:
            os.rename(src_file, new_file_path)
            # print(f"Renamed {src_file} to {new_file_path}")
        except Exception as e:
            print(f"file already exists {new_file_path}: {e}")

def process_date_folder(date_folder_name, input_dir, output_dir):
    try:
        # Create the main folder name
        main_folder_name = f"survey_data_{date_folder_name}"

        # Create the full path for the new directory
        survey_data_path = os.path.join(output_dir, main_folder_name)
        # Create the new directory
        os.makedirs(survey_data_path, exist_ok=True)

        # Create subdirectories
        subdirectories = ['Data', 'Output', 'PAVE', 'ROW']
        for subdirectory in subdirectories:
            subdirectory_path = os.path.join(survey_data_path, subdirectory)
            os.makedirs(subdirectory_path, exist_ok=True)

        # Paths for source directories
        data_folder_path = os.path.join(input_dir, date_folder_name, 'data')
        output_path = os.path.join(survey_data_path, 'Output')
        data_path = os.path.join(survey_data_path, 'Data')
        photo_directory = os.path.join(input_dir, date_folder_name, 'photo', date_folder_name)

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
        for root, dirs, files in os.walk(input_dir):
            for file_name in files:
                if file_name.endswith('.xlsx'):
                    src_file = os.path.join(root, file_name)
                    # Check if the .xlsx file belongs to the current date folder
                    if date_folder_name in root:
                        dst_file = os.path.join(output_path, file_name)
                        xlsx_files.append((src_file, dst_file))
        copy_files(xlsx_files)
        
        for data_output in os.listdir(data_folder_path):
            data_output_path = os.path.join(data_folder_path, data_output)
            if os.path.isdir(data_output_path):
                data_number = data_output.replace(date_folder_name, "").replace("RUN", "").lstrip("0")
                new_folder_data = f"{date_folder_name}_{data_number}"
                new_folder_data_path = os.path.join(data_path, new_folder_data)
                os.makedirs(new_folder_data_path, exist_ok=True)

        # Process Camera_GeoTagged and Log directories for the current date folder
        for run_folder in os.listdir(data_folder_path):
            run_folder_path = os.path.join(data_folder_path, run_folder)
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
                            new_file_name = f"{date_folder_name}_{run_number}-PAVE-0-{jpg_counter:05d}.jpg"
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
                            new_file_name = f"{date_folder_name}_{run_number}-ROW-0-{jpg_counter:05d}.jpg"
                            new_file_path = os.path.join(new_folder_path, new_file_name)
                            renamed_files.append((dst_file, new_file_path))
                            jpg_counter += 1

                    copy_files(jpg_files)
                    rename_files(renamed_files)

    except Exception as e:
        print(f"Error processing folder {date_folder_name}: {e}")

def copy_and_organize_files(input_dir, output_dir):
    try:
        # Create the destination parent directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Find all date folders in the source directory
        date_folders = [folder_name for folder_name in os.listdir(input_dir) if re.match(r'^\d{8}$', folder_name)]

        if not date_folders:
            print("No date folders found in the source directory.")
        else:
            with ThreadPoolExecutor(max_workers=100) as executor:
                future_to_date_folder = {executor.submit(process_date_folder, date_folder_name, input_dir, output_dir): date_folder_name for date_folder_name in date_folders}

                for future in as_completed(future_to_date_folder):
                    date_folder_name = future_to_date_folder[future]
                    try:
                        future.result()
                        print(f"✅ Processed folder: {date_folder_name} Successfully")
                    except Exception as exc:
                        print(f"{date_folder_name} generated an exception: {exc}")
    except Exception as e:
        print(f"Error in copy_and_organize_files: {e}")

# Generate random iri
def generate_parts(target_values, num_parts, tolerance):
    parts_list = []
    for target_value in target_values:
        total_sum = target_value * num_parts
        while True:
            # Generate random parts
            parts = np.random.uniform(low=total_sum / num_parts * 0.9, high=total_sum / num_parts * 1.1, size=num_parts)
            # Ensure the sum is correct
            if np.abs(np.sum(parts) - total_sum) < tolerance:
                parts_list.append(parts)
                break
            
    return parts_list

def process_csv_files(path):
    all_iri_dataframes = [] # empty list
    all_rutting_dataframes = [] # empty list
    
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
            iri_df['iri'] = (iri_df['iri left (m/km)'] + iri_df['iri right (m/km)']) / 2
            iri_df.drop(columns=['geometry'], errors='ignore', inplace=True)

            # Generate random values for iri_lane
            target_values = iri_df['iri']
            num_parts = 4
            tolerance = 0.3
            parts_list = generate_parts(target_values, num_parts, tolerance)

            # Expand DataFrame by repeating the rows
            iri_df = iri_df.loc[iri_df.index.repeat(num_parts)].reset_index(drop=True)
            iri_df['iri_lane'] = np.concatenate(parts_list)

            increment = 5 if fnmatch.fnmatch(filename, '*xw_iri_qgis*') else 5
            iri_df['event_start'] = range(0, len(iri_df) * increment, increment)
            iri_df['event_end'] = iri_df['event_start'] + increment

            # Append the processed IRI DataFrame to the list
            all_iri_dataframes.append(iri_df)

        # Process 'xw_rutting' files
        for filename in rutting_files:
            file_path = os.path.join(root, filename)
            rut_df = pd.read_csv(file_path, delimiter=';')
            rut_df.columns = rut_df.columns.str.strip()
            if 'Unnamed: 5' in rut_df.columns:
                rut_df.drop(columns=['Unnamed: 5'], inplace=True, errors='ignore')
            else:
                pass
            increment = 5 if fnmatch.fnmatch(filename, '*xw_rutting*') else 5
            rut_df['event_start'] = range(0, len(rut_df) * increment, increment)
            rut_df['event_end'] = rut_df['event_start'] + increment
            rut_df['chainage'] = rut_df['event_start']
            survey_code = filename.split('_')[2].split('.')[0]
            rut_df['survey_code'] = survey_code
            rut_df['rut_point_x'] = rut_df['qgis_shape'].apply(lambda x: float(x.split('(')[1].split(')')[0].split(',')[0].split(' ')[1]))
            rut_df['rut_point_y'] = rut_df['qgis_shape'].apply(lambda x: float(x.split('(')[1].split(')')[0].split(',')[0].split(' ')[0]))
            rut_df['rut_point_x'].fillna(0, inplace=True)
            rut_df['rut_point_y'].fillna(0, inplace=True)
        
            rut_df.rename(columns={'#Date':'Date', 'left rutting height': 'left_rutting', 'right rutting height': 'right_rutting', 'average height': 'avg_rutting'}, inplace=True)
            rut_df.drop(columns=['qgis_shape'], inplace=True)

            all_rutting_dataframes.append(rut_df)

    if all_iri_dataframes:
        iri_dataframes = pd.concat(all_iri_dataframes, ignore_index=True)
    else:
        iri_dataframes = pd.DataFrame()

    if all_rutting_dataframes:
        rutting_dataframes = pd.concat(all_rutting_dataframes, ignore_index=True)
    else:
        rutting_dataframes = pd.DataFrame()
        
    print(f"✅ Finished processing: .CSV files.")
    return iri_dataframes, rutting_dataframes

# edit join 9/9 
def left_join_dataframes(df_rutting, df_iri):
    return pd.merge(df_rutting, df_iri, how='left', on=['event_start', 'event_end', 'survey_code'], suffixes=('_rut', '_iri'))

def add_frame_num_to_joined_df(joined_df, derived_values, frame_numbers):
    joined_df['frame_num_ch'] = pd.NA
    joined_df['frame_num'] = pd.NA
    
    derived_to_frame_mapping = pd.DataFrame({
        'frame_num_ch': derived_values,
        'frame_num': frame_numbers
    })
    
    for i, frame_num_ch in enumerate(derived_values):
        mask = (joined_df['event_start'] <= frame_num_ch) & (joined_df['event_end'] > frame_num_ch)
        joined_df.loc[mask, 'frame_num_ch'] = frame_num_ch
        joined_df.loc[mask, 'frame_num'] = frame_numbers[i]
        
    return joined_df

def get_jpg_filenames(directory):
    jpg_dict = {}
    for root, dirs, files in os.walk(directory):
        jpg_files = [f for f in files if f.endswith('.jpg')]
        if jpg_files:
            folder_name = os.path.basename(os.path.dirname(root))
            jpg_dict[folder_name] = len(jpg_files)
            
    frame_df = pd.DataFrame(list(jpg_dict.items()), columns=['survey_code','frame_num'])
    frame_df['survey_code'] = frame_df['survey_code'].str.replace(
        r'_(\d+)', lambda m: f"RUN{int(m.group(1)):02d}", regex=True
    )
    
    detailed_df = pd.DataFrame(columns=['frame_num', 'survey_code'])
    for index, row in frame_df.iterrows():
        pic_counts = range(1, int(row['frame_num']) + 1)
        temp_df = pd.DataFrame({
            'frame_num': pic_counts,
            'survey_code': row['survey_code'],
        })
        detailed_df = pd.concat([detailed_df, temp_df], ignore_index=True)

    return detailed_df

# Perform fainal dataframe
def process_fainal_df(output_dir):
    frame_numbers_df = get_jpg_filenames(output_dir)  # This returns a DataFrame
    frame_numbers = frame_numbers_df['frame_num'].astype(int).tolist()
    iri_dataframes, rutting_dataframes = process_csv_files(output_dir)

    joined_df = left_join_dataframes(rutting_dataframes, iri_dataframes)
    
    grouped_df = joined_df.groupby('survey_code').agg(
        max_chainage=('chainage', 'max'),
        min_chainage=('chainage', 'min')
    ).reset_index()

    joined_df = pd.merge(joined_df, grouped_df, on='survey_code', how='left')
    
    max_event_start = joined_df['event_start'].max()

    # Calculate derived values
    derived_values = [round((max_event_start * num) / max(frame_numbers)) for num in frame_numbers]

    # Add frame numbers to the joined DataFrame
    final_df = add_frame_num_to_joined_df(joined_df, derived_values, frame_numbers)
    
    final_df = final_df.rename(columns={'rut_chainage':'chainage'})
    
    selected_columns = [
        'left_rutting', 'right_rutting', 'avg_rutting', 'event_start', 'event_end', 'survey_code',
        'rut_point_x', 'rut_point_y', 'Date', 'iri left (m/km)', 'iri right (m/km)', 'iri', 'iri_lane', 
        'chainage', 'max_chainage', 'min_chainage', 'pic_count', 'frame_num', 'frame_num_ch'
    ] 
    
    selected_columns = [col for col in selected_columns if col in final_df.columns]
    # final_df = final_df[final_df['iri'].notnull()][selected_columns]
    
    return final_df
# edit join 9/9 

def find_csv_files(start_dir, prefix='log_'):
    csv_files = []
    for dirpath, dirnames, filenames in os.walk(start_dir):
        for filename in fnmatch.filter(filenames, f'{prefix}*.xlsx'):
            csv_files.append(os.path.join(dirpath, filename))
    return csv_files

def main(final_df):
    for survey_date in os.listdir(output_dir): # eg. base_dir = r"D:\xenomatixs"
        path = os.path.join(output_dir, survey_date, 'Output')
        mdb = os.path.join(output_dir, survey_date, 'Data')
        
        log_csv_files = find_csv_files(path)
        if log_csv_files:
            log_df = pd.read_excel(log_csv_files[0])
            log_df.rename(columns={'ผิว': 'event_name', 'link_id ระบบ': 'section_id'}, inplace=True)
            log_df.columns = log_df.columns.str.strip()

            folder_names = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
            for folder_name in folder_names:
                print(f"🔄 Processing folder: {folder_name}")
                
                # Perform the initial merge and filter rows where frame_num is between numb_start and numb_end
                merged_df = pd.merge(final_df, log_df, how='left', on=['survey_code'], suffixes=('_final_df', '_log_df'))
                print(merged_df.columns)
                merged_df = merged_df[(merged_df['frame_num'] >= merged_df['numb_start']) & 
                                    (merged_df['frame_num'] <= merged_df['numb_end'])]
                
                # merged_df.to_csv('sss.csv')
                filtered_df = merged_df[merged_df['survey_code'] == folder_name]
                run_code = re.sub(r'RUN0*(\d+)', r'_\1', folder_name)
                
                # add filter_df as min_chainage and max_chainage is group by numb_start and numb_end and merge to merged_df use in process_key
                filter_df = merged_df.groupby(['numb_start', 'numb_end'], group_keys=False).agg(
                    min_chainage=('chainage', 'min'),
                    max_chainage=('chainage', 'max')
                ).reset_index()
                
                merged_df = pd.merge(merged_df, filter_df, on=['numb_start', 'numb_end'], how='left')
# .csv
                def process_val(df):
                    df['chainage'] = df['chainage']
                    df['lon'] = df['rut_point_y']
                    df['lat'] = df['rut_point_x']
                    df['iri_right'] = df['iri right (m/km)']
                    df['iri_left'] = df['iri left (m/km)']
                    df['iri'] = df['iri']
                    df['iri_lane'] = df['iri_lane']
                    df['rutt_right'] = df['right_rutting']
                    df['rutt_left'] = df['left_rutting']
                    df['rutting'] = df['avg_rutting']
                    df['texture'] = 0
                    df['etd_texture'] = 0
                    df['event_name'] = df['event_name'].str.lower()
                    df['frame_number'] = df['frame_num']
                    df['file_name'] = df['survey_code'].str.replace(r'RUN0*(\d+)', r'_\1', regex=True)
                    df['run_code'] = df['file_name'].str.split('_').str[-1]

                    return df

                processed_val = process_val(merged_df)

                selected_columns_val = [
                    'chainage', 'lon', 'lat', 'iri_right', 'iri_left', 'iri', 'iri_lane', 'rutt_right', 'rutt_left', 
                    'rutting', 'texture', 'etd_texture', 'event_name', 'frame_number', 'file_name', 'run_code'
                ]

                selected_columns_val = [col for col in selected_columns_val if col in processed_val.columns]
                processed_val_filename = os.path.join(mdb, 'access_valuelaser.csv')
                processed_val[selected_columns_val].to_csv(os.path.join(processed_val_filename), index=False)
                
                def process_dis(df):
                    df['chainage_pic'] = df['chainage']
                    df['frame_number'] = df['frame_num']
                    df['event_name'] = df['event_name'].str.lower()
                    df['name_key'] = df['survey_code'].str.replace(r'RUN0*(\d+)', r'_\1', regex=True)
                    df['run_code'] = df['file_name'].str.split('_').str[-1]

                    return df

                processed_dis = process_dis(merged_df)

                selected_columns_dis = [
                    'chainage_pic', 'frame_number', 'event_name', 'name_key', 'run_code'
                ]

                selected_columns_dis = [col for col in selected_columns_dis if col in processed_dis.columns]
                processed_dis_filename = os.path.join(mdb, 'access_distress_pic.csv')
                processed_dis[selected_columns_dis].to_csv(os.path.join(processed_dis_filename), index=False)
                
                def process_key(df):
                    df['event_str'] = df['min_chainage_y']
                    df['event_end'] = df['max_chainage_y']
                    df['event_num'] = df['event_name'].str[0].str.lower()
                    df['event_type'] = 'pave type'
                    df['event_name'] = df['event_name'].str.lower()
                    df['link_id'] = df['linkid']
                    df['lane_no'] = df['linkid'].apply(lambda x: x[11:13])
                    df['survey_date'] = df['date']
                    df['lat_str'] = df.groupby(['survey_code', 'linkid'])['rut_point_x'].transform('first')
                    df['lat_end'] = df.groupby(['survey_code', 'linkid'])['rut_point_x'].transform('last')
                    df['lon_str'] = df.groupby(['survey_code', 'linkid'])['rut_point_y'].transform('first')
                    df['lon_end'] = df.groupby(['survey_code', 'linkid'])['rut_point_y'].transform('last')
                    df['name_key'] = df['survey_code'].str.replace(r'RUN0*(\d+)', r'_\1', regex=True)
                    df['run_code'] = df['name_key'].str.split('_').str[-1]
                    
                    return df

                processed_key = merged_df.groupby('survey_code', group_keys=False).apply(process_key).reset_index(drop=True)
                processed_key = processed_key.groupby(['linkid', 'survey_date']).first().reset_index()

                selected_columns_key = [
                    'event_str', 'event_end', 'event_num', 'event_type', 'event_name', 'link_id', 'section_id', 
                    'km_start', 'km_end', 'length', 'lane_no', 'survey_date', 'lat_str', 'lat_end', 'lon_str', 
                    'lon_end', 'name_key', 'run_code'
                ]

                selected_columns_key = [col for col in selected_columns_key if col in processed_key.columns]
                processed_key_filename = os.path.join(mdb, 'access_key.csv')
                processed_key[selected_columns_key].sort_values(by=['run_code', 'event_str', 'event_end'], ascending=[True, True, False]).to_csv(os.path.join(processed_key_filename), index=False)
# .csv
# .mdb 
                mdb_folder_path = os.path.join(mdb, run_code)
                # print(f'store in: {mdb_folder_path}')
                mdb_path = os.path.join(mdb_folder_path, f'{run_code}_edit.mdb')
                # print(f'this name: {mdb_path}')
            
                if not os.path.isdir(mdb):
                    print(f"⛔ Directory not found: {mdb}")
                    continue
                
                def mdb_video_process(df):
                    df['CHAINAGE'] = df['chainage']
                    df['LRP_OFFSET'] = df['chainage']
                    df['LRP_NUMBER'] = 0
                    df['FRAME'] = df['frame_num']
                    df['GPS_TIME'] = 0
                    df['X'] = df['rut_point_y']
                    df['Y'] = df['rut_point_x']
                    df['Z'] = 0
                    df['HEADING'] = 0
                    df['PITCH'] = 0
                    df['ROLL'] = 0

                    return df

                video_process = mdb_video_process(filtered_df)
                
                selected_mdb_video_process = [
                    'CHAINAGE', 'LRP_OFFSET', 'LRP_NUMBER', 'FRAME', 'GPS_TIME', 
                    'X', 'Y', 'Z', 'HEADING', 'PITCH', 'ROLL'
                ]

                selected_mdb_video_process = [col for col in selected_mdb_video_process if col in video_process.columns]
                mdb_video_process_filename = os.path.join(mdb_folder_path, f'Video_Processed_{run_code}_2.csv')
                video_process[selected_mdb_video_process].to_csv(mdb_video_process_filename, index=False)
                
                mdb_video_header = pd.DataFrame({
                    'CAMERA': [1, 2],
                    'NAME': ['ROW-0', 'PAVE-0'],
                    'DEVICE': ['XENO', 'XENO'],
                    'SERIAL': ['6394983', '6394984'],
                    'INTERVAL': [5, 2],
                    'WIDTH': [0, 0],
                    'HEIGHT': [0, 0],
                    'FRAME_RATE': [0, 0],
                    'FORMAT': ['422 YUV 8', 'Mono 8'],
                    'X_SCALE': [0, 0.5],
                    'Y_SCALE': [0, 0.5],
                    'DATA_FORMAT': [-1, -1],
                    'PROCESSING_METHOD': [-1, -1],
                    'ENABLE_MOBILE_MAPPING': [True, False],
                    'DISP_PITCH': [0, 0],
                    'DISP_ROLL': [0, 0],
                    'DISP_YAW': [0, 0],
                    'DISP_X': [0, 0],
                    'DISP_Y': [0, 0],
                    'DISP_Z': [0, 0],
                    'HFOV': [0, 0],
                    'VFOV': [0, 0]
                })
                
                mdb_video_header_filename = os.path.join(mdb_folder_path, f'Video_Header_{run_code}.csv')
                mdb_video_header.to_csv(mdb_video_header_filename, index=False)
                
                def mdb_survey_header(df):
                    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df['SURVEY_ID'] = run_code
                    df['SURVEY_FILE'] = run_code
                    df['SURVEY_DESC'] = None
                    df['SURVEY_DATE'] = current_datetime
                    df['VEHICLE'] = 'ISS'
                    df['OPERATOR'] = 'ISS'
                    df['USER_1_NAME'] = None
                    df['USER_1'] = None
                    df['USER_2_NAME'] = None
                    df['USER_2'] = None
                    df['USER_3_NAME'] = None
                    df['USER_3'] = None
                    df['LRP_FILE'] = f'LRP_{run_code}'
                    df['LRP_RESET'] = 'N'
                    df['LRP_START'] = 0
                    df['CHAIN_INIT'] = 0
                    df['CHAIN_START'] = 0
                    df['CHAIN_END'] = round(df['numb_end'] * (df['max_chainage'] / df['pic_count'])).max()
                    df['SECT_LEN'] = 0
                    df['DIR'] = 'I'
                    df['LANE'] = 1
                    df['DEVICES'] = 'GPS-Geo-DR,LP_V3-LWP,LP_V3-RWP,TPL,Video'
                    df['OTHERSIDE'] = True
                    df['VERSION'] = '2.7.3.4/2.7.3.4'
                    df['MEMO'] = None
                    df['LENGTH'] = round(df['numb_end'] * (df['max_chainage'] / df['pic_count'])).max()

                    df = df.astype({
                        'SURVEY_DATE': 'datetime64[ns]',
                        'LRP_START': 'int',
                        'CHAIN_INIT': 'int',
                        'CHAIN_START': 'int',
                        'CHAIN_END': 'int',
                        'SECT_LEN': 'int',
                        'LANE': 'int',
                        'OTHERSIDE': 'bool',
                        'LENGTH': 'int'
                    })
                    
                    return df

                survey_header = mdb_survey_header(filtered_df)
                survey_header = survey_header.groupby(['SURVEY_ID']).first().reset_index()
                
                selected_mdb_survey_header = [
                    'SURVEY_ID', 'SURVEY_FILE', 'SURVEY_DESC', 'SURVEY_DATE', 'VEHICLE', 'OPERATOR', 'USER_1_NAME', 'USER_1', 
                    'USER_2_NAME', 'USER_2', 'USER_3_NAME', 'USER_3', 'LRP_FILE', 'LRP_RESET', 'LRP_START', 'CHAIN_INIT', 
                    'CHAIN_START','CHAIN_END', 'SECT_LEN', 'DIR', 'LANE', 'DEVICES', 'OTHERSIDE', 'VERSION', 'MEMO', 'LENGTH'
                ]

                selected_mdb_survey_header = [col for col in selected_mdb_survey_header if col in survey_header.columns]
                mdb_survey_header_filename = os.path.join(mdb_folder_path, f'Survey_Header_{run_code}.csv')
                survey_header[selected_mdb_survey_header].to_csv(mdb_survey_header_filename, index=False)
                
                def mdb_KeyCode_Raw(df):
                    df['CHAINAGE_START'] = df['min_chainage_y']
                    df['CHAINAGE_END'] = df['max_chainage_y']
                    df['EVENT'] = df['event_name'].str[0].str.lower()
                    df['SWITCH_GROUP'] = 'pave type.'
                    df['EVENT_DESC'] = df['event_name'].str.lower()
                    df['LATITUDE_START'] = df.groupby(['survey_code', 'linkid'])['rut_point_x'].transform('first')
                    df['LATITUDE_END'] = df.groupby(['survey_code', 'linkid'])['rut_point_x'].transform('last')
                    df['LONGITUDE_START'] = df.groupby(['survey_code', 'linkid'])['rut_point_y'].transform('first')
                    df['LONGITUDE_END'] = df.groupby(['survey_code', 'linkid'])['rut_point_y'].transform('last')
                    df['link_id'] = df['linkid']
                    df['section_id'] = df['section_id']
                    df['km_start'] = df['km_start']
                    df['km_end'] = df['km_end']
                    df['length'] = df['length']
                    df['lane_no'] = df['linkid'].apply(lambda x: x[11:13])
                    df['survey_date'] = df['date']
                    
                    return df

                KeyCode_Raw = merged_df.groupby('survey_code', group_keys=False).apply(mdb_KeyCode_Raw).reset_index(drop=True)
                KeyCode_Raw = KeyCode_Raw.groupby(['linkid', 'survey_date']).first().reset_index()
                KeyCode_Raw = KeyCode_Raw[KeyCode_Raw['survey_code'] == folder_name]

                selected_mdb_KeyCode_Raw = [
                    'CHAINAGE_START', 'CHAINAGE_END', 'EVENT', 'SWITCH_GROUP', 'EVENT_DESC', 'LATITUDE_START', 'LATITUDE_END', 
                    'LONGITUDE_START', 'LONGITUDE_END', 'link_id', 'section_id', 'km_start', 'km_end', 'length', 'lane_no', 
                    'survey_date'
                ]

                selected_mdb_KeyCode_Raw = [col for col in selected_mdb_KeyCode_Raw if col in KeyCode_Raw.columns]
                mdb_KeyCode_Raw_filename = os.path.join(mdb_folder_path, f'KeyCode_Raw_{run_code}.csv')
                KeyCode_Raw[selected_mdb_KeyCode_Raw].sort_values(by=['lane_no', 'CHAINAGE_START', 'CHAINAGE_END'], ascending=[True, True, False]).to_csv(mdb_KeyCode_Raw_filename, index=False)    
# .mdb 
# insert .mdb
                def create_access_db(db_path):
                    if os.path.isfile(db_path):
                        print(f"⛔ File already exists: {db_path}")
                    else:
                        access_app = win32com.client.Dispatch("Access.Application")
                        access_app.NewCurrentDatabase(db_path)
                        print(f"✅ Created new Access database at: {db_path}")
                        access_app.Quit()

                def table_exists(con, table_name):
                    try:
                        cur = con.cursor()
                        cur.execute(f"SELECT 1 FROM [{table_name}] WHERE 1=0;")
                        cur.close()
                        return True
                    except pyodbc.Error as e:
                        if '42S02' in str(e):  # '42S02' indicates a "table not found" error in SQL
                            return False
                        else:
                            raise e 

                def insert_csv_to_access(csv_path, table_name, access_db_path, max_retries=2, retry_delay=2):
                    conn_str = r"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={};".format(access_db_path)
                    con = None
                    retries = 0
                    
                    while retries < max_retries:
                        try:
                            con = pyodbc.connect(conn_str)
                            con.autocommit = False  # Turn off auto-commit for better performance
                            
                            if table_exists(con, table_name):
                                print(f"⏯️ Table {table_name} already exists. Skipping insertion.")
                                break
                            
                            cur = con.cursor()

                            start_time = time.time()

                            strSQL = (f"SELECT * INTO [{table_name}] "
                                    f"FROM [text;HDR=Yes;FMT=Delimited(,);Database={os.path.dirname(csv_path)}].{os.path.basename(csv_path)};")
                            cur.execute(strSQL)

                            con.commit()  # Commit the transaction after all operations

                            end_time = time.time()
                            print(f"⌛ Inserted table {table_name} in {end_time - start_time:.2f} seconds.")
                            break  # If the operation is successful, exit the retry loop

                        except pyodbc.Error as e:
                            error_code = e.args[0]
                            if error_code == 'HY000':
                                print(f"⌛ Database is locked, retrying in {retry_delay} seconds... (Attempt {retries + 1}/{max_retries})")
                                retries += 1
                                time.sleep(retry_delay)
                            else:
                                print(f"An error occurred: {e}")
                                break
                        except Exception as e:
                            print(f"⛔ An unexpected error occurred: {e}")
                            break
                        finally:
                            if con:
                                con.close()

                    if retries == max_retries:
                        print(f"⛔ Failed to insert table {table_name} after {max_retries} attempts.")

                def process_csv_files(csv_files, mdb_folder_path, mdb_path):
                    countc = os.cpu_count()
                    cpu = countc / 2
                    with ThreadPoolExecutor(max_workers=cpu) as executor:
                        futures = [executor.submit(insert_csv_to_access, os.path.join(mdb_folder_path, csv_name), table_name, mdb_path) for csv_name, table_name in csv_files.items()]
                        for future in as_completed(futures):
                            try:
                                future.result()  # Ensure exceptions are raised
                            except Exception as e:
                                print(f"⛔ Error processing CSV file: {e}")
                                
                create_access_db(mdb_path)

                csv_files = {
                    f'KeyCode_Raw_{run_code}.csv': f'KeyCode_Raw_{run_code}', 
                    f'Survey_Header_{run_code}.csv': f'Survey_Header',
                    f'Video_Header_{run_code}.csv': f'Video_Header_{run_code}',
                    f'Video_Processed_{run_code}_2.csv': f'Video_Processed_{run_code}_2'
                }
                
                process_csv_files(csv_files, mdb_folder_path, mdb_path)
                
                for csv_name in csv_files.keys():
                    os.remove(os.path.join(mdb_folder_path, csv_name))
# insert .mdb
    print(f"🎉 All files have been processed!. ")

def make_processed_file(base_dir):
    processed = os.path.join(base_dir, 'processed')
    input_dir = os.path.join(base_dir, 'input')
    date_folders = [folder_name for folder_name in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, folder_name)) and re.match(r'^\d{8}$', folder_name)]
    
    def move_folder(folder_name):
        shutil.move(
            os.path.join(input_dir, folder_name),
            os.path.join(processed, folder_name)
        )
    with ThreadPoolExecutor(max_workers=200) as executor:
        executor.map(move_folder, date_folders)
        
    print(f"🎉 Processed Done.")



if __name__ == "__main__":
    try:
        # base_dir = str(input(''))
        base_dir = r"D:\xenomatix"
        input_dir = os.path.join(base_dir, "input")
        output_dir = os.path.join(base_dir, "output")
        copy_and_organize_files(input_dir, output_dir)
        final_df = process_fainal_df(output_dir)
        main(final_df)
        # make_processed_file(base_dir)
    except Exception as e:
        print(f"⛔ Error in the main block: {e}")




# # edit ver.
# import pandas as pd
# import os
# import fnmatch

# # Constants
# DIRECTORY_PATH = r"D:\xenomatix\output\survey_data_20240726\Output\20240726RUN03"
# JPG_DIRECTORY_PATH = r"D:\xenomatix\output\survey_data_20240726\PAVE\20240726_3\PAVE-0"

# # Functions
# def get_jpg_filenames(directory):
#     return [filename for filename in os.listdir(directory) if filename.endswith(".jpg")]

# def extract_numeric_from_filenames(filenames):
#     return [int(fname.split('-')[-1].split('.jpg')[0]) for fname in filenames]

# def process_csv_file(file_path, increment):
#     df = pd.read_csv(file_path, delimiter=';')
    
#     df['event_start'] = 0
#     df['event_end'] = increment
    
#     event_start = 0
#     event_end = increment
    
#     for i in range(len(df)):
#         df.at[i, 'event_start'] = event_start
#         df.at[i, 'event_end'] = event_end
#         event_start = event_end
#         event_end += increment
    
#     return df

# def create_interval_rows(df, start_col='event_start', end_col='event_end', interval_size=5):
#     new_rows = []
    
#     for _, row in df.iterrows():
#         start = row[start_col]
#         end = row[end_col]
        
#         while start < end:
#             new_row = row.copy()
#             new_row[start_col] = start
#             new_row[end_col] = start + interval_size
#             new_rows.append(new_row)
#             start += interval_size
    
#     return pd.DataFrame(new_rows)

# def left_join_dataframes(df_rutting, df_iri):
#     return pd.merge(df_rutting, df_iri, how='left', on=['event_start', 'event_end'], suffixes=('_rutting', '_iri'))

# def add_frame_num_to_joined_df(joined_df, derived_values, frame_numbers):
#     joined_df['frame_num_ch'] = pd.NA
#     joined_df['frame_num'] = pd.NA
    
#     derived_to_frame_mapping = pd.DataFrame({
#         'frame_num_ch': derived_values,
#         'frame_num': frame_numbers
#     })
    
#     for i, frame_num_ch in enumerate(derived_values):
#         mask = (joined_df['event_start'] <= frame_num_ch) & (joined_df['event_end'] > frame_num_ch)
#         joined_df.loc[mask, 'frame_num_ch'] = frame_num_ch
#         joined_df.loc[mask, 'frame_num'] = frame_numbers[i]
    
#     return joined_df

# # Main
# jpg_filenames = get_jpg_filenames(JPG_DIRECTORY_PATH)
# frame_numbers = extract_numeric_from_filenames(jpg_filenames)

# dataframes = {}

# for filename in os.listdir(DIRECTORY_PATH):
#     if filename.endswith(".csv"):
#         file_path = os.path.join(DIRECTORY_PATH, filename)
#         increment = 20 if fnmatch.fnmatch(filename, '*xw_iri_qgis*') else 5
#         df = process_csv_file(file_path, increment)
        
#         # Create new rows based on intervals
#         new_df = create_interval_rows(df, interval_size=5)
        
#         # Store the new DataFrame in the dictionary with the filename as the key
#         dataframes[filename] = new_df

# print("Processed and updated CSV files into DataFrames.")

# # Perform the left join and store the result
# joined_dataframes = {}

# for rutting_file in dataframes:
#     if fnmatch.fnmatch(rutting_file, '*xw_rutting*'):
#         for iri_file in dataframes:
#             if fnmatch.fnmatch(iri_file, '*xw_iri_qgis*'):
#                 joined_df = left_join_dataframes(dataframes[rutting_file], dataframes[iri_file])
                
#                 max_event_start = joined_df['event_start'].max()
                
#                 derived_values = [round((max_event_start * num) / max(frame_numbers)) for num in frame_numbers]
#                 print('max_event_end = ', max_event_start)
#                 print('frame_numbers = ', max(frame_numbers))
#                 print("Derived Values:", derived_values)
#                 print("Count of derived_values:", len(derived_values))
#                 print("Frame Numbers:", frame_numbers)
#                 print("Count of frame_numbers:", len(frame_numbers))
                
#                 joined_df = add_frame_num_to_joined_df(joined_df, derived_values, frame_numbers)
               
#                 joined_dataframes[f"{rutting_file}_{iri_file}"] = joined_df

# print("Performed the left join and stored the result.")

# # Return the joined DataFrames with frame numbers
# result = joined_dataframes['xw_rutting_20240726RUN02.csv_xw_iri_qgis_20240726RUN02.csv'].to_csv('result.csv', index=False)

