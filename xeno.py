import os
import fnmatch
import numpy as np
import pandas as pd

# pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Define the path to your directories
path = r"D:\xenomatixs\output\survey_data_20240726\Output"
pic = r"D:\xenomatixs\output\survey_data_20240726\PAVE"
log = r'D:\xenomatixs\output\survey_data_20240726\Output'

def split_and_randomize(value, parts=4):
    random_parts = np.random.rand(parts)
    random_parts /= random_parts.sum()  # Normalize to ensure sum is equal to 1
    split_values = random_parts * value  # Scale fractions to match the original value
    return split_values

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
            iri_df['iri'] = (iri_df['iri left (m/km)'] + iri_df['iri right (m/km)']) / 2        
            iri_df.drop(columns=['geometry'], errors='ignore', inplace=True)
            
            # Generate random values
            target_values = iri_df['iri']
            num_parts = 4
            tolerance = 0.3
            parts_list = generate_parts(target_values, num_parts, tolerance)

            # Expand DataFrame by repeating the rows
            iri_df = iri_df.loc[iri_df.index.repeat(num_parts)].reset_index(drop=True)
            iri_df['iri_lane'] = np.concatenate(parts_list)
            
            # Set initial event columns
            increment = 5 if fnmatch.fnmatch(filename, '*xw_iri_qgis*') else 5
            iri_df['chainage'] = iri_df.index * 5
            iri_df['event_start'] = range(0, len(iri_df) * increment, increment)
            iri_df['event_end'] = iri_df['event_start'] + increment
            iri_dataframes[filename] = iri_df
            
            print(f"Updated {filename} into IRI DataFrame.")
        
        # Process 'xw_rutting' files
        for filename in rutting_files:
            file_path = os.path.join(root, filename)
            rut_df = pd.read_csv(file_path, delimiter=';')
            rut_df.columns = rut_df.columns.str.strip()
            rut_df.drop(columns=['Unnamed: 5'], inplace=True, errors='ignore')
            increment = 5 if fnmatch.fnmatch(filename, '*xw_rutting*') else 5
            rut_df['event_start'] = range(0, len(rut_df) * increment, increment)
            rut_df['event_end'] = rut_df['event_start'] + increment
            
            survey_code = filename.split('_')[2].split('.')[0]
            rut_df['index'] = rut_df.index * 25 // 5
            rut_df.set_index('index', inplace=True)
            rut_df['survey_code'] = survey_code
            rut_df['rut_point_x'] = rut_df['qgis_shape'].apply(lambda x: float(x.split('(')[1].split(')')[0].split(',')[0].split(' ')[1]))
            rut_df['rut_point_y'] = rut_df['qgis_shape'].apply(lambda x: float(x.split('(')[1].split(')')[0].split(',')[0].split(' ')[0]))
            rut_df['rut_point_x'].fillna(0, inplace=True)
            rut_df['rut_point_y'].fillna(0, inplace=True)
        
            rut_df.rename(columns={'left rutting height': 'left_rutting', 'right rutting height': 'right_rutting', 'average height': 'avg_rutting'}, inplace=True)
            rut_df.drop(columns=['qgis_shape'], inplace=True)
            rutting_dataframes[filename] = rut_df

            print(f"Updated {filename} into Rutting DataFrame.")

    return iri_dataframes, rutting_dataframes

# Perform the left join on xw_rutting and xw_iri_qgis
def left_join_dataframes(df_rutting, df_iri):
    joined_df = pd.merge(df_rutting, df_iri, how='left', on=['event_start', 'event_end', 'survey_code'], suffixes=('_rutting', '_iri'))
    return joined_df

# Perform jpg file and frame number
def get_jpg_names_and_nums(directory):
    jpg_dict = {}
    
    for root, dirs, files in os.walk(directory):
        jpg_files = [f for f in files if f.endswith('.jpg')]
        if jpg_files:
            folder_name = os.path.basename(os.path.dirname(root))
            jpg_dict[folder_name] = len(jpg_files)

    frame_df = pd.DataFrame(list(jpg_dict.items()), columns=['survey_code', 'pic_count'])
    frame_df['survey_code'] = frame_df['survey_code'].str.replace('_', 'RUN0')

    return jpg_files, frame_df

# use

jpg_files, frame_df = get_jpg_names_and_nums(pic)
iri_dataframes, rutting_dataframes = process_csv_files(path)

joined_dataframes = {}
for rutting_file in rutting_dataframes:
    for iri_file in iri_dataframes:
        if 'xw_rutting' in rutting_file and 'xw_iri_qgis' in iri_file:
            joined_df = left_join_dataframes(rutting_dataframes[rutting_file], iri_dataframes[iri_file])

            # Group by survey_code and calculate min and max chainage
            grouped_df = joined_df.groupby('survey_code').agg(
                max_chainage=('chainage', 'max'),
                min_chainage=('chainage', 'min')
            ).reset_index()

            joined_df = pd.merge(joined_df, grouped_df, on='survey_code', how='left')
            joined_df = pd.merge(joined_df, frame_df, on='survey_code', how='left')
            joined_df['frame_num'] = joined_df.groupby(['survey_code', 'pic_count']).cumcount() + 1
            joined_df['chainage_pic'] = round(joined_df['max_chainage'] / joined_df['pic_count'] * joined_df['frame_num'],0)
            joined_dataframes[f"{rutting_file}_{iri_file}"] = joined_df
            
final_df = pd.concat(joined_dataframes.values(), ignore_index=True)
final_df = final_df[final_df['iri'].notnull()]
final_df.to_csv('final_df.csv', index=False)

def find_csv_files(start_dir, prefix='log_'):
    csv_files = []
    for dirpath, dirnames, filenames in os.walk(start_dir):
        for filename in fnmatch.filter(filenames, f'{prefix}*.xlsx'):
            csv_files.append(os.path.join(dirpath, filename))
    return csv_files

log_csv_files = find_csv_files(log)
if log_csv_files:
    log_df = pd.read_excel(log_csv_files[0])

    # Rename columns and clean up column names
    log_df.rename(columns={'ผิว': 'event_name', 'link_id ระบบ': 'section_id'}, inplace=True)
    log_df.columns = log_df.columns.str.strip()

    # Perform the initial merge and filter rows where frame_num is between numb_start and numb_end
    merged_df = pd.merge(final_df, log_df, how='left', on=['survey_code'], suffixes=('_final_df', '_log_df'))
    merged_df = merged_df[(merged_df['frame_num'] >= merged_df['numb_start']) & 
                          (merged_df['frame_num'] <= merged_df['numb_end'])]

    def process_val(df):
        df['chainage'] = df['chainage_pic']
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
        df['file_name'] = df['survey_code'].str.replace('RUN0', '_')
        df['run_code'] = df['file_name'].str.split('_').str[-1]

        return df

    processed_val = process_val(merged_df)

    selected_columns_val = [
        'chainage', 'lon', 'lat', 'iri_right', 'iri_left', 'iri', 'iri_lane', 'rutt_right', 'rutt_left', 
        'rutting', 'texture', 'etd_texture', 'event_name', 'frame_number', 'file_name', 'run_code'
    ]

    selected_columns_val = [col for col in selected_columns_val if col in processed_val.columns]
    processed_val[selected_columns_val].to_csv('access_valuelaser.csv', index=False)
    
    def process_dis(df):
        df['chainage_pic'] = df['chainage_pic']
        df['frame_number'] = df['frame_num']
        df['event_name'] = df['event_name'].str.lower()
        df['name_key'] = df['survey_code'].str.replace('RUN0', '_')
        df['run_code'] = df['file_name'].str.split('_').str[-1]

        return df

    processed_dis = process_dis(merged_df)

    selected_columns_dis = [
        'chainage_pic', 'frame_number', 'event_name', 'name_key', 'run_code'
    ]

    selected_columns_dis = [col for col in selected_columns_dis if col in processed_dis.columns]
    processed_dis[selected_columns_dis].to_csv('access_distress_pic.csv', index=False)
    
    def process_key(df):
        df['event_str'] = round(df['numb_start'] * (df['max_chainage'] / df['pic_count']))
        df['event_end'] = round(df['numb_end'] * (df['max_chainage'] / df['pic_count']))
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
        df['name_key'] = df['survey_code'].str.replace('RUN0', '_')
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
    processed_key[selected_columns_key].sort_values(by=['run_code', 'event_str', 'event_end'], ascending=[True, True, False]).to_csv('access_key.csv', index=False)
    
# 

import os
import fnmatch
import pandas as pd

log = r'D:\xenomatixs\output\survey_data_20240726\Output'

def find_csv_files(start_dir, prefix='log_'):
    csv_files = []
    for dirpath, dirnames, filenames in os.walk(start_dir):
        for filename in fnmatch.filter(filenames, f'{prefix}*.xlsx'):
            csv_files.append(os.path.join(dirpath, filename))
    return csv_files

log_csv_files = find_csv_files(log)
if log_csv_files:
    log_df = pd.read_excel(log_csv_files[0])

    # Rename columns and clean up column names
    log_df.rename(columns={'ผิว': 'event_name', 'link_id ระบบ': 'section_id'}, inplace=True)
    log_df.columns = log_df.columns.str.strip()

    # Perform the initial merge and filter rows where frame_num is between numb_start and numb_end
    merged_df = pd.merge(final_df, log_df, how='left', on=['survey_code'], suffixes=('_final_df', '_log_df'))
    merged_df = merged_df[(merged_df['frame_num'] >= merged_df['numb_start']) & 
                          (merged_df['frame_num'] <= merged_df['numb_end'])]

    def process_val(df):
        df['chainage'] = df['chainage_pic']
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
        df['file_name'] = df['survey_code'].str.replace('RUN0', '_')
        df['run_code'] = df['file_name'].str.split('_').str[-1]

        return df

    processed_val = process_val(merged_df)

    selected_columns_val = [
        'chainage', 'lon', 'lat', 'iri_right', 'iri_left', 'iri', 'iri_lane', 'rutt_right', 'rutt_left', 
        'rutting', 'texture', 'etd_texture', 'event_name', 'frame_number', 'file_name', 'run_code'
    ]

    selected_columns_val = [col for col in selected_columns_val if col in processed_val.columns]
    processed_val[selected_columns_val].to_csv('access_valuelaser.csv', index=False)
    
    def process_dis(df):
        df['chainage_pic'] = df['chainage_pic']
        df['frame_number'] = df['frame_num']
        df['event_name'] = df['event_name'].str.lower()
        df['name_key'] = df['survey_code'].str.replace('RUN0', '_')
        df['run_code'] = df['file_name'].str.split('_').str[-1]

        return df

    processed_dis = process_dis(merged_df)

    selected_columns_dis = [
        'chainage_pic', 'frame_number', 'event_name', 'name_key', 'run_code'
    ]

    selected_columns_dis = [col for col in selected_columns_dis if col in processed_dis.columns]
    processed_dis[selected_columns_dis].to_csv('access_distress_pic.csv', index=False)
    

    def process_key(df):
        df['event_str'] = round(df['numb_start'] * (df['max_chainage'] / df['pic_count']))
        df['event_end'] = round(df['numb_end'] * (df['max_chainage'] / df['pic_count']))
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
        df['name_key'] = df['survey_code'].str.replace('RUN0', '_')
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
    processed_key[selected_columns_key].sort_values(by=['run_code', 'event_str', 'event_end'], ascending=[True, True, False]).to_csv('access_key.csv', index=False)
    
# 

import os
import fnmatch
import pandas as pd
from datetime import datetime

log = r'D:\xenomatixs\output\survey_data_20240726\Output'

def find_csv_files(start_dir, prefix='log_'):
    csv_files = []
    found_filenames = []
    for dirpath, dirnames, filenames in os.walk(start_dir):
        for filename in fnmatch.filter(filenames, f'{prefix}*.xlsx'):
            csv_files.append(os.path.join(dirpath, filename))
            found_filenames.append(filename)  # Store the filenames
    return csv_files, found_filenames

folder_names = [name for name in os.listdir(log) if os.path.isdir(os.path.join(log, name))]
log_csv_files, filenames = find_csv_files(log)

if log_csv_files:
    log_df = pd.read_excel(log_csv_files[0])

    # Rename columns and clean up column names
    log_df.rename(columns={'ผิว': 'event_name', 'link_id ระบบ': 'section_id'}, inplace=True)
    log_df.columns = log_df.columns.str.strip()

    for folder_name in folder_names:
        print(f"Processing folder: {folder_name}")

        # Perform the initial merge and filter rows where frame_num is between numb_start and numb_end
        merged_df = pd.merge(final_df, log_df, how='left', on=['survey_code'], suffixes=('_final_df', '_log_df'))
        merged_df = merged_df[(merged_df['frame_num'] >= merged_df['numb_start']) & 
                              (merged_df['frame_num'] <= merged_df['numb_end'])]
        
        filtered_df = merged_df[merged_df['survey_code'] == folder_name]
        run_code = folder_name.replace('RUN0', '_')
        
        def mdb_video_process(df):
            df['CHAINAGE'] = df['chainage_pic']
            df['LRP_OFFSET'] = df['chainage_pic']
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
        mdb_video_process_filename = f'D:/xeno/mdb/Video_Processed_{run_code}_2.csv'
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

        mdb_video_header_filename = f'D:/xeno/mdb/Video_Header_{run_code}.csv'
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
            df['CHAIN_END'] = round(df['numb_end'] * (df['max_chainage'] / df['pic_count']))
            df['SECT_LEN'] = 0
            df['DIR'] = 'I'
            df['LANE'] = 1
            df['DEVICES'] = 'GPS-Geo-DR,LP_V3-LWP,LP_V3-RWP,TPL,Video'
            df['OTHERSIDE'] = True
            df['VERSION'] = '2.7.3.4/2.7.3.4'
            df['MEMO'] = None
            df['LENGTH'] = round(df['numb_end'] * (df['max_chainage'] / df['pic_count']))

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
        mdb_survey_header_filename = f'D:/xeno/mdb/Survey_Header_{run_code}.csv'
        survey_header[selected_mdb_survey_header].to_csv(mdb_survey_header_filename, index=False)
        
        def mdb_KeyCode_Raw(df):
            df['CHAINAGE_START'] = round(df['numb_start'] * (df['max_chainage'] / df['pic_count']))
            df['CHAINAGE_END'] = round(df['numb_end'] * (df['max_chainage'] / df['pic_count']))
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
        KeyCode_Raw[selected_mdb_KeyCode_Raw].sort_values(by=['lane_no', 'CHAINAGE_START', 'CHAINAGE_END'], ascending=[True, True, False]).to_csv(f'D:/xeno/mdb/KeyCode_Raw_{run_code}.csv', index=False)
        
# 

import os
import pyodbc
import win32com.client

def create_access_db(db_path):
    if os.path.isfile(db_path):
        print(f"File already exists: {db_path}")
    else:
        access_app = win32com.client.Dispatch("Access.Application")
        access_app.NewCurrentDatabase(db_path)
        print(f"Created new Access database at: {db_path}")
        access_app.Quit()

def insert_csv_to_access(csv_path, table_name, access_db_path):
    conn_str = r"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={};".format(access_db_path)
    con = None
    try:
        con = pyodbc.connect(conn_str)
        con.autocommit = False  # Turn off auto-commit for better performance
        cur = con.cursor()

        # Start timing the operation
        import time
        start_time = time.time()

        strSQL = (f"SELECT * INTO [{table_name}] "
                  f"FROM [text;HDR=Yes;FMT=Delimited(,);Database={os.path.dirname(csv_path)}].{os.path.basename(csv_path)};")
        cur.execute(strSQL)
        
        con.commit()  # Commit the transaction after all operations
        
        # End timing and log the time taken
        end_time = time.time()
        print(f"Inserted table {table_name} in {end_time - start_time:.2f} seconds.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        if con:
            con.rollback()
    finally:
        if con:
            con.close()



log = r'D:\xenomatixs\output\survey_data_20240726\Output'
patmdb = r'D:\xeno\mdb'

folder_names = [name for name in os.listdir(log) if os.path.isdir(os.path.join(log, name))]

for folder_name in folder_names:
    run_code = folder_name.replace('RUN0', '_')
    mdb_path = os.path.join(patmdb, f'{run_code}_edit.mdb')
    create_access_db(mdb_path)

    # Define CSV files and corresponding table names
    csv_files = {
        f'KeyCode_Raw_{run_code}.csv': f'KeyCode_Raw_{run_code}', 
        # f'Survey_Header_{run_code}.csv': f'Survey_Header_{run_code}',
        f'Survey_Header_{run_code}.csv': f'Survey_Header',
        f'Video_Header_{run_code}.csv': f'Video_Header_{run_code}',
        f'Video_Processed_{run_code}_2.csv': f'Video_Processed_{run_code}_2'
    }

    for csv_name, table_name in csv_files.items():
        csv_path = os.path.join(patmdb, csv_name)
        
        # Insert cleaned CSV into Access database
        insert_csv_to_access(csv_path, table_name, mdb_path)
        # os.remove(csv_path)
        
print(f"All CSV files have been processed and imported into the Access database.")   