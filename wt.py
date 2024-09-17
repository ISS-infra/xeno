import os
import fnmatch
import pandas as pd

# path = r'D:\xenomatix\input\20240726x\data\20240726RUN01\Log\xw_rutting_20240726RUN01.csv'
path = r'D:\xenomatix\input\20240726x\data\20240726RUN01\Log'
iri_dataframes = {}
rutting_dataframes = {}
        
for root, dirs, files in os.walk(path):
    rutting_files = [f for f in files if f.endswith('.csv') and 'xw_rutting' in f]        
    print(rutting_files)
            
    # Process 'xw_rutting' files
    for filename in rutting_files:
        print(filename)
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

        # print(f"Updated {filename} into Rutting DataFrame.")

        print(f"✅ Finished processing: .CSV files.")