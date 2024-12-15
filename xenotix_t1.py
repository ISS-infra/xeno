import os
import pandas as pd

# Ex. 20240726RUN02
# Load rutting data
rut = r'D:\Xenomatix\20240726\data\20240726RUN02\Log\xw_rutting_20240726RUN02.csv'
rut_df = pd.read_csv(rut, sep=';')
rut_df = rut_df.drop(columns=['Unnamed: 5'])
survey_code = ((os.path.basename(rut)).split('_')[2]).split('.')[0]
for index, row in rut_df.iterrows():
    rut_df.at[index, 'rut_index'] = int(index * 25 / 5)
rut_df.set_index('rut_index', inplace=True)
rut_df['survey_code'] = survey_code 

# Load IRI data
iri = r'D:\Xenomatix\20240726\data\20240726RUN02\Log\xw_iri_qgis_20240726RUN02.csv'
iri_df = pd.read_csv(iri, sep=';')
survey_code = ((os.path.basename(rut)).split('_')[2]).split('.')[0]
for index, row in iri_df.iterrows():
    iri_df.at[index, 'index'] = int(index + 1)
iri_df.set_index('index', inplace=True)
iri_df['survey_code'] = survey_code 




iri = r'D:\Xenomatix\20240726\data\20240726RUN02\Log\xw_iri_qgis_20240726RUN02.csv'
iri_df = pd.read_csv(iri, sep=';')
survey_code = ((os.path.basename(iri)).split('_')[2]).split('.')[0]
for index, row in iri_df.iterrows():
    iri_df.at[index, 'index'] = int(index + 1)
iri_df.set_index('index', inplace=True)
iri_df['event_str'] = [i * 25 for i in range(len(iri_df))]
iri_df['event_end'] = [i * 25 + 25 for i in range(len(iri_df))]
iri_df['survey_code'] = survey_code 

# Load Log data
log = r'D:\Xenomatix\20240726\log_xenomatix_20240726.xlsx'
log_df = pd.read_excel(log)
for index, row in log_df.iterrows():
    log_df.at[index, 'index'] = int(index + 1)
log_df.set_index('index', inplace=True)

# Load Frame data
frame_num = r'D:\Xenomatix\20240726\data\20240726RUN02\Camera_GeoTagged'
frame_list = [f for f in os.listdir(frame_num) if f.endswith('.jpg')]
fra_df = pd.DataFrame({'frame': frame_list})
for index, row in fra_df.iterrows():
    fra_df.at[index, 'index'] = int(index + 1)
fra_df.set_index('index', inplace=True)


merged_df = log_df.merge(iri_df, on='survey_code', suffixes=('_log', '_iri'))

# Custom join
merged_df = pd.DataFrame()
for iri_index, iri_row in iri_df.iterrows():
    # Find all rows in rut_df where rut_df.index is between iri_row['event_str'] and iri_row['event_end']
    condition = (rut_df.index >= iri_row['event_str']) & (rut_df.index < iri_row['event_end'])
    matched_rows = rut_df[condition]

    # If there are matched rows, append them with the current iri_row
    if not matched_rows.empty:
        matched_rows = matched_rows.copy()
        if 'survey_code' in matched_rows.columns:
            matched_rows.drop(columns=['survey_code'], inplace=True)
        for col in iri_df.columns:
            matched_rows[col] = iri_row[col]
        merged_df = pd.concat([merged_df, matched_rows])

merged_df.reset_index(inplace=True)

# Define aggregation functions for all columns
aggregation_functions = {
    'rut_index': lambda x: list(x),  # Aggregate rut_index into a list lambda==convert to list
}

# Add all other columns with desired aggregation functions
for col in merged_df.columns:
    if col not in aggregation_functions and col not in ['event_str', 'event_end']:
        aggregation_functions[col] = lambda x: list(x)  # Convert to list 

# Group by [event_str, event_end] and apply aggregation functions
grouped_df = merged_df.groupby(['event_str', 'event_end', 'survey_code']).agg(aggregation_functions).reset_index()

# grouped_df.to_csv('grouped_iri_rut.csv', index=False)



# a.chainage == b.chainage and b.survey_code == a.survey_code
# เพิ่มชื่อไฟล์ใน dataframe เช่น 20240726RUN02 เพื่อจะเอามา join กับ log_xenomatix survey_code = survey_code
# ลูป frame ด้วยบัญัติไตรยาง find_chainage = (round((max_chainage / max_frame) * frame)) # == chainage



# # Merging DataFrames
# merged_df = pd.DataFrame()
# for iri_index, iri_row in iri_df.iterrows():
#     # Find all rows in rut_df where rut_df.index is between iri_row['event_str'] and iri_row['event_end']
#     condition = (rut_df.index >= iri_row['event_str']) & (rut_df.index < iri_row['event_end'])
#     matched_rows = rut_df[condition]

#     # If there are matched rows, append them with the current iri_row
#     if not matched_rows.empty:
#         matched_rows = matched_rows.copy()
#         if 'survey_code' in matched_rows.columns:
#             matched_rows.drop(columns=['survey_code'], inplace=True)
#         for col in iri_df.columns:
#             matched_rows[col] = iri_row[col]
#         merged_df = pd.concat([merged_df, matched_rows])

# # Reset index
# merged_df.reset_index(inplace=True)

# # Rename survey_code to run_code
# merged_df.rename(columns={'survey_code': 'run_code'}, inplace=True)

# # Define aggregation functions for all columns
# aggregation_functions = {
#     'rut_index': lambda x: list(x),
# }

# # Add all other columns with desired aggregation functions
# for col in merged_df.columns:
#     if col not in aggregation_functions and col not in ['event_str', 'event_end', 'run_code']:
#         aggregation_functions[col] = lambda x: list(x)

# # Group by [event_str, event_end, run_code] and apply aggregation functions
# grouped_df = merged_df.groupby(['event_str', 'event_end', 'run_code']).agg(aggregation_functions).reset_index()

# # Merging with log_df based on run_code
# final_df = grouped_df.merge(log_df, left_on='run_code', right_on='survey_code', suffixes=('_grouped', '_log'))



# max_chainage = rut_df.index.max()
# max_frame = fra_df.index.max()
# frame = log_df['numb_start'].iloc[0]
# find_chainage = (round((max_chainage / max_frame) * frame)) # == chainage 
# print(f"Max (chainage): {max_chainage}")
# print(f"Max (frame) number: {max_frame}")
# print(f"Chainage at frame {frame}: {find_chainage}")