import pandas as pd
import datetime




data_frame = pd.read_csv('kauia_dataset_excluded_extras.csv')
data_frame['Transaction Date'] = pd.to_datetime(data_frame['Transaction Date'])
#data_frame['Transaction Date'] = data_frame['Transaction Date'].dt.day_name()
first = data_frame.loc[data_frame['Transaction Date'].dt.day_name() == 'Monday'].first_valid_index()
last = data_frame.loc[data_frame['Transaction Date'].dt.day_name() == 'Sunday'].last_valid_index()
df_selected = data_frame.loc[(data_frame.index >= first) & (data_frame.index <= last)]

print(last)
df_selected.to_csv('dow_test_select_without_column.csv')