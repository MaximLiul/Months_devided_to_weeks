import pandas as pd
import datetime

def date_to_seconds(path_to_dataframe_in_csv, date_column_name, normalized_date_column_name,
                    unit_of_time='seconds'):  # unit_of_time = 'seconds' or 'days'
    data_frame = pd.read_csv(path_to_dataframe_in_csv)
    data_frame[date_column_name] = pd.to_datetime(data_frame[date_column_name])
    minimum_date = data_frame[date_column_name].min()
    if unit_of_time == 'seconds':
        data_frame[normalized_date_column_name] = data_frame[date_column_name].apply(lambda date: (date - minimum_date).seconds + (24*60*60)*(date - minimum_date).days)
    elif unit_of_time == 'days':
        data_frame[normalized_date_column_name] = data_frame[date_column_name].apply(lambda date: (date - minimum_date).days)
    elif unit_of_time == 'weeks':
        data_frame[normalized_date_column_name] = data_frame[date_column_name].apply(lambda date: (date - minimum_date).days // 7)
    else:
        return print('Not supported value of time units')
    return data_frame




def to_weeks_devision(path_to_dataframe_in_csv, date_column_name, normalized_date_column_name,
                            transaction_column, # if there is no a transaction column in the data_frame then put transaction_column = 'index'
                            column_for_analysis_name, last_n_weeks,
                            product_column='StockCode', user_column='CustomerID',
                            quantity_column='Product Quantity',
                            is_normalized=True,
                            select_certain_id=None,
                            output_file=None):  # for users_column the method counts visits; for product_column it counts quantity last_n_weeks):
    data_frame = date_to_seconds(path_to_dataframe_in_csv, date_column_name, normalized_date_column_name, unit_of_time='weeks')

    if transaction_column == 'index':
        data_frame[transaction_column] = data_frame.index

    #Creation of the column 'Weeks Marker' and putting values from 0 to 3 to them.
    data_frame.loc[
        data_frame[normalized_date_column_name] > data_frame[normalized_date_column_name].max() - last_n_weeks, 'Weeks Marker'] = data_frame[
        normalized_date_column_name].apply(lambda week_number: '{} last'.format(week_number % 4))

    data_frame.loc[
        data_frame[normalized_date_column_name] <= data_frame[normalized_date_column_name].max() - last_n_weeks, 'Weeks Marker'] = data_frame[
        normalized_date_column_name].apply(lambda week_number: '{}'.format(week_number % 4))

    list_of_selected_columns = [column_for_analysis_name, transaction_column, quantity_column, 'Weeks Marker']
    df = data_frame[list_of_selected_columns].copy()

    # if there is no a transaction column in the data_frame then we create it and assign an index value to the transaction column


    if column_for_analysis_name == product_column:
        df_grouped_by_column_for_analysis_time_interval = df.groupby([column_for_analysis_name, 'Weeks Marker'])[
            quantity_column].sum().unstack().fillna(0).reset_index()
    elif column_for_analysis_name == user_column:

        df_grouped_by_column_for_analysis_time_interval_transaction = df.groupby(
            [column_for_analysis_name, transaction_column, 'Weeks Marker'], as_index=False).count()

        # counting the number of visits for each user for a considered day of the week
        df_grouped_by_column_for_analysis_time_interval = \
            df_grouped_by_column_for_analysis_time_interval_transaction.groupby(
                [column_for_analysis_name, 'Weeks Marker'])[transaction_column].count().unstack().fillna(0).reset_index()
    else:
        return print('column_for_analysis_name value is not correct')

    marker_list = ['0', '0 last', '1', '1 last', '2', '2 last', '3', '3 last']

    df_grouped_by_column_for_analysis_time_interval['Total'] = df_grouped_by_column_for_analysis_time_interval[marker_list].sum(axis=1)
    # reordering of the columns
    columns = [column_for_analysis_name, '0', '1', '2', '3', '0 last', '1 last', '2 last', '3 last', 'Total']
    df_grouped_by_column_for_analysis_time_interval = df_grouped_by_column_for_analysis_time_interval[columns]

    # normalization: create a list of days of the week (columns of a df_grouped_by_column_for_analysis_time_interval) and
    # iterate over them, dividing on a total amount of visits/quantities
    if is_normalized:

        for marker in marker_list:
            df_grouped_by_column_for_analysis_time_interval[marker] = df_grouped_by_column_for_analysis_time_interval[
                                                                       marker] / df_grouped_by_column_for_analysis_time_interval['Total']

    if output_file is not None:
        if select_certain_id is None:
            return df_grouped_by_column_for_analysis_time_interval.sort_values('Total',
                                                                               ascending=False).to_csv(
                '{}.csv'.format(output_file), index=False)
        else:
            return df_grouped_by_column_for_analysis_time_interval.loc[
                df_grouped_by_column_for_analysis_time_interval.index == select_certain_id].to_csv(
                '{}.csv'.format(output_file), index=False)
    else:
        if select_certain_id is None:
            return df_grouped_by_column_for_analysis_time_interval
        else:
            return df_grouped_by_column_for_analysis_time_interval.loc[
                df_grouped_by_column_for_analysis_time_interval.index == select_certain_id]




to_weeks_devision('kauia_dataset_excluded_extras.csv', 'Transaction Date', 'Lala', 'index', 'Member ID', 4, product_column='Product Name', user_column='Member ID').sort_values('Total',
                                                                               ascending=False).to_excel('to_weeks_devision_user.xls')

#date_to_seconds('kauia_dataset_excluded_extras.csv', 'Transaction Date', 'Normalized Date', unit_of_time='weeks').to_csv('weeks.csv')