from ETL.data_clean_functions import clean_data
from ETL.data_filter_functions import filter_data, remove_null, drop_duplicate_entries
from ETL.load_csv import read_csv


def chicago_etl_data():
    crimes_dataset = read_csv()

    null_check_columns = ['id', 'casenumber', 'location_description']
    crimes_dataset = remove_null(crimes_dataset, null_check_columns)

    crimes_dataset = drop_duplicate_entries(crimes_dataset)

    crimes_dataset = filter_data(crimes_dataset)

    processed_data = clean_data(crimes_dataset)

    processed_data.show()

    return processed_data