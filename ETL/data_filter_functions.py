from pyspark.sql.functions import to_timestamp, hour, substring, col, month
from pyspark.sql.types import IntegerType


def str_to_date(df):
    df = df.withColumn('occurrence_date', to_timestamp(df.date1, 'MM/dd/yyyy HH:mm:ss'))
    return df


def add_hour_column(df):
    df = df.withColumn('hour', hour(df.occurrence_date).cast(IntegerType()))
    return df


def add_month_column(df):
    df = df.withColumn('month', month(df.occurrence_date).cast(IntegerType()))
    return df


def add_am_pm_column(df):
    df = df.withColumn('am_pm', substring('date1', 21, 2))
    return df


def remove_null(df, null_check_column):
    for column_name in null_check_column:
        df = df.select('*').where(col(column_name).isNotNull())
    print('data check : unique')
    return df


def drop_duplicate_entries(df):
    df = df.dropDuplicates(['id'])
    df = df.dropDuplicates(['casenumber'])
    print('Dropped duplicate columns')
    return df


def filter_data(df):
    columns_to_drop = ['iucr', 'beat', 'date1', 'domestic', 'fbicode', \
                       'updatedon', 'x_coordinate', 'y_coordinate']
    df = str_to_date(df)
    df = add_hour_column(df)
    df = add_month_column(df)
    df = add_am_pm_column(df)
    df = df.drop(*columns_to_drop)
    print('data filtered')
    return df