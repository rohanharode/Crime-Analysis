from pyspark.sql.functions import trim, col, regexp_replace, lower


def clean_data(df):
    crimes_dataset = df
    crimes_dataset = trim_and_lowercase(crimes_dataset)
    crimes_dataset = streamline_data(crimes_dataset)

    print('data cleaned')
    return crimes_dataset


def trim_and_lowercase(df):
    df = df.select(trim(col('id')).alias('id'), \
                   trim(col('casenumber')).alias('casenumber'), \
                   trim(col('occurrence_date')).alias('occurrence_date'), \
                   trim(lower(col('block'))).alias('block'), \
                   trim(lower(col('crimetype'))).alias('crimetype'), \
                   trim(lower(col('description'))).alias('description'), \
                   trim(lower(col('location_description'))).alias('location_description'), \
                   trim(col('arrest')).alias('arrest'), \
                   trim(col('year')).alias('year'), \
                   trim(col('latitude')).alias('latitude'), \
                   trim(col('longitude')).alias('longitude'), \
                   trim(col('hour')).alias('hour'), \
                   trim(col('am_pm')).alias('am_pm'), \
                   trim(col('location')).alias('location'), \
                   trim(col('month')).alias('month'), \
                   trim(col('district')).alias('district'), \
                   trim(col('ward')).alias('ward'), \
                   trim(col('communityarea')).alias('communityarea'), \
                   # trim(col('domestic')).alias('domestic'), \
                   # trim(col('iucr')).alias('iucr'), \
                   # trim(col('beat')).alias('beat'), \
                   # trim(col('fbicode')).alias('fbicode'), \
                   # trim(col('updatedon')).alias('updatedon'), \
                   # trim(col('x_coordinate')).alias('x_coordinate'), \
                   # trim(col('y_coordinate')).alias('y_coordinate'), \ \
 \
                   )

    return df


def streamline_data(df):
    df = df.select('id', 'casenumber', 'occurrence_date', 'arrest', \
                   regexp_replace('block', " ", "_").alias('block'), \
                   regexp_replace(col('crimetype'), " ", "_").alias('crimetype'), \
                   regexp_replace(col('description'), " ", "_").alias('description'), \
                   regexp_replace(col('location_description'), " ", "_").alias('location_description'), \
                   'year', 'latitude', 'longitude', 'hour', 'am_pm', 'location', 'month', 'district', 'ward',
                   'communityarea')

    return df