from cassandra.cluster import Cluster
from pyspark.sql.functions import desc, col
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession, functions

from ETL.chicago_etl import chicago_etl_data

cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('data operations').getOrCreate()
spark = SparkSession.builder.appName('Chicago Crime').config('spark.cassandra.connection.host',
                                                             ','.join(cluster_seeds)).getOrCreate()
sc = spark.sparkContext

"""*******************************************************  FETCHING DATA   ***********************************************************************"""


def get_processed_data(file_format='csv', KEYSPACE='pirates'):
    if file_format == 'cassandra':
        data = spark.read.format("org.apache.spark.sql.cassandra").options(table='chicagocrime',
                                                                           keyspace=KEYSPACE).load()
    else:
        data = spark.read.format('csv').load('./datafile/mycsv/*.csv', header=True)

    ## Datatable columns
    ## id, casenumber, arrest, block, crimetype, description, hour, latitude, longitude, location, location_description, month, occurrence_date, x_coordinate, y_coordinate, year

    major_crimetypes = ['theft', 'battery', 'criminal_damage', 'narcotics', 'other_offense',\
                        'assault', 'burglary', 'motor_vehicle_theft', 'deceptive_practice',\
                        'robbery', 'criminal_trespass']

    # Adding major crimetype column and removing null hour entries to allow date conversion to hour
    data = data.select('*').withColumn('major_crime', data.crimetype.isin(major_crimetypes).astype('int')) \
        .where(col('hour').isNotNull())
    data.show()
    return data


## Read datasets for graph/plots
def get_csv(filename):
    dataset = spark.read.format('csv').load('../BigData/datafile/graph_dataset/' + filename + '/*.csv', header=True)
    return dataset


## Get tables from cassandra db from given keyspace
def get_cassandra_table(table_name, KEYSPACE='pirates'):
    dataset = spark.read.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=KEYSPACE).load()
    return dataset


"""*******************************************************  TABLE   CREATION   ***********************************************************************"""


## Primary crime count (each individual crime)
def get_crimetype(data):
    crimetype = data.select('id', 'crimetype')
    crimetype = crimetype.groupby('crimetype').agg(functions.count(crimetype['id']).alias('count')).sort(desc('count'))
    return crimetype


## Total Crimetype each year
def get_crimetype_yearly(data):
    crimetype_yearly = data.select('id', 'year', 'crimetype')
    crimetype_yearly = crimetype_yearly.groupby('year', 'crimetype').agg(functions.count(crimetype_yearly['id']) \
                                                                         .alias('count')).sort('year', desc('count'))
    return crimetype_yearly


## Total crime in all locations
def get_location_crime(data):
    location_crime = data.select('id', 'location_description')
    location_crime = location_crime.groupby('location_description').agg(functions.count(location_crime['id']) \
                                                                        .alias('count')).sort(desc('count'))
    return location_crime


## Hourly Intensity of Crime
def get_hourly_crime(data):
    hourly_crime = data.withColumn('hour_info', data.hour.cast(IntegerType()))
    hourly_crime = hourly_crime.groupby('hour_info', 'am_pm').agg(functions.count(hourly_crime['id']).alias('count')) \
        .sort('am_pm', 'hour_info')
    return hourly_crime


## Hourly major crime
def get_hourly_major_crime(data):
    hourly_major_crime = data.withColumn('hour_info', data.hour.cast(IntegerType()))
    hourly_major_crime = hourly_major_crime.select('id', 'hour_info', 'am_pm', 'major_crime', 'crimetype').where(
        col('major_crime') == 1)
    hourly_major_crime = hourly_major_crime.select('id', 'hour_info', 'am_pm', 'crimetype').groupBy('hour_info',
                                                                                                    'am_pm',
                                                                                                    'crimetype') \
        .agg(functions.count(hourly_major_crime['id']).alias('count')).sort('am_pm', 'hour_info')
    return hourly_major_crime


## Generating monthly major crime for year 2010
def get_crime_2010(data):
    crime_2010 = data.withColumn('month_number', data.month.cast(IntegerType()))
    crime_2010 = crime_2010.select('id', 'month_number', 'crimetype', 'year').where(col('major_crime') == 1).where(
        col('year') == 2010)
    crime_2010 = crime_2010.groupBy('month_number', 'crimetype') \
        .agg(functions.count(crime_2010['id']).alias('count')).sort('month_number', 'crimetype')
    return crime_2010


## Generating monthly major crime for year 2011
def get_crime_2011(data):
    crime_2011 = data.withColumn('month_number', data.month.cast(IntegerType()))
    crime_2011 = crime_2011.select('id', 'month_number', 'crimetype', 'year').where(col('major_crime') == 1).where(
        col('year') == 2011)
    crime_2011 = crime_2011.groupBy('month_number', 'crimetype') \
        .agg(functions.count(crime_2011['id']).alias('count')).sort('month_number', 'crimetype')
    return crime_2011


## Generating monthly major crime for year 2012
def get_crime_2012(data):
    crime_2012 = data.withColumn('month_number', data.month.cast(IntegerType()))
    crime_2012 = crime_2012.select('id', 'month_number', 'crimetype', 'year').where(col('major_crime') == 1).where(
        col('year') == 2012)
    crime_2012 = crime_2012.groupBy('month_number', 'crimetype') \
        .agg(functions.count(crime_2012['id']).alias('count')).sort('month_number', 'crimetype')
    return crime_2012


## Generating monthly major crime for year 2013
def get_crime_2013(data):
    crime_2013 = data.withColumn('month_number', data.month.cast(IntegerType()))
    crime_2013 = crime_2013.select('id', 'month_number', 'crimetype', 'year').where(col('major_crime') == 1).where(
        col('year') == 2013)
    crime_2013 = crime_2013.groupBy('month_number', 'crimetype') \
        .agg(functions.count(crime_2013['id']).alias('count')).sort('month_number', 'crimetype')
    return crime_2013


## Generating monthly major crime for year 2014
def get_crime_2014(data):
    crime_2014 = data.withColumn('month_number', data.month.cast(IntegerType()))
    crime_2014 = crime_2014.select('id', 'month_number', 'crimetype', 'year').where(col('major_crime') == 1).where(
        col('year') == 2014)
    crime_2014 = crime_2014.groupBy('month_number', 'crimetype') \
        .agg(functions.count(crime_2014['id']).alias('count')).sort('month_number', 'crimetype')
    return crime_2014


## Generating monthly major crime for year 2015
def get_crime_2015(data):
    crime_2015 = data.withColumn('month_number', data.month.cast(IntegerType()))
    crime_2015 = crime_2015.select('id', 'month_number', 'crimetype', 'year').where(col('major_crime') == 1).where(
        col('year') == 2015)
    crime_2015 = crime_2015.groupBy('month_number', 'crimetype') \
        .agg(functions.count(crime_2015['id']).alias('count')).sort('month_number', 'crimetype')
    return crime_2015


## Generating monthly major crime for year 2016
def get_crime_2016(data):
    crime_2016 = data.withColumn('month_number', data.month.cast(IntegerType()))
    crime_2016 = crime_2016.select('id', 'month_number', 'crimetype', 'year').where(col('major_crime') == 1).where(
        col('year') == 2016)
    crime_2016 = crime_2016.groupBy('month_number', 'crimetype') \
        .agg(functions.count(crime_2016['id']).alias('count')).sort('month_number', 'crimetype')
    return crime_2016


## Generating monthly major crime for year 2017
def get_crime_2017(data):
    crime_2017 = data.withColumn('month_number', data.month.cast(IntegerType()))
    crime_2017 = crime_2017.select('id', 'month_number', 'crimetype', 'year').where(col('major_crime') == 1).where(
        col('year') == 2017)
    crime_2017 = crime_2017.groupBy('month_number', 'crimetype') \
        .agg(functions.count(crime_2017['id']).alias('count')).sort('month_number', 'crimetype')
    return crime_2017


## Generating monthly major crime for year 2018
def get_crime_2018(data):
    crime_2018 = data.withColumn('month_number', data.month.cast(IntegerType()))
    crime_2018 = crime_2018.select('id', 'month_number', 'crimetype', 'year').where(col('major_crime') == 1).where(
        col('year') == 2018)
    crime_2018 = crime_2018.groupBy('month_number', 'crimetype') \
        .agg(functions.count(crime_2018['id']).alias('count')).sort('month_number', 'crimetype')
    return crime_2018


## Generating table for location wise plots
def get_geolocation(data):
    geolocation = data.select('id', 'year', 'crimetype', 'major_crime', 'latitude', 'longitude').where(
        col('major_crime') == 1).where(col('year') >= 2010)
    geolocation = geolocation.select('*').where(col('latitude').isNotNull()).sort('year', 'crimetype')
    ## null checking for latitude only as both latutide/longitude has similar entries missing
    return geolocation


## Table for crime severity deduction
def get_crime_severity(data):
    crime_severity = data.select('id', 'crimetype', 'description', 'arrest', 'major_crime').where(
        col('major_crime') == 1)
    return crime_severity


## Table for ward vs arrest mapping
def get_ward_arrest(data):
    ward_arrest = data.select('id', 'ward', 'arrest').where(col('ward').isNotNull())
    ward_arrest = ward_arrest.groupBy('ward', 'arrest').agg(functions.count(ward_arrest['id']).alias('count')).sort(
        'ward', 'arrest')
    return ward_arrest


## Table for district vs arrest mapping
def get_district_arrest(data):
    district_arrest = data.select('id', 'district', 'arrest').where(col('district').isNotNull())
    district_arrest = district_arrest.groupBy('district', 'arrest').agg(
        functions.count(district_arrest['id']).alias('count')).sort('district', 'arrest')
    return district_arrest


## Table for communityarea vs arrest mapping
def get_communityarea_arrest(data):
    communityarea_arrest = data.select('id', 'communityarea', 'arrest').where(col('communityarea').isNotNull())
    communityarea_arrest = communityarea_arrest.groupBy('communityarea', 'arrest').agg(
        functions.count(communityarea_arrest['id']).alias('count')).sort('communityarea', 'arrest')
    return communityarea_arrest


"""*************************************************  CASSANDRA   OPERATIONS    ************************************************************************"""


## write to cassandra tables
def write_to_cassandra(df, table_name, KEYSPACE='pirates'):
    df.write.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=KEYSPACE).save()


# Write to csv function takes dataframe and string name as input
def write_to_csv_or_cassandra(df, table_name, format='csv'):
    if format == 'csv':
        df.coalesce(1).write.format('csv').mode('overwrite').save('./datafile/graph_dataset/' + table_name,
                                                                  header='true')

    if format == 'cassandra':
        write_to_cassandra(df, table_name)


## Creating cassandra session on localhost
def connect_to_cassandra(KEYSPACE='pirates'):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute(
        'CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {\'class\' : \'SimpleStrategy\', \'replication_factor\' : 1}' % KEYSPACE)
    session = cluster.connect(keyspace=KEYSPACE)
    return session


## Checking and creating master tables in cassandra
def create_master_tables(session, truncate='no', drop_table='no'):
    a = 1
    ## Drop master cassandra table
    if drop_table == 'yes':
        session.execute('DROP TABLE chicagocrime')

    ## Create main database from processed data
    session.execute('CREATE TABLE IF not EXISTS chicagocrime (id INT, casenumber TEXT, occurrence_date TIMESTAMP, arrest BOOLEAN, block TEXT, crimetype TEXT, \
         description TEXT, location_description TEXT, year INT, latitude FLOAT, longitude FLOAT, hour INT, am_pm TEXT, \
         location TEXT, month INT, district FLOAT, ward FLOAT, communityarea FLOAT, PRIMARY KEY (id, casenumber))')

    ## Truncate master table
    if truncate == 'yes':
        session.execute('TRUNCATE chicagocrime')


## Checking and creating required tables in cassandra
def create_cassandra_tables(session):
    ## Creating smaller databases for graph/plots
    session.execute('CREATE TABLE IF not EXISTS crimetype (crimetype TEXT, count INT,PRIMARY KEY (crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS location_crime (location_description TEXT, count INT,PRIMARY KEY (location_description))')

    session.execute(
        'CREATE TABLE IF not EXISTS hourly_crime (hour_info INT , am_pm TEXT, count INT, PRIMARY KEY (hour_info,am_pm))')

    session.execute(
        'CREATE TABLE IF not EXISTS hourly_major_crime (hour_info INT, am_pm TEXT, crimetype TEXT,count INT, PRIMARY KEY (hour_info, am_pm, crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS crimetype_yearly (year INT, crimetype TEXT, count INT, PRIMARY KEY (year, crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS crime_2010 (month_number INT, crimetype TEXT, count INT, PRIMARY KEY (month_number, crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS crime_2011 (month_number INT, crimetype TEXT, count INT, PRIMARY KEY (month_number, crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS crime_2012 (month_number INT, crimetype TEXT, count INT, PRIMARY KEY (month_number, crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS crime_2013 (month_number INT, crimetype TEXT, count INT, PRIMARY KEY (month_number, crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS crime_2014 (month_number INT, crimetype TEXT, count INT, PRIMARY KEY (month_number, crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS crime_2015 (month_number INT, crimetype TEXT, count INT, PRIMARY KEY (month_number, crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS crime_2016 (month_number INT, crimetype TEXT, count INT, PRIMARY KEY (month_number, crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS crime_2017 (month_number INT, crimetype TEXT, count INT, PRIMARY KEY (month_number, crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS crime_2018 (month_number INT, crimetype TEXT, count INT, PRIMARY KEY (month_number, crimetype))')

    session.execute(
        'CREATE TABLE IF not EXISTS geolocation (id INT, year INT, crimetype TEXT, latitude FLOAT, longitude FLOAT, major_crime INT,  PRIMARY KEY (id))')

    session.execute(
        'CREATE TABLE IF not EXISTS crime_severity (id INT, crimetype TEXT, description TEXT, arrest BOOLEAN, major_crime INT,  PRIMARY KEY (id))')

    session.execute(
        'CREATE TABLE IF not EXISTS ward_arrest ( ward FLOAT, arrest BOOLEAN, count INT, PRIMARY KEY (ward, arrest))')

    session.execute(
        'CREATE TABLE IF not EXISTS district_arrest ( district FLOAT, arrest BOOLEAN, count INT, PRIMARY KEY (district, arrest))')

    session.execute(
        'CREATE TABLE IF not EXISTS communityarea_arrest ( communityarea FLOAT, arrest BOOLEAN, count INT, PRIMARY KEY (communityarea, arrest))')


## Truncate cassandra tables
def truncate_cassandra_tables(table_name='none'):
    session = connect_to_cassandra()
    if table_name == 'none':
        pass
    elif table_name == 'all':
        session.execute('TRUNCATE crimetype')
        session.execute('TRUNCATE location_crime')
        session.execute('TRUNCATE hourly_crime')
        session.execute('TRUNCATE hourly_major_crime')
        session.execute('TRUNCATE crimetype_yearly')
        session.execute('TRUNCATE crime_2010')
        session.execute('TRUNCATE crime_2011')
        session.execute('TRUNCATE crime_2012')
        session.execute('TRUNCATE crime_2013')
        session.execute('TRUNCATE crime_2014')
        session.execute('TRUNCATE crime_2015')
        session.execute('TRUNCATE crime_2016')
        session.execute('TRUNCATE crime_2017')
        session.execute('TRUNCATE crime_2018')
        session.execute('TRUNCATE geolocation')
        session.execute('TRUNCATE crime_severity')
        session.execute('TRUNCATE ward_arrest')
        session.execute('TRUNCATE district_arrest')
        session.execute('TRUNCATE communityarea_arrest')

    else:
        session.execute('TRUNCATE ' + table_name)


## Drop cassandra tables
def drop_cassandra_tables(table_name='none'):
    session = connect_to_cassandra()
    if table_name == 'none':
        pass
    elif table_name == 'all':
        session.execute('DROP TABLE crimetype')
        session.execute('DROP TABLE location_crime')
        session.execute('DROP TABLE hourly_crime')
        session.execute('DROP TABLE hourly_major_crime')
        session.execute('DROP TABLE crimetype_yearly')
        session.execute('DROP TABLE crime_2010')
        session.execute('DROP TABLE crime_2011')
        session.execute('DROP TABLE crime_2012')
        session.execute('DROP TABLE crime_2013')
        session.execute('DROP TABLE crime_2014')
        session.execute('DROP TABLE crime_2015')
        session.execute('DROP TABLE crime_2016')
        session.execute('DROP TABLE crime_2017')
        session.execute('DROP TABLE crime_2018')
        session.execute('DROP TABLE geolocation')
        session.execute('DROP TABLE crime_severity')
        session.execute('DROP TABLE ward_arrest')
        session.execute('DROP TABLE district_arrest')
        session.execute('DROP TABLE communityarea_arrest')

    else:
        session.execute('DROP TABLE ' + table_name)


"""*********************************************  WRITE TO CASSANDRA  ******************************************************************************"""


## Run ETL on data and create master table of processed data in cassandra
def generate_master_tables(format='cassandra', truncate='no',
                           drop_table='no'):  # takes parameter format as csv or cassandra
    session = connect_to_cassandra()
    create_master_tables(session, truncate, drop_table)

    chicagocrime = chicago_etl_data()
    write_to_csv_or_cassandra(chicagocrime, 'chicagocrime', format)
    print('chicagocrime table created')


## Function call to create csv files or write to cassandra db
def generate_tables(format='csv'):  # takes parameter format as csv or cassandra

    data = get_processed_data(file_format='cassandra', KEYSPACE='pirates')

    ## Connecting to cassandra and check for tables in cassandra keyspace
    session = connect_to_cassandra()
    create_cassandra_tables(session)

    crimetype = get_crimetype(data)
    write_to_csv_or_cassandra(crimetype, 'crimetype', format)
    print('crimetype table created')

    crimetype_yearly = get_crimetype_yearly(data)
    write_to_csv_or_cassandra(crimetype_yearly, 'crimetype_yearly', format)
    print('crimetype_yearly table created')

    location_crime = get_location_crime(data)
    write_to_csv_or_cassandra(location_crime, 'location_crime', format)
    print('location_crime table created')

    hourly_crime = get_hourly_crime(data)
    write_to_csv_or_cassandra(hourly_crime, 'hourly_crime', format)
    print('hourly_crime table created')

    hourly_major_crime = get_hourly_major_crime(data)
    write_to_csv_or_cassandra(hourly_major_crime, 'hourly_major_crime', format)
    print('hourly_major_crime table created')

    crime_2010 = get_crime_2010(data)
    write_to_csv_or_cassandra(crime_2010, 'crime_2010', format)
    print('crime_2010 table created')

    crime_2011 = get_crime_2011(data)
    write_to_csv_or_cassandra(crime_2011, 'crime_2011', format)
    print('crime_2011 table created')

    crime_2012 = get_crime_2012(data)
    write_to_csv_or_cassandra(crime_2012, 'crime_2012', format)
    print('crime_2012 table created')

    crime_2013 = get_crime_2013(data)
    write_to_csv_or_cassandra(crime_2013, 'crime_2013', format)
    print('crime_2013 table created')

    crime_2014 = get_crime_2014(data)
    write_to_csv_or_cassandra(crime_2014, 'crime_2014', format)
    print('crime_2014 table created')

    crime_2015 = get_crime_2015(data)
    write_to_csv_or_cassandra(crime_2015, 'crime_2015', format)
    print('crime_2015 table created')

    crime_2016 = get_crime_2016(data)
    write_to_csv_or_cassandra(crime_2016, 'crime_2016', format)
    print('crime_2016 table created')

    crime_2017 = get_crime_2017(data)
    write_to_csv_or_cassandra(crime_2017, 'crime_2017', format)
    print('crime_2017 table created')

    crime_2018 = get_crime_2018(data)
    write_to_csv_or_cassandra(crime_2018, 'crime_2018', format)
    print('crime_2018 table created')

    geolocation = get_geolocation(data)
    write_to_csv_or_cassandra(geolocation, 'geolocation', format)
    print('geolocation table created')

    crime_severity = get_crime_severity(data)
    write_to_csv_or_cassandra(crime_severity, 'crime_severity', format)
    print('crime_severity table created')

    ward_arrest = get_ward_arrest(data)
    write_to_csv_or_cassandra(ward_arrest, 'ward_arrest', format)
    print('ward_arrest table created')

    district_arrest = get_district_arrest(data)
    write_to_csv_or_cassandra(district_arrest, 'district_arrest', format)
    print('district_arrest table created')

    communityarea_arrest = get_communityarea_arrest(data)
    write_to_csv_or_cassandra(communityarea_arrest, 'communityarea_arrest', format)
    print('communityarea_arrest table created')


"""*********************************************    FUNCTION     CALLS     *************************************************************************"""

""" ## UNCOMMENT TO RUN INDIVIDUALLY
## TRUNCATE cassandra tables, takes 'all', table name as parameter
truncate_cassandra_tables(table_name='none')

## DROP cassandra tables, takes 'all', table name as parameter
drop_cassandra_tables(table_name='none')

## Run ETL on RAW DATA and generate master table in cassandra
generate_master_tables(format='cassandra', truncate='no', drop_table='no')

## Run all data operations
generate_tables(format='cassandra')
"""

"""
FUNCTIONS BEING CALLED IN app.py

# Command to run the file individually (makes connection to cassandra database)
uncomment the commands above and run using (provided dependencies are met)

spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 data_operations/data_operations.py

"""