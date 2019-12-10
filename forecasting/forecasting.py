import pandas as pd
import seaborn as sns
from fbprophet import Prophet
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Forecasting Crime').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
sc = spark.sparkContext

## Using FB Prophet model to forecast crime data for 2019
def crime_forecasting(KEYSPACE = 'pirates'):

    pan = spark.read.format("org.apache.spark.sql.cassandra").options(table='chicagocrime', keyspace=KEYSPACE).load()
    pan = pan.toPandas()

    ## For some OS gives JAVA heap error, then use the following command to load data
    #pan = pd.read_csv('/Users/darklord/PycharmProjects/BigData/datafile/mycsv/part-00000-8101e9fe-075a-4eed-b16b-2ca079001c1b-c000.csv', error_bad_lines=False)


    plt.figure(figsize=(15, 10))
    svm = sns.countplot(y='crimetype', data=pan, order=pan['crimetype'].value_counts().iloc[:15].index)
    fig = svm.get_figure()
    fig.savefig("/Users/darklord/PycharmProjects/BigData/static/images/forecasting/theft_count.png")

    plt.figure(figsize=(15, 10))
    svm = sns.countplot(y='location_description', data=pan, order=pan['location_description'].value_counts().iloc[:15].index)
    fig = svm.get_figure()
    fig.savefig("/Users/darklord/PycharmProjects/BigData/static/images/forecasting/location_description_count.png")

    pan.index = pd.DatetimeIndex(pan.occurrence_date)
    pan1 = pan.resample('M').size().reset_index()

    pan1.columns = ['Date', 'Crime Count']
    chicago_prophet_df = pd.DataFrame(pan1)
    chicago_prophet_df_final = chicago_prophet_df.rename(columns={'Date':'ds', 'Crime Count':'y'})

    m = Prophet()
    m.fit(chicago_prophet_df_final)

    future = m.make_future_dataframe(periods=365)
    future.tail()

    forecast = m.predict(future)

    m.plot(forecast, xlabel='Date', ylabel='Crime Rate').savefig("/Users/darklord/PycharmProjects/BigData/static/images/forecasting/forecast.png")
    m.plot_components(forecast).savefig("/Users/darklord/PycharmProjects/BigData/static/images/forecasting/trends.png")


#crime_forecasting()
"""
Function being called in app.py

# Command to run the file individually (makes connection to cassandra database)
uncomment the command above and run using (provided dependencies are met)

spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 forecasting/forecasting.py

"""