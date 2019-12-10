import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import desc, col, when, coalesce, lit
import numpy as np

cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Crime Severity').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def ward_analysis(KEYSPACE ='pirates'):

    df = spark.read.format("org.apache.spark.sql.cassandra").options(table='ward_arrest', keyspace=KEYSPACE).load()
    arrest_success = df.filter(col('arrest') == True).sort('ward')
    arrest_true_count = list(arrest_success.select('count').toPandas()['count'])
    data = df.groupby('ward').agg(functions.sum(df['count']).alias('count')).sort('ward')
    data = data.withColumn("ward1", data["ward"].cast(types.IntegerType())).drop(data["ward"])
    data = data.select(data['ward1'].alias('ward'),data['count'])
    wards = list(data.select('ward').toPandas()['ward'])
    total_count = list(data.select('count').toPandas()['count'])
    ind = np.arange(len(wards))
    width = 0.35
    p1 = plt.bar(ind, arrest_true_count, width)
    p2 = plt.bar(ind, total_count, width,bottom=arrest_true_count)
    #plt.figure(figsize=(14,12))
    plt.ylabel('Count')
    plt.title('Arrest vs Total Crime - Ward Data')
    plt.xticks(ind, wards, rotation = 'vertical')
    plt.legend(['Arrests', 'Total Crime'])
    plt.savefig('./static/images/ward_static.png')


#ward_analysis()
"""
Function being called in app.py

# Command to run the file individually (makes connection to cassandra database)
uncomment the command above and run using (provided dependencies are met)

spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 area_wise_analysis/ward_level_analysis.py

"""