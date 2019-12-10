import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import desc, col, when, coalesce, lit
import numpy as np

cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Crime Severity').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def communityarea_analysis(KEYSPACE = 'pirates'):
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table='communityarea_arrest', keyspace=KEYSPACE).load()
    df = df.select('*').where(df['communityarea']<=40)
    arrest_success = df.filter(col('arrest') == True).sort('communityarea')
    arrest_true_count = list(arrest_success.select('count').toPandas()['count'])
    data = df.groupby('communityarea').agg(functions.sum(df['count']).alias('count')).sort('communityarea')
    data = data.withColumn("community1", data["communityarea"].cast(types.IntegerType())).drop(data["communityarea"])
    data = data.select(data['community1'].alias('communityarea'),data['count'])
    communityareas= list(data.select('communityarea').toPandas()['communityarea'])
    total_count = list(data.select('count').toPandas()['count'])
    ind = np.arange(len(communityareas))
    width = 0.35
    p1 = plt.bar(ind, arrest_true_count, width)
    p2 = plt.bar(ind, total_count, width,bottom=arrest_true_count)

    plt.ylabel('Count')
    plt.title('Arrest vs Total Crime - Community Area Data')
    plt.xticks(ind, communityareas, rotation = 'vertical')
    plt.legend(['Arrests', 'Total Crime'])
    plt.savefig('./static/images/communityarea_static.png')


#communityarea_analysis()
"""
Function being called in app.py

# Command to run the file individually (makes connection to cassandra database)
uncomment the command above and run using (provided dependencies are met)

spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 area_wise_analysis/communityarea_level_analysis.py

"""