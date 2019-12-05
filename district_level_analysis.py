import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import desc, col, when, coalesce, lit
import numpy as np
from pyspark.sql.types import IntegerType

cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Crime Severity').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

KEYSPACE ='pirates'

df = spark.read.format("org.apache.spark.sql.cassandra").options(table='district_arrest', keyspace=KEYSPACE).load()
arrest_success = df.filter(col('arrest') == True).sort('district')
arrest_true_count = list(arrest_success.select('count').toPandas()['count'])
data = df.groupby('district').agg(functions.sum(df['count']).alias('count')).sort('district')
data = data.withColumn("district1", data["district"].cast(IntegerType())).drop(data["district"])
data = data.select(data['district1'].alias('district'),data['count'])
districts = list(data.select('district').toPandas()['district'])
total_count = list(data.select('count').toPandas()['count'])
ind = np.arange(len(districts))
width = 0.35
p1 = plt.bar(ind, arrest_true_count, width)
p2 = plt.bar(ind, total_count, width,bottom=arrest_true_count)

plt.ylabel('Count')
plt.title('Arrest vs Total Crime - Ward Data')
plt.xticks(ind, districts, rotation = 'vertical')
plt.legend(['Arrests', 'Total Crime'])
plt.savefig('./static/images/district_static.png')


