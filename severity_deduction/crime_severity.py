from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import desc, col, when, coalesce, lit
import matplotlib.pyplot as plt
import numpy as np

cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Crime Severity').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
sc = spark.sparkContext

"""
*** AVAILABLE CRIME TYPES ***
'theft', 'battery', 'criminal_damage', 'narcotics', 'other_offense', 'assault', 'burglary',
 'motor_vehicle_theft', 'deceptive_practice', 'robbery', 'criminal_trespass'

"""


def severity_deduction(KEYSPACE='pirates'):
    crime_severity = spark.read.format("org.apache.spark.sql.cassandra").options(table='crime_severity', keyspace=KEYSPACE).load()
    ## view different crime types
    crime = crime_severity.filter(crime_severity['crimetype'] == 'assault').groupBy('description')\
        .agg(functions.count(crime_severity['id']).alias('occurrence')).sort(desc('occurrence'))
    crime.show(200,False)

    crime_severity = crime_severity.withColumn("category", lit(None).cast(types.StringType()))

## Create severity categorization
    crime_severity = crime_severity.select(col("*"),when((col('crimetype') == 'deceptive_practice'),coalesce(col('category'), lit("moderate"))) \
        \
                                           .when((col('crimetype') == 'assault') & (col('description').rlike("aggravated")),coalesce(col('category'), lit("severe"))) \
                                           .when((col('crimetype') == 'assault') & (col('description').rlike("agg_pro")),coalesce(col('category'), lit("moderate"))) \
        \
                                           .when((col('crimetype') == 'battery') & (col('description').rlike("aggravated:|aggr")),coalesce(col('category'), lit("severe"))) \
        \
                                           .when((col('crimetype') == 'other_offense') & (col('description').rlike("harassment|violation|explosive|hazardous")),coalesce(col('category'), lit("severe")))\
        \
                                           .when((col('crimetype') == 'narcotics') & (col('description').rlike("manu|criminal")),coalesce(col('category'), lit("severe"))) \
                                           .when((col('crimetype') == 'narcotics') & (col('description').rlike("poss|sale|cannabis")),coalesce(col('category'), lit("moderate"))) \
        \
                                           .when((col('crimetype') == 'robbery') & (col('description').rlike("armed")),coalesce(col('category'), lit("severe"))) \
                                           .when((col('crimetype') == 'robbery') & (col('description').rlike("aggravated")),coalesce(col('category'), lit("moderate"))) \
        \
                                           .otherwise(coalesce(col('category'), lit("moderate"))).alias("severity")).drop(col('category'))


    crime_severity = crime_severity.select("*").where((col('crimetype') == 'assault') | (col('crimetype') == 'battery') \
                                                      | (col('crimetype') == 'narcotics')| (col('crimetype') == 'other_offense') \
                                                       | (col('crimetype') == 'robbery'))
    crime_severity = crime_severity.select("*").groupBy('crimetype', 'severity').agg(functions.count(crime_severity['id']).alias('occurrence')).sort('crimetype', 'severity' ,desc('occurrence'))

    severe_count = list(crime_severity.filter(col('severity') == 'severe').select(col('occurrence')).toPandas()['occurrence'])
    moderate_count = list(crime_severity.filter(col('severity') == 'moderate').select(col('occurrence')).toPandas()['occurrence'])
    x = np.arange(5)
    crimes = list(crime_severity.select('crimetype').distinct().sort(col('crimetype')).toPandas()['crimetype'])
    width = 0.4
    bar1 = plt.bar(x - width / 2, severe_count, width - 0.1, color = 'lightcoral', label='Severe')
    bar2 = plt.bar(x + width / 2, moderate_count, width - 0.1, color = 'lightseagreen',label='Moderate')

    # Plot of severity of crime across years
    plt.xlabel('Crime Type')
    plt.ylabel('Count')
    plt.title('Categorization of crime severity')
    plt.legend(['Severe','Moderate'])
    plt.xticks(x, crimes, fontsize=6, rotation=10)
    plt.savefig('./static/images/crime_severity.png')
    crime_severity.show(50, False)
    return crime_severity



#severity_deduction()
"""
Function being called in app.py

# Command to run the file individually (makes connection to cassandra database)
uncomment the command above and run using (provided dependencies are met)

spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 severity_deduction/crime_severity.py

"""