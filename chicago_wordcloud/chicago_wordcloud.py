from PIL import Image
import numpy as np
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col


cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Word Cloud').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


table_name = ['crimetype', 'location_crime']
column_names = ['crimetype', 'location_description']

def generate_wordcloud(keyspace='pirates'):
    for i in range(0, len(column_names)):
        # Read the whole data to form wordcount
        if table_name[i]=='crimetype':
            data = spark.read.format("org.apache.spark.sql.cassandra")\
                .options(table= table_name[i], keyspace=keyspace).load().sort(desc('count'))
        else:
            data = spark.read.format("org.apache.spark.sql.cassandra") \
                .options(table=table_name[i], keyspace=keyspace).load().where(col('count') >= 1000).sort(desc('count'))

        dict_key = list(data.select(column_names[i]).toPandas()[column_names[i]])
        value = list(data.select('count').toPandas()['count'])
        crime_dict = dict(zip(dict_key, value))

        image_mask = np.array(Image.open("wordcloud/chicago_contour.jpg"))

        # generate word cloud
        if table_name[i] == 'crimetype':
            wc = WordCloud(background_color="white", max_words=2000, mask=image_mask, contour_width=3, contour_color='steelblue', max_font_size=150)
        else:
            wc = WordCloud(background_color="white", max_words=2000, mask=image_mask, contour_width=3, contour_color='steelblue', max_font_size=200)

        wc.generate_from_frequencies(crime_dict)

        # show
        plt.imshow(wc, interpolation='bilinear')
        plt.axis("off")
        plt.figure()
        # store to file
        wc.to_file("./static/images/wc_"+column_names[i]+".png")



#generate_wordcloud()
"""
Function being called in app.py

# Command to run the file individually (makes connection to cassandra database)
uncomment the command above and run using (provided dependencies are met)

spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 wordcloud/chicago_wordcloud.py

"""