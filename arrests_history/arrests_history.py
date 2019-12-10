import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Arrest History').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

KEYSPACE ='pirates'

def arrest_history():
    arrest_data = spark.read.format("org.apache.spark.sql.cassandra").options(table='chicagocrime', keyspace=KEYSPACE).load()
    arrest_data = arrest_data.select('arrest','year').toPandas()
    arrest_count = arrest_data['arrest'].value_counts()

    arrest_percent = (arrest_count / arrest_data['arrest'].sum()) * 100

    # Graph label
    arrest_percent.rename("Percentage of Arrests", fontsize=15, inplace=True)

    # True and False to % Arrested and % Not Arrested
    arrest_percent.rename({True: '% Arrested', False: '% Not Arrested'},inplace=True)

    # auto percentage function
    def make_autopct(values):
        def my_autopct(pct):
            total = sum(values)
            val = int(round(pct*total/100.0))
            return '{p:.1f}%  ({v:d})'.format(p=pct, v=val)
        return my_autopct

    #Pie chart for total arrests in Chicago

    colors = ['darksalmon', 'mediumaquamarine']
    explode = (0.1,0)
    arrest_percent.plot.pie(fontsize=15, autopct=make_autopct(arrest_count), explode=explode, colors=colors, figsize=(7, 7), startangle=90)
    plt.title('Chicago\'s Successful arrests', fontsize=15)
    plt.savefig("./static/images/percentageofarrests.png")


    # Line chart for yearwise arrests in Chicago
    arrest_per_year = arrest_data.groupby('year')['arrest'].value_counts().rename('counts').to_frame()
    arrest_per_year['percentage'] = (100 * arrest_per_year / arrest_per_year.groupby(level=0).sum())
    arrest_per_year.reset_index(level=[1],inplace=True)
    line_plot = arrest_per_year[arrest_per_year['arrest'] == True]['percentage']

    labels = line_plot.index.values
    fig=plt.figure(figsize=(12, 10))
    plt.title('Percentages of successful arrests from 2001 to 2018', fontsize=15)
    plt.xlabel("Years", fontsize=15)
    plt.ylabel("Successful Arrest Percentage", fontsize=15)
    plt.xticks(line_plot.index, line_plot.index.values, fontsize=10)

    line_plot.plot(grid=True, marker='o', color='dodgerblue')
    plt.savefig("../static/images/successfularrests.png")


#arrest_history()
"""
Function being called in app.py

# Command to run the file individually (makes connection to cassandra database)
uncomment the command above and run using (provided dependencies are met)

spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 arrests_history/arrests_history.py

"""