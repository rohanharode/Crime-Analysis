import dash
from flask import Flask
import dash_core_components as dcc
import dash_html_components as html
from pyspark.sql.functions import col
from dash.dependencies import Input, Output
from pyspark.sql import SparkSession, functions
from forecasting.forecasting import crime_forecasting
from arrests_history.arrests_history import arrest_history
from area_wise_analysis.ward_level_analysis import ward_analysis
from severity_deduction.crime_severity import severity_deduction
from chicago_wordcloud.chicago_wordcloud import generate_wordcloud
from area_wise_analysis.district_level_analysis import district_analysis
from area_wise_analysis.communityarea_level_analysis import communityarea_analysis
from data_operations.data_operations import get_cassandra_table, generate_master_tables, generate_tables

"""
# Command to run the file (makes connection to cassandra database)

spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 app.py

"""

app = Flask(__name__)
cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Chicago Crime Analysis').config('spark.cassandra.connection.host',\
                                                                      ','.join(cluster_seeds)).getOrCreate()
sc = spark.sparkContext

# Our cassandra keyspace
KEYSPACE = 'pirates'

"""
## Run ETL on RAW DATA and generate master table in cassandra
generate_master_tables(format='cassandra', truncate='no', drop_table='no')

## Run all data operations and generate child tables
generate_tables(format='cassandra')

## Generate Word Cloud
generate_wordcloud()

## Generate severity deduction
severity_deduction(KEYSPACE='pirates')

## Generate Arrest history
arrest_history()

#Generate Area-wise Arrest Analysis
ward_analysis()
district_analysis()
communityarea_analysis()

#Crime Forecasting
crime_forecasting()
"""


## Get data from cassandra
hourly_major_crime = get_cassandra_table(table_name='hourly_major_crime', KEYSPACE='pirates')
crime_2010 = get_cassandra_table(table_name='crime_2010', KEYSPACE='pirates')
crime_2011 = get_cassandra_table(table_name='crime_2011', KEYSPACE='pirates')
crime_2012 = get_cassandra_table(table_name='crime_2012', KEYSPACE='pirates')
crime_2013 = get_cassandra_table(table_name='crime_2013', KEYSPACE='pirates')
crime_2014 = get_cassandra_table(table_name='crime_2014', KEYSPACE='pirates')
crime_2015 = get_cassandra_table(table_name='crime_2015', KEYSPACE='pirates')
crime_2016 = get_cassandra_table(table_name='crime_2016', KEYSPACE='pirates')
crime_2017 = get_cassandra_table(table_name='crime_2017', KEYSPACE='pirates')
crime_2018 = get_cassandra_table(table_name='crime_2018', KEYSPACE='pirates')
geo_crime = get_cassandra_table(table_name='geolocation', KEYSPACE='pirates')

# Crime count month wise for different years
count_all_2010 = crime_2010.groupBy('month_number').agg(functions.sum('count').alias('cnt')).sort('month_number')
count_all_2010_list = [int(row.cnt) for row in count_all_2010.collect()]

count_all_2011 = crime_2011.groupBy('month_number').agg(functions.sum('count').alias('cnt')).sort('month_number')
count_all_2011_list = [int(row.cnt) for row in count_all_2011.collect()]

count_all_2012 = crime_2012.groupBy('month_number').agg(functions.sum('count').alias('cnt')).sort('month_number')
count_all_2012_list = [int(row.cnt) for row in count_all_2012.collect()]

count_all_2013 = crime_2013.groupBy('month_number').agg(functions.sum('count').alias('cnt')).sort('month_number')
count_all_2013_list = [int(row.cnt) for row in count_all_2013.collect()]

count_all_2014 = crime_2014.groupBy('month_number').agg(functions.sum('count').alias('cnt')).sort('month_number')
count_all_2014_list = [int(row.cnt) for row in count_all_2014.collect()]

count_all_2015 = crime_2015.groupBy('month_number').agg(functions.sum('count').alias('cnt')).sort('month_number')
count_all_2015_list = [int(row.cnt) for row in count_all_2015.collect()]

count_all_2016 = crime_2016.groupBy('month_number').agg(functions.sum('count').alias('cnt')).sort('month_number')
count_all_2016_list = [int(row.cnt) for row in count_all_2016.collect()]

count_all_2017 = crime_2017.groupBy('month_number').agg(functions.sum('count').alias('cnt')).sort('month_number')
count_all_2017_list = [int(row.cnt) for row in count_all_2017.collect()]

count_all_2018 = crime_2018.groupBy('month_number').agg(functions.sum('count').alias('cnt')).sort('month_number')
count_all_2018_list = [int(row.cnt) for row in count_all_2018.collect()]

# Creating essential lists
am_pm = list(hourly_major_crime.select('am_pm').distinct().toPandas()['am_pm'])
crimes = list(hourly_major_crime.select('crimetype').distinct().toPandas()['crimetype'])
month_list = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
year_list = list(geo_crime.select('year').distinct().toPandas()['year'])

# geo crime data for scatter map box/geo crime density
geo_df = geo_crime.select('latitude', 'longitude', 'crimetype', 'year').cache()

# Integrating Flask on top of Dash server
dashapp = dash.Dash(__name__, server= app, url_base_pathname='/')

#Switching to multiple tabs. Allowed callbacks outside of Main Layout
dashapp.config['suppress_callback_exceptions'] = True

#Tab styling
tabs_styles = {
    'height': '44px'
}
tab_style = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '6px',
    'fontWeight': 'bold'
}

tab_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#119DFF',
    'color': 'white',
    'padding': '6px'
}

#Webapp layout

with app.app_context():
    dashapp.layout = html.Div(
    children=[
        html.Div([
            # --header section
            html.Div([
                html.H2('Chicago Crime Analysis'),
            ], style={'text-align': 'center', 'width': '100%', 'display': 'inline-block', 'vertical-align': 'middle','font-family':'Helvetica'}),
            dcc.Tabs(id="tabs-example", value='interactive', children=[
                dcc.Tab(label='Interactive Visualizations', value='interactive', style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label='WordCloud', value='wordcloud', style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label='Forecasting', value='forecasting', style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label="Static Visualizations", value="static", style=tab_style, selected_style=tab_selected_style)
            ], style=tabs_styles),
            html.Div(id='tabs-content-example')
            ],style={'width': '100%','background-position': 'initial initial', 'background-repeat': 'initial initial'},
        )
    ], style={'background-image': 'url("./static/images/chicago.jpg")'})

#Render pageview according to tabs
@dashapp.callback(Output('tabs-content-example', 'children'),
              [Input('tabs-example', 'value')])
def render_content(tab):
    if tab == 'interactive':
        return html.Div([
        html.Div([
        html.Div([
            html.P(
                'Crime Type',
                className="control_label",
                style={
                'textAlign': 'center'
                }
            ),
            dcc.Dropdown(
                id='crimetype-column',
                options=[{'label': i, 'value': i} for i in crimes],
                value='robbery'
            ),
        ],
        style={'width': '25%', 'display': 'inline-block'}),

        html.Div([
            html.P(
                'Day Timing',
                className="control_label",
                style={
                    'textAlign': 'center'
                }
            ),
            dcc.Dropdown(
                id='day-night-column',
                options=[{'label': i, 'value': i} for i in am_pm],
                value='AM'
            ),
        ], style={'width': '25%', 'display': 'inline-block'}),



        html.Div([
            dcc.Checklist(id="list_all_crime",
                options=[{'label': 'All Crime', 'value': 'all'}]),
            ], style={'width': '20%', 'float':'right','padding': '60px 50px 0px 30px'}),
         ],
        style={
        'borderBottom': 'thin lightgrey solid',
        'backgroundColor': 'rgb(250, 250, 250)',
        'padding': '10px 5px'
        }),

        html.Div([
            html.Div([
                dcc.Graph(id='x-time-series')
                    ], style={'display': 'inline-block', 'width': '47%', 'margin-left': '2.5%'}, className='six columns'),
            html.Div([
                dcc.Graph(id='x-month-series')
                     ], style={'display': 'inline-block', 'width': '47%', 'margin-left': '1.2%'}, className='six columns'),
                 ], className='row',style={'padding': '10px 5px'}),

            html.Div([
                html.P(
                    'Drag the slider to change the year',
                    className="control_label",
                    style={
                        'textAlign': 'center'
                    }
                ),
                dcc.Slider(
                    id='year-slider',
                    min=min(year_list),
                    max=max(year_list),
                    value=max(year_list),
                    marks={str(year): str(year) for year in year_list},
                    step=None
                ),
            ], style={'width': '50%', 'margin-left': '25%', 'padding': '0px 20px 20px 20px',
                      'borderBottom': 'thin lightgrey solid',
                      'backgroundColor': 'rgb(250, 250, 250)'}),

            html.Div([
                dcc.Graph(id='my-graph')
            ], style={'display': 'block', 'width': '70%', 'margin-left': '10%', 'padding': '30px 20px 20px 20px'}),

        ], style={'margin-top': '10px'})

    elif tab == 'wordcloud':
        return html.Div([
            html.Div([
                html.P(
                    'WordCloud based on Crime Type',
                    className="control_label",
                    style={'textAlign': 'center','width': '50%', 'margin-left': '25%', 'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                    'backgroundColor': 'rgb(250, 250, 250)','font-family':'Helvetica','font-size':'18px'}
                ),
                html.Img(src="./static/images/wc_crimetype.png",style={'width':'98%','height':'800px'})
            ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

            html.Div([
                html.P(
                    'WordCloud based on Crime Location ',
                    className="control_label",
                    style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                           'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                           'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                ),
                html.Img(src="./static/images/wc_location_description.png",style={'width':'98%','height':'800px'})
                ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
            ], className='row',
                style={
                    'padding': '10px 5px',
                    'margin-left':'3%',
                    'margin-top': '10px'
             })
    elif tab == 'forecasting':
        return html.Div([
            html.Div([
                html.P(
                    'Crime forecast for 2019',
                    className="control_label",
                    style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                           'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                           'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                ),
                html.Img(src="./static/images/forecasting/forecast.png", style={'width': '98%', 'height': '500px'})
            ], style={'display': 'inline-block', 'width': '60%', 'margin-left': '20%'}),

            html.Div([
                html.P(
                    'Crime Rate Trend over years',
                    className="control_label",
                    style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                           'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                           'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                ),
                html.Img(src="./static/images/forecasting/trends.png", style={'width': '98%', 'height': '500px'})
            ], style={'display': 'inline-block', 'width': '60%', 'margin-left': '20%'}),
        ], style={
            'padding': '10px 5px',
            'margin-left': '3%',
            'margin-top': '10px'
        }),
    elif tab == 'static':
        return html.Div([
            html.Div([
            html.Div([
                html.Img(src="./static/images/crime_severity.png",style={'width':'60%','height':'500px'})
            ], style={'display': 'inline-block', 'width': '100%','margin-left':'20%'}),
            ]),

            html.Div([
                html.Div([
                    html.Img(src="./static/images/percentageofarrests.png", style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                html.Div([
                    html.Img(src="./static/images/successfularrests.png", style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
            ], className='row',
                style={
                    'padding': '10px 5px',
                    'margin-left': '3%'
                }),

            html.Div([
                html.Img(src="./static/images/district_static.png", style={'width': '60%', 'height': '600px'})
            ], style={'display': 'inline-block', 'width': '100%', 'margin-left': '20%'}),

            html.Div([
                html.Img(src="./static/images/ward_static.png",
                         style={'width': '60%', 'height': '600px'})
            ], style={'display': 'inline-block', 'width': '100%', 'margin-left': '20%'}),

            html.Div([
                html.Img(src="./static/images/communityarea_static.png",
                         style={'width': '60%', 'height': '600px'})
            ], style={'display': 'inline-block', 'width': '100%', 'margin-left': '20%'}),

            html.Div([
                html.Div([
                    html.P(
                        'Crime Location vs Count',
                        className="control_label",
                        style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                               'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                               'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                    ),
                    html.Img(src="./static/images/location_description_count.png",
                             style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                html.Div([
                    html.P(
                        'Crime Type vs Count',
                        className="control_label",
                        style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                               'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                               'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                    ),
                    html.Img(src="./static/images/theft_count.png", style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
            ], className='row',
                style={
                    'padding': '10px 5px',
                    'margin-left': '3%'
                }),
        ], style={'margin-top': '10px'})


def create_time_series(dff, title):
    return {
        'data': [dict(
            x=list(dff.select('hour_info').toPandas()['hour_info']),
            y=list(dff.select('count').toPandas()['count']),
            mode='lines+markers'
        )],
        'layout': {
            'height': 350,
            'margin': {'l': 40, 'b': 30, 'r': 10, 't': 40},
            'annotations': [{
                'x': 0, 'y': 0.85, 'xanchor': 'left', 'yanchor': 'bottom',
                'xref': 'paper', 'yref': 'paper', 'showarrow': False,
                'align': 'center', 'bgcolor': 'rgba(255, 255, 255, 0.5)',
                'text': title
            }],
            'title' : "Hourly intensity of " + title.capitalize(),
            'xaxis': {
                'title': "Time",
            },
            'yaxis': {
                'title': "Count",
            },
    }
    }

#Return hourly count of crime
@dashapp.callback(
    Output('x-time-series', 'figure'),
    [Input('crimetype-column', 'value'),
     Input('day-night-column', 'value'),
     ])
def update_crime_day_night_timeseries(xaxis_column_name, yaxis_column_name):
    dff = hourly_major_crime.filter(hourly_major_crime['crimetype'] == xaxis_column_name).filter(hourly_major_crime['am_pm'] == yaxis_column_name).sort('hour_info')
    title = '{}'.format(xaxis_column_name)
    return create_time_series(dff,title)


#Return coordinates for crime in various years
@dashapp.callback(
    Output('my-graph', 'figure'),
    [Input('crimetype-column', 'value'),
     Input('year-slider', 'value')])
def update_geo_graph(crimetype,year):

    dff = geo_df.filter(geo_df['year'] == year).filter(geo_df['crimetype'] == crimetype)
    return {
        'data': [{
            'lat': list(dff.select('latitude').toPandas()['latitude']),
            'lon': list(dff.select('longitude').toPandas()['longitude']),
            'type': 'scattermapbox',
        }],
        'layout': {'mapbox': {'accesstoken': 'pk.eyJ1IjoiZGFya2xvcmQwNyIsImEiOiJjazNlODhjN2cwMG9mM2ltOGVjMDJhbjRoIn0.dSTEqXzCSY69cWjdklL-HQ','bearing':0,
                                'pitch':0,'zoom':9,'center':{'lat':41.88,'lon':-87.67}},'width':1000,'height':600,'title' : "Crime Density of " + crimetype.capitalize() +" in " + str(year)}
    }

#Plotting month time series
def create_month_series(dff,title,year):
    return {
        'data': [dict(
            x=month_list,
            y=dff,
            mode='lines+markers'
        )],
        'layout': {
            'height': 350,
            'margin': {'l': 40, 'b': 30, 'r': 10, 't': 40},
            'annotations': [{
                'x': 0, 'y': 0.85, 'xanchor': 'left', 'yanchor': 'bottom',
                'xref': 'paper', 'yref': 'paper', 'showarrow': False,
                'align': 'left', 'bgcolor': 'rgba(255, 255, 255, 0.5)',
                'text': title
            }],
            'title': "Analysis of " + title.capitalize() + " in " + str(year),
            'xaxis':{
                 'title': "Month",
            },
            'yaxis' : {
                'title': "Count",
            },
        }
    }

# Return monthly count of different crime types
@dashapp.callback(
    Output('x-month-series', 'figure'),
    [Input('crimetype-column', 'value'),
     Input('year-slider', 'value'),
     Input('list_all_crime', 'value')
     ])
def update_month_timeseries(xaxis_column_name, yaxis_column_name, all_crime_checkbox):
    if yaxis_column_name == 2010:
        if all_crime_checkbox:
            crime_theft_list = count_all_2010_list
        else:
            crime_theft = crime_2010.select('month_number', 'crimetype', col('count').alias('cnt')).filter(crime_2010['crimetype'] == xaxis_column_name).sort('month_number')
            crime_theft_list = [int(row.cnt) for row in crime_theft.collect()]
    elif yaxis_column_name == 2011:
        if all_crime_checkbox:
            crime_theft_list = count_all_2011_list
        else:
            crime_theft = crime_2011.select('month_number', 'crimetype', col('count').alias('cnt')).filter(crime_2011['crimetype'] == xaxis_column_name).sort('month_number')
            crime_theft_list = [int(row.cnt) for row in crime_theft.collect()]
    elif yaxis_column_name == 2012:
        if all_crime_checkbox:
            crime_theft_list = count_all_2012_list
        else:
            crime_theft = crime_2012.select('month_number', 'crimetype', col('count').alias('cnt')).filter(crime_2012['crimetype'] == xaxis_column_name).sort('month_number')
            crime_theft_list = [int(row.cnt) for row in crime_theft.collect()]
    elif yaxis_column_name == 2013:
        if all_crime_checkbox:
            crime_theft_list = count_all_2013_list
        else:
            crime_theft = crime_2013.select('month_number', 'crimetype', col('count').alias('cnt')).filter(crime_2013['crimetype'] == xaxis_column_name).sort('month_number')
            crime_theft_list = [int(row.cnt) for row in crime_theft.collect()]
    elif yaxis_column_name == 2014:
        if all_crime_checkbox:
            crime_theft_list = count_all_2014_list
        else:
            crime_theft = crime_2014.select('month_number', 'crimetype', col('count').alias('cnt')).filter(crime_2014['crimetype'] == xaxis_column_name).sort('month_number')
            crime_theft_list = [int(row.cnt) for row in crime_theft.collect()]
    elif yaxis_column_name == 2015:
        if all_crime_checkbox:
            crime_theft_list = count_all_2015_list
        else:
            crime_theft = crime_2015.select('month_number', 'crimetype', col('count').alias('cnt')).filter(crime_2015['crimetype'] == xaxis_column_name).sort('month_number')
            crime_theft_list = [int(row.cnt) for row in crime_theft.collect()]
    elif yaxis_column_name == 2016:
        if all_crime_checkbox:
            crime_theft_list = count_all_2016_list
        else:
            crime_theft = crime_2016.select('month_number', 'crimetype', col('count').alias('cnt')).filter(crime_2016['crimetype'] == xaxis_column_name).sort('month_number')
            crime_theft_list = [int(row.cnt) for row in crime_theft.collect()]
    elif yaxis_column_name == 2017:
        if all_crime_checkbox:
            crime_theft_list = count_all_2017_list
        else:
            crime_theft = crime_2017.select('month_number', 'crimetype', col('count').alias('cnt')).filter(crime_2017['crimetype'] == xaxis_column_name).sort('month_number')
            crime_theft_list = [int(row.cnt) for row in crime_theft.collect()]
    else:
        if all_crime_checkbox:
            crime_theft_list = count_all_2018_list
        else:
            crime_theft = crime_2018.select('month_number', 'crimetype', col('count').alias('cnt')).filter(crime_2018['crimetype'] == xaxis_column_name).sort('month_number')
            crime_theft_list = [int(row.cnt) for row in crime_theft.collect()]

    if all_crime_checkbox:
        title = '{}'.format('all crimes')
    else:
        title = '{}'.format(xaxis_column_name)
    return create_month_series(crime_theft_list, title,yaxis_column_name)


#Run Flask server
if __name__ == '__main__':
    app.run()