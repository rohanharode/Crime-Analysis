from pyspark.sql import types

crime_schema = types.StructType([
    types.StructField('id', types.IntegerType()),
    types.StructField('casenumber', types.StringType()),
    types.StructField('date1', types.StringType()),
    types.StructField('block', types.StringType()),
    types.StructField('iucr', types.IntegerType()),
    types.StructField('crimetype', types.StringType()),
    types.StructField('description', types.StringType()),
    types.StructField('location_description', types.StringType()),
    types.StructField('arrest', types.BooleanType()),
    types.StructField('domestic', types.BooleanType()),
    types.StructField('beat', types.IntegerType()),
    types.StructField('district', types.FloatType()),
    types.StructField('ward', types.FloatType()),
    types.StructField('communityarea', types.FloatType()),
    types.StructField('fbicode', types.StringType()),
    types.StructField('x_coordinate', types.FloatType()),
    types.StructField('y_coordinate', types.FloatType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('updatedon', types.StringType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('location', types.StringType()),

])