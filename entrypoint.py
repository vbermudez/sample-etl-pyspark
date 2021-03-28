import os

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import LongType, ShortType, StringType, StructField, StructType
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

def get_session():
    return SparkSession.builder.appName('Sample ETL').getOrCreate()

def build_schema():
    return StructType([
        StructField('Invoice ID', StringType(), False),
        StructField('City', StringType(), False),
        StructField('Gender', StringType(), False),
        StructField('Unit price', LongType(), False),
        StructField('Quantity', ShortType(), False),
    ])

def extract_from_csv(csv_path, session):
    data = session.read \
        .csv(csv_path, header = True, sep = ',') \
        .schema(build_schema()) \
        .cache()
    data.registerTemplate('sales')
    return data

def transform_csv(query, session):
    return session.sql(query)

def load_csv(data, path):
    data.write.format('json').save(path)

def extract_from_stream(host, port, session):
    return session.readStream \
        .format('socket') \
        .option('host', host) \
        .option('port', port) \
        .load()

def transform_stream(data):
    words = data.select(
        explode(
            split(data.value, ' ')
        )
    ).alias('word')
    return words.groupBy('word').count()

def load_stream(data):
    query = data.writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

    query.awaitTermination()

def csv_demo(session):
    csv_path = os.path.join(os.path.abspath(os.getcwd()), 'sample_data', 'supermarket_sales - Sheet1.csv')
    data = extract_from_csv(csv_path, session)
    transformed = transform_csv('select count(*) as Total, City from sales group by City', session)
    
    print('Transformed')
    print(transformed.show())

    json_path = os.path.join(os.path.abspath(os.getcwd()), 'sample_data', 'transformed.json')
    load_csv(transformed, json_path)

def stream_demo(session):
    data = extract_from_stream('localhost', 9999, session)
    transformed = transform_stream(data)
    load_stream(transformed)

if __name__ == '__main__':
    session = get_session()
    csv_demo(session)
    stream_demo(session)

