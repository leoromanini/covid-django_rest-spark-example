import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.utils import AnalysisException


def get_json_covid_our_data():
    """
    Function that access the data source endpoint
    """
    url = 'https://covid.ourworldindata.org/data/owid-covid-data.json'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def process_json_data(dict_our_data):
    """
    Function that process data with Apache Spark
    """
    if dict_our_data:
        spark = SparkSession.builder.appName('covid') \
            .config('spark.driver.memory', '10G').getOrCreate()

        sc = spark.sparkContext
        rdd = sc.parallelize([dict_our_data])
        df = spark.read.option('multiLine', 'true').json(rdd)

        schema = StructType([StructField('location', StringType(), True),
                             StructField('date', StringType(), True),
                             StructField('total_cases', DoubleType(), True),
                             StructField('new_cases', DoubleType(), True),
                             StructField('new_cases_smoothed', DoubleType(), True),
                             StructField('total_deaths', DoubleType(), True),
                             StructField('new_deaths', DoubleType(), True),
                             StructField('new_deaths_smoothed', DoubleType(), True),
                             StructField('total_cases_per_million', DoubleType(), True),
                             StructField('new_cases_per_million', DoubleType(), True),
                             StructField('new_cases_smoothed_per_million', DoubleType(), True),
                             StructField('total_deaths_per_million', DoubleType(), True),
                             StructField('new_deaths_per_million', DoubleType(), True),
                             StructField('new_deaths_smoothed_per_million', DoubleType(), True),
                             StructField('reproduction_rate', DoubleType(), True),
                             ])

        df_final = spark.createDataFrame([], schema=schema)

        for location in df.columns:
            df_location = df.select('{}'.format(location))

            df_exploded = df_location.withColumn('Exp_RESULTS', F.explode(F.col('{}.data'.format(location)))).\
                withColumn('location', F.col('{}.location'.format(location)))

            try:
                df_final = df_final.union(df_exploded.select('location', 'Exp_RESULTS.date', 'Exp_RESULTS.total_cases',
                                                             'Exp_RESULTS.new_cases', 'Exp_RESULTS.new_cases_smoothed',
                                                             'Exp_RESULTS.total_deaths', 'Exp_RESULTS.new_deaths',
                                                             'Exp_RESULTS.new_deaths_smoothed', 'Exp_RESULTS.total_cases_per_million',
                                                             'Exp_RESULTS.new_cases_per_million', 'Exp_RESULTS.new_cases_smoothed_per_million',
                                                             'Exp_RESULTS.total_deaths_per_million', 'Exp_RESULTS.new_deaths_per_million',
                                                             'Exp_RESULTS.new_deaths_smoothed_per_million', 'Exp_RESULTS.reproduction_rate'))
            except AnalysisException:
                # Ignore records that don't have the attributes above
                pass

        # Just in case to perform tests with a small amount of data
        # df_final.createTempView('table')
        # df_final = spark.sql('select * from table limit 1000')

        return df_final
