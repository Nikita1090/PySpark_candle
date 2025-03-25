from pyspark.sql import SparkSession
import pyspark
import pandas as pd
import xml.etree.ElementTree as ET
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
import sys
from pyspark.sql.functions import col, max as max_, min as min_
from datetime import datetime
from pyspark.sql.functions import sum,avg,max,min,first,last
from pyspark.sql import SQLContext


candle_width = 0

candle_timeto = 10

candle_timefrom = 18


schema = StructType([
    StructField('SYMBOL', StringType(), True),
    StructField('SYSTEM', StringType(), True),
    StructField('MOMENT', StringType(), True),
    StructField('ID_DEAL', IntegerType(), True),
    StructField('PRICE_DEAL', DoubleType(), True),
    StructField('VOLUME', IntegerType(), True),
    StructField('OPEN_POS', IntegerType(), True),
    StructField('DIRECTION', StringType(), True),
])




def get_config(file_path='config.xml'):
    config = {
        'candle.width': '300000',
        'candle.date.from': '19000101',
        'candle.date.to': '20200101',
        'candle.time.from': '1000',
        'candle.time.to': '1800'
    }

    if os.path.isfile(file_path):
        tree = ET.parse(file_path)
        root = tree.getroot()

        for elem in root.findall('property'):
            param_name = elem.find('name').text
            param_value = elem.find('value').text

            if param_name in config:
                config[param_name] = param_value

    return config


def round(num):
    return int(num*10 +0.5) / 10


def get_time_interval(time_str):
    # Преобразование строки во временную метку
    interval_size_ms = candle_width
    dt = datetime.strptime(time_str, '%Y%m%d%H%M%S%f')

    # Вычисление количества промежутков от начала дня до заданного времени
    start_time = datetime(dt.year, dt.month, dt.day, candle_timefrom, 0, 0)
    elapsed_time = dt - start_time
    total_intervals = (elapsed_time.total_seconds() * 1000) // interval_size_ms

    return int(total_intervals)


def map_function(row):
    # извлекаем значения столбцов из строки
    r0 = row[0]
    r1 = row[1]
    r2 = row[2]
    r3 = row[3]
    r4 = row[4]
    r5 = row[5]
    r6 = row[6]
    r7 = row[7]

    # возвращаем новую строку с новыми значениями столбцов
    return (r0, r1, r2, r3, r4, r5, r6, r7, int(get_time_interval(r2)))


def map_function2(row):
    # извлекаем значения столбцов из строки
    r0 = row[0]
    r1 = str(int(row[6]) - (int(row[6]) % candle_width))
    r2 = row[2]
    r3 = row[3]
    r4 = row[4]
    r5 = row[5]
    r6 = row[6]

    # возвращаем новую строку с новыми значениями столбцов
    return (r0, r1, round(r2), round(r3), round(r4), round(r5), r6)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        config_file_path = sys.argv[1]
    else:
        print("Careful! No arg!")
        config_file_path = 'config.xml'

    config = get_config(config_file_path)
    print(config)

    cwd = os.getcwd()

    spark = SparkSession.builder \
        .appName('MyPySparkApp') \
        .config('spark.master', 'local[*]') \
        .config('spark.executor.cores', 4) \
        .config('spark.executor.memory', '2g') \
        .getOrCreate()

    candle_width = int(config['candle.width'])
    candle_timefrom = int(config['candle.time.from'][:2])
    candle_timeto = int(config['candle.time.to'][:2])

    data = spark.read.csv('input.csv', header=False, schema=schema)
    data = data.filter(col("SYMBOL") != "#SYMBOL")
    data.cache()

    first_column = data.select("SYMBOL")
    first_column_rdd = first_column.rdd.distinct()
    list_of_names = first_column_rdd.map(lambda x: x[0]).collect()  # получили список всех фин инструментов

    folder_path = os.path.join(cwd, "output")
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)

    data = data.filter((col("MOMENT").substr(1, 8) >= config['candle.date.from']) &
                       (col("MOMENT").substr(1, 8) < config['candle.date.to']) &
                       (col("MOMENT").substr(9, 4) >= config['candle.time.from']) &
                       (col("MOMENT").substr(9, 4) < config['candle.time.to']))

    rdd = data.rdd.map(map_function)
    df_mapped = spark.createDataFrame(rdd, data.columns + ["Num"])
    #print(df_mapped.select("Num").collect())
    df = df_mapped.groupBy(col("SYMBOL"), col("Num"))\
        .agg(first("PRICE_DEAL"), max("PRICE_DEAL").alias("HIGH"),
             min("PRICE_DEAL").alias("LOW"), last("PRICE_DEAL"), first("MOMENT"))

    data = df.rdd.map(map_function2)
    df = spark.createDataFrame(data, df.columns).drop("first(MOMENT)").cache()

    for element in list_of_names:
        pandas_df = df.filter(col("SYMBOL") == element).toPandas()
        print(element)
        pandas_df.to_csv(os.path.join(cwd, "output/" + str(element) + ".csv"), index=False, header=False)
    spark.stop()
