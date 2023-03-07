from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pandas as pd
from .constmap import *

spark = SparkSession.builder.config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.1.1_0.18.2").getOrCreate()

def replace_null(c, alternate):
    if not c:
        return alternate
    # print('C herreeeeeeeeeeeeee: ',c)
    # return coalesce(c, alternate)
    return c

replace_null = udf(replace_null, StringType())

def is_error(c):
    if (c == '') or (c == 'N/A') or (c == 'NA') or (c == ' '):
        return 1
    return 0

is_error = udf(is_error, IntegerType())

def make_spark_mapping(sheet_name: str, table_name: str):
    tmp = pd.read_excel('/home/dieule/Downloads/input_test/PARAMETER.xlsx', sheet_name = sheet_name)
    [r1, c1, c2] = config[sheet_name][table_name]['index']
    schema = config[sheet_name][table_name]['schema']
    tmp = tmp.iloc[r1:, c1:c2].reset_index(drop='True')
    df_schema = StructType(schema)
    tmp = spark.createDataFrame(tmp, schema=df_schema)
    return tmp

def read_excel(path, sheet_name: str = None):
    if sheet_name:
        df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("treatEmptyValuesAsNulls", "true") \
            .option("dataAddress", f"\'{sheet_name}\'!A1") \
            .option("maxRowsInMemory", 2000)\
            .option("maxByteArraySize", 2147483647)\
            .option("inferSchema", "true") \
            .load(path)
    else:
        df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("treatEmptyValuesAsNulls", "true") \
            .option("maxRowsInMemory", 2000)\
            .option("maxByteArraySize", 2147483647)\
            .option("inferSchema", "true") \
            .load(path)
    return df

def join_frame(frame_source, frame_join, key1: str, key2: str, cols: list):
    frame_source = frame_source.join(frame_join, frame_source[key1] == frame_join[key2], how = 'left')\
        .select(frame_source['*'], *[frame_join[col_name] for col_name in cols])
    return frame_source

def make_lookup(frame, key_col, val_col):
    keys = frame.select(key_col).\
      rdd.flatMap(lambda x: x).collect()
    values = frame.select(val_col).\
        rdd.flatMap(lambda x: x).collect()

    dct = {k: v for k, v in zip(keys, values)}
    return dct

# def find_ltv(value_in, ltv_dct):
#     try:
#         value_in = float(value_in)
#         for level, value in ltv_dct.items():
#             if value_in <= value:
#                 return level
#         return "ERROR IN CONFIG"
#     except:
#         return "ERROR IN CONFIG"

def find_ltv(ltv_dct):
    def f(x):
        try: 
            value_in = float(x)
            for level, value in ltv_dct.items():
                if value_in <= value:
                    return level
            return "ERROR IN CONFIG"
        except:
            return "ERROR IN CONFIG"
    return udf(f)

# find_ltv = udf(find_ltv, MapType(StringType(), StringType()))

def map_dct(ccf_dct):
    def ff(value_in):
        if value_in in ccf_dct:
            return float(ccf_dct[value_in])
        else:
            return 'N/A'
    return udf(ff)

# map_dct = udf(map_dct, StringType())


def vlookup(vlookup_dct, alternative):
    def ff(value_in):
        try:
            for k, v in vlookup_dct.items():
                if value_in == k:
                    return v
            return alternative
        except:
            return alternative
    return udf(ff)


def write_excel(frame, path_save: str): 
    frame.write\
      .format("com.crealytics.spark.excel")\
      .option("header", "true")\
      .save(path_save)