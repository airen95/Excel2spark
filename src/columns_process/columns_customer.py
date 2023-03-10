from pyspark.sql import SparkSession, Window
import pyspark
from operator import add
from functools import reduce
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
from omegaconf import OmegaConf

from .utils import *

source = OmegaConf.load('config/source.yaml')

# =VLOOKUP(B2&C2,$'7. REG TABLE CAL'.B:D,2,0)
# =IFERROR(VLOOKUP(B2&C2,$'7. REG TABLE CAL'.B:D,3,0),"Check")
def cpty_type_cpty_sub_type(frame):
    counterparty_mapping = make_spark_mapping('7. REG TABLE CAL', 'counterparty_mapping')
    
    frame = frame.withColumn("lookup_value", concat(col("CPTY_TYPE1"), col("CPTY_SUB_TYPE2")))

    key1 = 'lookup_value'
    key2 = 'Concatenated column'
    cols = ['CPTY_TYPE', 'CPTY_SUB_TYPE']

    frame = join_frame(frame, counterparty_mapping, key1, key2, cols)
    
    # Handle errors using the coalesce() function
    frame = frame.withColumn("CPTY_SUB_TYPE", coalesce(col("CPTY_SUB_TYPE9"), lit("Check")))
    frame.drop("CPTY_TYPE8", "CPTY_SUB_TYPE9", "CUST_RATING_CD")
    return frame


def cust_rating_cd(frame):
    """
    if IF(AND(E2&D2<>"NANA",E2&D2<>""),
        VLOOKUP(E2&D2,'7. REG TABLE CAL'!N:O,2,0),
    else:
        IF(AND(OR(E2&D2="NANA",E2&D2=""),B2<>"FIN_INST"),
           "LT7"
        else:
            IF(AND(OR(E2&D2="NANA",E2&D2=""),B2="FIN_INST")
                IF(VLOOKUP(A2,'2. SCRA'!$A:$A,1,FALSE)='1. CUSTOMER'!A2,
                    VLOOKUP(A2,'2. SCRA'!$A:$J,10,FALSE),"NA"))))
    """
    rating_mapping = make_spark_mapping('7. REG TABLE CAL', 'RATING TABLE MAPPING')
    rating_mapping = rating_mapping.where(col( 'Concatenated column')!='NaN')
    rating_dct = make_lookup(rating_mapping, 'Concatenated column', 'RATING_CD')
    
    ###
    scra = pd.read_excel('/home/dieule/Downloads/input_test/SCRA.xlsx', header = 1)
    scra_dct = {str(k): v for k, v in zip(scra['CUSTOMER_ID'], scra['SCRA Group'])}

    ###
    
    frame = frame.withColumn('e2d2', concat(col('RATING_AGENCY_CODE'), col('CUSTOMER_RATING')))\
        .withColumn('CUST_RATING_CD',\
        when(~col('e2d2').isin('', 'NANA'), vlookup(rating_dct, 'NA')(col('e2d2'))).otherwise(\
            when((col('e2d2').isin('', 'NANA')) & (col('CPTY_TYPE1')!='FIN_INST'), 'LT7').otherwise(\
            when((col('e2d2').isin('', 'NANA')) & (col('CPTY_TYPE1')=='FIN_INST'), vlookup(scra_dct, 'NA')(col('CUSTOMER_ID'))).otherwise('NA') 
                )
        )).drop('e2d2')

    return frame
