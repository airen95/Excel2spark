from pyspark.sql import SparkSession, Window
import pyspark
from operator import add
from functools import reduce
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
from omegaconf import OmegaConf

from src.utils import *
from src.constmap import *

source = OmegaConf.load('config/source.yaml')



def ori_mature_remain_mature_crm_eligible(frame):
    """
    (I-H) / 365
    (I-J) / 365
    
    IF(D2="N",0,IF(ISERROR(E2/SUMIFS(E:E,B:B,B2,D:D,"Y")),0,E2/SUMIFS(E:E,B:B,B2,D:D,"Y")))
    """
    frame = frame.withColumn("GUARANTEE_ORIGINAL_MATURITY", datediff(col("GUARANTEE_REMAINING_DATE"), col("GUARANTEE_ORIGINAL_DATE")) / 365.0)
    
    frame = frame.withColumn("GUARANTEE_REMAINING_MATURITY", datediff(col("GUARANTEE_REMAINING_DATE"), col("REPORTING_DATE")) / 365.0)
    
    window_spec = Window.partitionBy(col("CUSTOMER_ID"))

    frame = frame.withColumn('sum',
            when(col('ELIGIBLE_GUARANTEE_FLAG') == 'Y', None).otherwise(
                sum(col('GUARANTEE_VALUE')).over(window_spec)
            )
        ).withColumn("value",
            when(col("ELIGIBLE_GUARANTEE_FLAG") == "N", 0)\
                .otherwise(
                    when(col("sum") == 0, 0)\
                    .otherwise(when(is_error(col("GUARANTEE_VALUE") / col("sum"))==1, 0)\
                    .otherwise(col("GUARANTEE_VALUE") / col("sum")))
                )       
        ).withColumn("GUARANTEE_CRM_ELIGIBLE %", concat(col("value"), lit("%")))\
        .drop('sum', 'value')
    
    return frame

def allocate_guarantee(frame, exposure):
    """
    Table 6 first
    =SUMIFS($'6. EXPOSURE'.AT:AT,$'6. EXPOSURE'.B:B,$'5. GUARANTEE'.B2,$'6. EXPOSURE'.AO:AO,">="&K2)*N2
    
    =SUMIFS($'6. EXPOSURE'.AU:AU,$'6. EXPOSURE'.B:B,$'5. GUARANTEE'.B2,$'6. EXPOSURE'.AO:AO,">="&K2)*N2*K2
    """
    # Must read processed table 6***********
    # exposure = read_excel('/content/drive/MyDrive/Excel2spark/input_test/EXPOSURE.xlsx')

    key1="CUSTOMER_ID"
    key2="CUSTOMER_ID"
    cols = ['RW_CC', 'ADJUSTED_GUARANTEE_MATURITY']
    # Join 2 table
    frame = join_frame(frame, exposure, key1, key2, cols)
    
    window_spec = Window.partitionBy(col("CUSTOMER_ID"))
    frame = frame.withColumn("sum", 
                            when(col('RW_CC') >= col('GUARANTOR_RW'), None)
                            .otherwise(sum(col('ADJUSTED_GUARANTEE_MATURITY')).over(window_spec))) \
                .withColumn("ALLOCATED_GUARANTEE_AFTER_MT_MISMATCH", col("sum")*col("GUARANTEE_CRM_ELIGIBLE %")) \
                .drop('sum',cols[0], cols[1])
             
    return frame

def guarantee_rwa(frame, exposure):
    """
    Table 6 first
    =SUMIFS($'6. EXPOSURE'.AT:AT,$'6. EXPOSURE'.B:B,$'5. GUARANTEE'.B2,$'6. EXPOSURE'.AO:AO,">="&K2)*N2
    
    =SUMIFS($'6. EXPOSURE'.AU:AU,$'6. EXPOSURE'.B:B,$'5. GUARANTEE'.B2,$'6. EXPOSURE'.AO:AO,">="&K2)*N2*K2
    """
    # Must read processed table 6***********
    # exposure = read_excel('/content/drive/MyDrive/Excel2spark/input_test/EXPOSURE.xlsx')

    key1="CUSTOMER_ID"
    key2="CUSTOMER_ID"
    cols = ['RW_CC', 'FINAL_ADJUSTED_GUARANTEE']
    # Join 2 table
    frame = join_frame(frame, exposure, key1, key2, cols)
    
    window_spec = Window.partitionBy(col("CUSTOMER_ID"))
    frame = frame.withColumn("sum", 
                            when(col('RW_CC') >= col('GUARANTOR_RW'), None)
                            .otherwise(sum(col('FINAL_ADJUSTED_GUARANTEE')).over(window_spec))) \
                .withColumn("GUARANTEE_RWA", col("sum")*col("GUARANTEE_CRM_ELIGIBLE %")*col('GUARANTOR_RW')) \
                .drop('sum',cols[0], cols[1])
             
    return frame


