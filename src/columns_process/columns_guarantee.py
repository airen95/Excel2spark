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
    
    IF(D2="N",
        0,
    ELSE:
        IF(ISERROR(E2/SUMIFS(E:E,B:B,B2,D:D,"Y")),
            0,
        ELSE:
            E2/SUMIFS(E:E,B:B,B2,D:D,"Y")))
    """
    frame = frame.withColumn("GUARANTEE_ORIGINAL_MATURITY", datediff(col("GUARANTEE_REMAINING_DATE"), col("GUARANTEE_ORIGINAL_DATE")) / 365.0)
    
    frame = frame.withColumn("GUARANTEE_REMAINING_MATURITY", datediff(col("GUARANTEE_REMAINING_DATE"), col("REPORTING_DATE")) / 365.0)
    
    window_spec = Window.partitionBy(col("CUSTOMER_ID"))

    frame = frame.withColumn('sum',
            when(col('ELIGIBLE_GUARANTEE_FLAG') == 'Y', None).otherwise(
                sum(col('GUARANTEE_VALUE')).over(window_spec)
            )
        ).withColumn("GUARANTEE_CRM_ELIGIBLE %",
            when(col("ELIGIBLE_GUARANTEE_FLAG") == "N", 0)\
                .otherwise(
                    when(col("sum") == 0, 0)\
                    .otherwise(when(is_error(col("GUARANTEE_VALUE") / col("sum"))==1, 0)\
                    .otherwise(col("GUARANTEE_VALUE") / col("sum")))
                ) 
        ).drop('sum', 'value')      
        # ).withColumn("GUARANTEE_CRM_ELIGIBLE %", concat(col("value"), lit("%")))\
    
    return frame

def allocate_guarantee(frame, exposure):
    """
    Table 6 first
    =SUMIFS($'6. EXPOSURE'.AT:AT,
        $'6. EXPOSURE'.B:B,$'5. GUARANTEE'.B2,
        $'6. EXPOSURE'.AO:AO,">="&K2)*N2
    """

    key1="CUSTOMER_ID"
    key2="CUSTOMER_ID"
    cols = ['RW_CC', 'ADJUSTED_GUARANTEE_MATURITY']
    # Join 2 table
    frame = join_frame(frame, exposure, key1, key2, cols)
    frame = frame.fillna({'RW_CC': 0, 'ADJUSTED_GUARANTEE_MATURITY': 0})
    
    window_spec = Window.partitionBy(col("CUSTOMER_ID"))
    frame = frame.withColumn("sum", 
                            when(col('RW_CC') < col('GUARANTOR_RW'), 0) \
                                .otherwise(sum(col('ADJUSTED_GUARANTEE_MATURITY')).over(window_spec))) \
                .withColumn("ALLOCATED_GUARANTEE_AFTER_MT_MISMATCH", col("sum")*col("GUARANTEE_CRM_ELIGIBLE %")) \
                .drop('sum',cols[0], cols[1])
    return frame

def guarantee_rwa(frame, exposure):
    """
    Table 6 first
    =SUMIFS($'6. EXPOSURE'.AU:AU,
        $'6. EXPOSURE'.B:B,$'5. GUARANTEE'.B2,
            $'6. EXPOSURE'.AO:AO,">="&K2)*N2*K2
    """

    key1="CUSTOMER_ID"
    key2="CUSTOMER_ID"
    cols = ['RW_CC', 'FINAL_ADJUSTED_GUARANTEE']
    # Join 2 table
    frame = join_frame(frame, exposure, key1, key2, cols)
    frame = frame.fillna({'RW_CC': 0, 'FINAL_ADJUSTED_GUARANTEE': 0})
    
    window_spec = Window.partitionBy(col("CUSTOMER_ID"))
    frame = frame.withColumn("sum", 
                        when(col('RW_CC') < col('GUARANTOR_RW'), 0)
                        .otherwise(sum(col('FINAL_ADJUSTED_GUARANTEE')).over(window_spec))) \
            .withColumn("GUARANTEE_RWA", col("sum")*col("GUARANTEE_CRM_ELIGIBLE %")*col('GUARANTOR_RW')) \
            .withColumn("GUARANTEE_CRM_ELIGIBLE %", concat(col("GUARANTEE_CRM_ELIGIBLE %") * 100, lit("%"))) \
            .drop('sum',cols[0], cols[1])
            
    frame = frame.where(~col("CUSTOMER_ID").isNull())
    
    return frame


