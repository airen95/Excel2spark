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

# =IF(OR(B3="",C3="",D3="",E3="",F3="",G3="",H3="",I3=""),"INVALID",IF( AND(B3="Y",C3="Y",I3="Y"),"Group A and satisfy #",IF(AND(B3="Y",C3="Y"),"Group A",IF(E3="Y","Group B","Group C"))))

def scra_group(frame):
    frame = frame.withColumn("SCRA Group", when(
        (col("`{}`".format(scra_columns['B'])) == "") | (col("`{}`".format(scra_columns['C'])) == "") | 
        (col("`{}`".format(scra_columns['D'])) == "") | (col("`{}`".format(scra_columns['E'])) == "") |
        (col("`{}`".format(scra_columns['F'])) == "") | (col("`{}`".format(scra_columns['G'])) == "") |
        (col("`{}`".format(scra_columns['H'])) == "") | (col("`{}`".format(scra_columns['I'])) == ""),
        "INVALID").otherwise(
        when((col("`{}`".format(scra_columns['B'])) == "Y") & (col("`{}`".format(scra_columns['C'])) == "Y") & 
            (col("`{}`".format(scra_columns['I'])) == "Y"), "Group A and satisfy #")
        .when((col("`{}`".format(scra_columns['B'])) == "Y") & (col("`{}`".format(scra_columns['C'])) == "Y"), "Group A")
        .when(col("`{}`".format(scra_columns['E'])) == "Y", "Group B")
        .otherwise("Group C")
    ))
    return frame

