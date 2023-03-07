from pyspark.sql.types import *


config = {
    '9. REG TABLE': {
        'SPECIFIC PROVISIONS BUCKETS': {'index':[7, 23, 26],\
            'schema': [StructField("MIN_PROVISION", StringType(), True)\
                       ,StructField("MAX_PROVISION", StringType(), True)\
                       ,StructField("MATURITY_BUCKET", StringType(), True)\
                       ]},
        'LTV': {'index': [7, 15, 18],\
            'schema': [StructField("LTV_CD", StringType(), True)\
                       ,StructField("MIN (NOT_INCLUDING)", StringType(), True)\
                       ,StructField("MAX (INCLUDING)", StringType(), True)\
                       ]

        },
        'KEY CONFIGURATIONS': {'index': [7, 31, 33],
            'schema': [StructField("LABEL", StringType(), True)\
                       ,StructField("VALUE", StringType(), True)\
                       ]        

        },
        'CCF TABLE': {'index': [7, 9, 11],
            'schema': [StructField("CCF_CD", StringType(), True)\
                       ,StructField("CCF%", StringType(), True)\
                       ]           

        },
        'RETAIL QUALIFYING CRITERIA': {'index': [7, 34, 36],\
            'schema': [StructField("LABEL", StringType(), True)\
                       ,StructField("VALUE", StringType(), True)\
                       ]            

        }
    },
    '7. REG TABLE CAL': {
        'RATING TABLE MAPPING' : {'index': [9, 13, 15],\
            'schema': [StructField("Concatenated column", StringType(), True)\
                       ,StructField("RATING_CD", StringType(), True)
                       ]

        },
        'CCF MAPPING': {'index': [9, 16, 18],\
            'schema': [StructField("Concatenated column", StringType(), True)\
                       ,StructField("CCF_CD", StringType(), True)
                       ]            

        },
        'assess_mapping': {'index': [9, 22, 25],\
            'schema': [StructField("Concatenated column", StringType(), True)\
                       ,StructField("ASSET_CLASS", StringType(), True)\
                       ,StructField("ASSET_SUB_CLASS", StringType(), True)\
                       ]
        },
        'product_mapping': {'index': [9, 5, 8],\
            'schema': [StructField("Concatenated column", StringType(), True)\
                       ,StructField("PRODUCT_TYPE", StringType(), True)\
                       ,StructField("PRODUCT_SUB_TYPE", StringType(), True)\
                       ]
            }
    }
}