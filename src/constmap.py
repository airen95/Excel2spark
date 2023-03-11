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

        },
        'RATING TABLE': {'index': [7, 2, 5],
            'schema': [StructField("RATING_AGENCY_CD", StringType(), True)\
                       ,StructField("CUSTOMER_RATING", StringType(), True)\
                       ,StructField("RATING_CD", StringType(), True)\
                       ]           

        },
        'HAIRCUT TABLE': {'index': [7, 12, 14],
            'schema': [StructField("HAIRCUT_CD", StringType(), True)\
                       ,StructField("HAIRCUT%", StringType(), True)\
                       ]           
        },
        'COLL MATURITY BUCKETS': {'index': [7, 19, 22],
            'schema': [StructField("MIN_MATURITY", StringType(), True)\
                       ,StructField("MAX_MATURITY", StringType(), True),StructField("MATURITY_BUCKET", StringType(), True)  
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
        },
        'haircut': {'index': [9, 19, 21],\
            'schema': [StructField("Concatenated column", StringType(), True)\
                       ,StructField("HAIRCUT_CD", StringType(), True)
                       ]          

        },
        'counterparty_mapping': {'index': [9, 1, 4],\
            'schema': [StructField("Concatenated column", StringType(), True)\
                       ,StructField("CPTY_TYPE", StringType(), True)\
                       ,StructField("CPTY_SUB_TYPE", StringType(), True)\
                       ]
        },
        'collateral_mapping': {'index': [9, 9, 12],\
            'schema': [StructField("COLL_TYPE", StringType(), True)\
                       ,StructField("CRM_CD", StringType(), True)\
                       ,StructField("ELIGIBLE_CRM", StringType(), True)\
                       ]
        }
    }
    # '2. SCRA': {
    #     'SCRA' : {'index': [9, 0, 9],\
    #         'schema': [StructField(scra_columns['A'], StringType(), True)\
    #                    ,StructField(scra_columns['B'], StringType(), True)\
    #                    ,StructField(scra_columns['C'], StringType(), True)\
    #                    ,StructField(scra_columns['D'], StringType(), True)\
    #                    ,StructField(scra_columns['E'], StringType(), True)\
    #                    ,StructField(scra_columns['F'], StringType(), True)\
    #                    ,StructField(scra_columns['G'], StringType(), True)\
    #                    ,StructField(scra_columns['H'], StringType(), True)\
    #                    ,StructField(scra_columns['I'], StringType(), True)\
    #                 ]
    #     }
    # }
}

output_config = {
    'off': {'EAD BEFORE CRM': 'EAD BEFORE CRM (OFF-BS)',\
        'COLL AMT': 'FINAL_ADJUSTED_COLL',\
        'NETTING AMT': 'NETTING_VALUE_ADJUSTED',\
        'GUARANTEE AMT': 'FINAL_ADJUSTED_GUARANTEE',\
        'RWA WITH CRM': 'RWA_OFF_BS',\
        'RWA WITHOUT CRM': 'RWA_OFF_BS Without CRM',\
        'EAD AFTER CRM': 'EAD AFTER CRM (OFF-BS)'
    
    },
    'on': {
        'EAD BEFORE CRM': 'EAD BEFORE CRM (ON-BS)',\
        'COLL AMT': 'FINAL_ADJUSTED_COLL',\
        'NETTING AMT': 'NETTING_VALUE_ADJUSTED',\
        'GUARANTEE AMT': 'FINAL_ADJUSTED_GUARANTEE',\
        'RWA WITH CRM': 'RWA_ON_BS',\
        'RWA WITHOUT CRM': 'RWA_ON_BS Without CRM',\
        'OUTSTANDING AMOUNT': 'OUTSTANDING_AMT_LCY / UTILIZED LIMIT',\
        'ACCURED INTEREST': 'ACCRUED_INTEREST_LCY',\
        'OUTSTANDING FEE': 'OUTSTANDING_FEES_PENALITIES_LCY',\
        'EAD AFTER CRM': 'EAD AFTER CRM (ON-BS)'
    }
}

keywords = ['CASH', 'VAMC', 'DATC', 'SOVEREIGN', 'PSE', 'MDB', \
    'DOM_CIS', 'FOR_CIS', "COVERED BOND RATED", 'COVERED BOND UNRATED', 'CORP_GEN', 'SME',\
    'SL', 'REGULATORY RETAIL', 'TRANSACTOR', 'REG_RETAIL_SME', 'OTHER RETAIL',\
    'CRE_INC', 'CRE_GEN', 'RRE_INC', 'RRE_GEN', 'ADC', 'ADC_RRE', 'BAD_DEBT', 'BAD_DEBT_RRE_GEN',\
    'EQUITY_INV', 'OTHER ASSETS']


cols = {'CASH, GOLD AND CASH EQUIVALENT' : ['CASH'],
 'EXPOSURE TO SOVEREIGNS': ['VAMC', 'DATC', 'SOVEREIGN'],
 'VAMC/DATC': ['VAMC', 'DATC'],
 'SOVEREIGN': ['SOVEREIGN'],
 'EXPOSURE TO PSEs': ['PSE'],
 'EXPOSURE TO MDBs': ['MDB'],
 'EXPOSURE TO BANKS': ['DOM_CIS', 'FOR_CIS', 'COVERED BOND RATED', 'COVERED BOND UNRATED'],
 'EXPOSURE TO CORPORATES': ['CORP_GEN', 'SME', 'SL'],
 'GENERAL CORPORATE EXPOSURE': ['CORP_GEN', 'SME'],
 'SPECIALIZED LENDING EXPOSURE': ['SL'],
 'RETAIL EXPOSURES': ['REGULATORY RETAIL', 'TRANSACTOR', 'REG_RETAIL_SME', 'REG_RETAIL_SME', 'OTHER RETAIL'],
 'REGULATORY RETAIL': ['REGULATORY RETAIL'],
 'TRANSACTORS': ['TRANSACTOR'],
 'REGULATORY RETAIL SME': ['REG_RETAIL_SME'],
 'OTHER RETAIL': ['OTHER RETAIL'],
 'REAL ESTATE EXPOSURE': ['CRE_INC', 'CRE_GEN', 'RRE_INC', 'RRE_GEN', 'ADC', 'ADC_RRE'],
 'COMMERCIAL REAL ESTATE': ['CRE_INC', 'CRE_GEN'],
 'INCOME PRODUCING CRE': ['CRE_INC'],
 'GENERAL CRE': ['CRE_GEN'],
 'RESIDENTIAL REAL ESTATE': ['RRE_INC', 'RRE_GEN'],
 'INCOME PRODUCING RRE': ['RRE_INC'],
 'GENERAL RRE': ['RRE_GEN'],
 'LAND ACQUISITION, DEVELOPMENT AND CONSTRUCTION': ['ADC', 'ADC_RRE'],
 'DEFAULTED EXPOSURES': ['BAD_DEBT', 'BAD_DEBT_RRE_GEN'],
 'SUBORDINATED DEBT AND EQUITY': ['EQUITY_INV'],
 'OTHER ASSETS': ['OTHER ASSETS']}

vertical_cols = ['EAD BEFORE CRM', 'COLL AMT', 'NETTING AMT',
       'GUARANTEE AMT', 'RWA WITH CRM', 'RWA WITHOUT CRM',
       'OUTSTANDING AMOUNT', 'ACCURED INTEREST', 'OUTSTANDING FEE',
       'EAD AFTER CRM']

horizontal_cols = ['CASH, GOLD AND CASH EQUIVALENT',
 'EXPOSURE TO SOVEREIGNS',
 'VAMC/DATC',
 'SOVEREIGN',
 'EXPOSURE TO PSEs',
 'EXPOSURE TO MDBs',
 'EXPOSURE TO BANKS',
 'EXPOSURE TO CORPORATES',
 'GENERAL CORPORATE EXPOSURE',
 'SPECIALIZED LENDING EXPOSURE',
 'RETAIL EXPOSURES',
 'REGULATORY RETAIL',
 'TRANSACTORS',
 'REGULATORY RETAIL SME',
 'OTHER RETAIL',
 'REAL ESTATE EXPOSURE',
 'COMMERCIAL REAL ESTATE',
 'INCOME PRODUCING CRE',
 'GENERAL CRE',
 'RESIDENTIAL REAL ESTATE',
 'INCOME PRODUCING RRE',
 'GENERAL RRE',
 'LAND ACQUISITION, DEVELOPMENT AND CONSTRUCTION',
 'DEFAULTED EXPOSURES',
 'SUBORDINATED DEBT AND EQUITY',
 'OTHER ASSETS']