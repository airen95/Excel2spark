from src.columns_process.columns_scra import *
from src.columns_process.columns_customer import *
from src.columns_process.columns_guarantee import *
from src.columns_process.columns_collateral import *
from src.columns_process.credit_output import *
from src.columns_process.exposure_od_cc import *
from src.utils import *
from src.constmap import keywords, horizontal_cols
import pandas as pd


# flow 2:
# 3 cột cuối, 5 cột O, bảng 6 (AU), bảng 5 cột P, bảng 6, bảng 11(Cancel)

#modified
# 3 cột cuối, 6 (AR -> AT), bảng 6 (0), bảng 6 (AU - AY), bảng 5 , 6, bảng 11

def flow_table_3_for_flow2(frame, table_6):
    """Table 3 with last col"""
    # frame = read_excel(path)
    frame = final_col(frame, table_6)
    return frame

def flow_table_6_for_AR_AT(frame, table_3, table_5):
    """Table 6 to column AR -> AT"""
    frame = final_adjusted_coll(frame, table_3)
    frame = netting_value_adjusted(frame)
    frame = adjusted_guarantee_maturity(frame, table_5)
    return frame

def flow_table_5_for_col_O(frame, table_6):
    """Table 5 for col 0"""
    frame = allocate_guarantee(frame, table_6)
    # frame = guarantee_rwa(frame)
    
    return frame

def flow_table_6_for_AU_AY(frame, table_5):
    frame = final_adjusted_guarantee(frame, table_5)
    frame = ead_before_crm_on_bs(frame)
    frame = ead_before_crm_off_bs(frame)
    frame = ead_after_crm_on_bs(frame)
    frame = ead_after_crm_off_bs(frame)
    return frame

def flow_table_5_for_col_P(frame, table_6):
    """Table 5 for col P"""
    frame = guarantee_rwa(frame, table_6)
    
    return frame

def flow_table_6_for_AZ_BE(frame, table_5):
    """Table 6 to column AZ -> BE"""
    frame = rwa_on_bs(frame, table_5)
    frame = rwa_off_bs(frame)
    frame = final_cols(frame)    
    return frame


def flow_table_11(table_6):
    # exposure = read_excel(path_exposure)
    on, off = table_output(table_6)

    on_table = pd.DataFrame()
    off_table = pd.DataFrame()

    on_table['ASSESS_CLASS'] = horizontal_cols
    off_table['ASSESS_CLASS'] = horizontal_cols

    for col_1, data_1 in on.items():
        on_table[col_1] = data_1

    for col_1, data_1 in off.items():
        off_table[col_1] = data_1

    on_table['Total'] = on_table.apply(lambda x: x['OUTSTANDING AMOUNT'] + x['ACCURED INTEREST']+x['OUTSTANDING FEE'], axis = 1)
    off_table['Total'] = off_table.apply(lambda x: x['OUTSTANDING AMOUNT'] + x['ACCURED INTEREST']+x['OUTSTANDING FEE'], axis = 1)
       
    return on_table, off_table

def flow_2(table_3, table_5, table_6):
    table_3 = flow_table_3_for_flow2(table_3, table_6)
    table_6 = flow_table_6_for_AR_AT(table_6, table_3, table_5)
    table_5 = flow_table_5_for_col_O(table_5, table_6)
    table_6 = flow_table_6_for_AU_AY(table_6, table_5)
    table_5 = flow_table_5_for_col_P(table_5, table_6)
    table_6 = flow_table_6_for_AZ_BE(table_6, table_5)
    # on_table, off_table = flow_table_11(table_6)
    on_table, off_table = [], []
    print('---------Done flow 2 --------------')
    return table_3, table_5, table_6, on_table, off_table