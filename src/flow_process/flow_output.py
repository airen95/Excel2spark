from src.columns_process.credit_output import *
from src.constmap import keywords, horizontal_cols
from src.utils import read_excel
import pandas as pd

def flow_credit_output(path_exposure: str):
    exposure = read_excel(path_exposure)
    on, off = table_output(exposure)

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

