from omegaconf import OmegaConf
import time
from tqdm import tqdm
from pyspark.sql.functions import col


from src.flow_process.flow_6 import *
from src.utils import write_excel



cfg = OmegaConf.load('config/source.yaml')
select_columns = cfg.select_columns
path_1 = cfg.data_path['customer']
path_2 = cfg.data_path['scra']
path_3 = cfg.data_path['collateral']
path_od = cfg.data_path['od']
path_cc = cfg.data_path['cc']
path_5 = cfg.data_path['guarantee']
path_6 = cfg.data_path['exposure']


def path_save(path_in: str) -> str:
    return path_in.replace(cfg.data_path.path, cfg.path_save).replace(".xlsx", "")

def run(path_1, path_3, path_5, path_6):
    t1 = time.time()
    table_1, table_2, table_3, table_od, table_cc, table_5, table_6 = flow_1(path_1, path_3, path_5, path_6)
    
    table_3, table_5, table_6, on_table, off_table = flow_2(table_3, table_5, table_6)
    
    list_tables = [table_1, table_2, table_3, table_5, table_od, table_cc, table_6]
    print(f'All process in {time.time() - t1:.2f}')
    
    return list_tables

    

if __name__ == "__main__":
    
    list_tables = run(path_1, path_3, path_5, path_6)
  

    write_excel(list_tables[0], path_save(path_1))
    write_excel(list_tables[1], path_save(path_2))
    write_excel(list_tables[4], path_save(path_od))
    write_excel(list_tables[5], path_save(path_cc))
    write_excel(list_tables[2], path_save(path_3))
    write_excel(list_tables[3], path_save(path_5))
    write_excel(list_tables[6], path_save(path_6))
    
    t1 = time.time()
    exposure = flow_exposure(path_6)
    exposure = exposure.select(*select_columns)
    write_excel(exposure, 'output/exposure.xlsx')

    print(f'Process in {time.time() - t1}.2f')