from omegaconf import OmegaConf
import time
from tqdm import tqdm

from src.flow_process.flow_1 import flow_1 
from src.flow_process.flow_2 import flow_2 
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
    return path_in.replace(cfg.data_path.path, cfg.path_save).replace(".xlsx", ".csv")

def run(path_1, path_3, path_5, path_6):
    t1 = time.time()
    table_1, table_2, table_3, table_od, table_cc, table_5, table_6 = flow_1(path_1, path_3, path_5, path_6)
    
    table_3, table_5, table_6, on_table, off_table = flow_2(table_3, table_5, table_6)
    
    list_tables = [table_1.toPandas(), table_2.toPandas(), table_3.toPandas(), table_5.toPandas(), table_od.toPandas(), table_cc.toPandas(), table_6.toPandas()]
    print(f'All process in {time.time() - t1:.2f}')
    
    return list_tables

    

if __name__ == "__main__":
    
    list_tables = run(path_1, path_3, path_5, path_6)
  
    # write_excel(table_1, path_save(path_1))
    # write_excel(table_2, path_save(path_2))
    # write_excel(table_od, path_save(path_od))
    # write_excel(table_cc, path_save(path_cc))
    # write_excel(table_3, path_save(path_3))
    # write_excel(table_5, path_save(path_5))
    # write_excel(table_6, path_save(path_6))

    