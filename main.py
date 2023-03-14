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
    return path_in.replace(cfg.data_path.path, cfg.path_save).replace(".xlsx", "")

def run(path_1, path_3, path_5, path_6):
    t1 = time.time()
    table_1, table_2, table_3, table_od, table_cc, table_5, table_6 = flow_1(path_1, path_3, path_5, path_6)
    
    table_3, table_5, table_6, on_table, off_table = flow_2(table_3, table_5, table_6)
    table_6 = table_6.select(*select_columns)
    list_tables = [table_1, table_2, table_3, table_5, table_od, table_cc, table_6, on_table, off_table]
    print(f'All process in {time.time() - t1:.2f}')
    
    return list_tables

    

if __name__ == "__main__":
    
    list_tables = run(path_1, path_3, path_5, path_6)
  

    write_excel(list_tables[0], cfg.path_save.customer)
    write_excel(list_tables[1], cfg.path_save.scra)
    write_excel(list_tables[4], cfg.path_save.od)
    write_excel(list_tables[5], cfg.path_save.cc)
    write_excel(list_tables[2], cfg.path_save.collateral)
    write_excel(list_tables[3], cfg.path_save.guarantee)
    write_excel(list_tables[6], cfg.path_save.exposure)
    # write_excel(list_tables[7], "./output/ontable")
    # write_excel(list_tables[8], "./output/offtable")
    
    # # t1 = time.time()
    # exposure = flow_exposure(path_6)
    # exposure = exposure.select(*select_columns)
    # write_excel(exposure, 'output/exposure.xlsx')

    # print(f'Process in {time.time() - t1}.2f')