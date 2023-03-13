from omegaconf import OmegaConf
import time
from tqdm import tqdm
from pyspark.sql.functions import col


from src.flow_process.flow_6 import *
from src.utils import write_excel



cfg = OmegaConf.load('config/source.yaml')
select_columns = cfg.select_columns
# path_1 = cfg.data_path['customer']
# path_2 = cfg.data_path['scra']
# path_3 = cfg.data_path['collateral']
# path_od = cfg.data_path['od']
# path_cc = cfg.data_path['cc']
# path_5 = cfg.data_path['guarantee']
path_6 = cfg.data_path['exposure']

if __name__ == "__main__":
    t1 = time.time()
    exposure = flow_exposure(path_6)
    exposure = exposure.select(*select_columns)
    write_excel(exposure, 'output/exposure.xlsx')

    print(f'Process in {time.time() - t1}.2f')



    