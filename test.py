from src.flow_process.flow_6_case_2 import *
# from src.utils import write_excel
from omegaconf import OmegaConf

cfg = OmegaConf.load('config/source.yaml')
select_columns = cfg.select_columns

path_6 = cfg.data_path['exposure']

def write_excel(frame, path_save: str):
    t1 = time.time()
     
    frame.write\
      .format("com.crealytics.spark.excel")\
      .mode("overwrite")\
      .option("header", "true")\
      .save(path_save)

if __name__ == "__main__":
    t1 = time.time()
    exposure = flow_exposure(path_6)
    exposure = exposure.select(*select_columns)
    write_excel(exposure, 'output/test_6.xlsx')
    print(f'All done in {time.time() - t1}:.2f')
