from columns_process.exposure_od_cc import transactor_flag_od
from .utils import *

def flow_od(path_od: str):
    frame = read_excel(path_od)
    frame = transactor_flag_od(frame)
    return frame
