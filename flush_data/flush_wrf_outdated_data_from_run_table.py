#!/home/uwcc-admin/curw_fcst_db_utils/venv/bin/python3
import traceback
from datetime import datetime, timedelta

from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.curw_fcst.common import get_distinct_fgts_for_given_id, get_curw_fcst_hash_ids
from db_adapter.curw_fcst.timeseries import Timeseries


WRF_A_4_1_2 = 19
WRF_C_4_1_2 = 20
WRF_E_4_1_2 = 22
WRF_SE_4_1_2 = 21

WRF_C_4_0 = 12
WRF_E_4_0 = 13
WRF_A_4_0 = 15
WRF_SE_4_0 = 16

WRF_A_v3 = 1
WRF_C_v3 = 2
WRF_E_v3 = 3
WRF_SE_v3 = 4

WRF_A_v4 = 5
WRF_C_v4 = 6
WRF_E_v4 = 7
WRF_SE_v4 = 8


def flush_run_entry_outdated(pool, hash_ids):

    TS = Timeseries(pool=pool)

    ################################################################################
    # delete as a whole (entry in run table and all related entries in data table) #
    ################################################################################
    count = 0
    for id in hash_ids:
        TS.delete_all_by_hash_id(id_=id)
        count+=1
        print(count, id)
    print("{} of hash ids are deleted".format(len(hash_ids)))


if __name__=="__main__":

    try:

        set_db_config_file_path('/home/uwcc-admin/curw_fcst_MME_utils/db_adapter_config.json')

        pool = get_Pool(host=connection.CURW_FCST_HOST, port=connection.CURW_FCST_PORT, user=connection.CURW_FCST_USERNAME,
                        password=connection.CURW_FCST_PASSWORD, db=connection.CURW_FCST_DATABASE)

        # pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT,
        #                 user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD, db=CURW_FCST_DATABASE)

        wrf_A_hash_ids = get_curw_fcst_hash_ids(pool=pool, sim_tag="gfs_d0_18", source_id=WRF_A_4_0,
                                          variable_id=None, unit_id=None, station_id=None,
                                          start=None, end=None)

        flush_run_entry_outdated(pool=pool, hash_ids=wrf_A_hash_ids)

        wrf_C_hash_ids = get_curw_fcst_hash_ids(pool=pool, sim_tag="gfs_d0_18", source_id=WRF_C_4_0,
                                                variable_id=None, unit_id=None, station_id=None,
                                                start=None, end=None)

        flush_run_entry_outdated(pool=pool, hash_ids=wrf_C_hash_ids)

        wrf_E_hash_ids = get_curw_fcst_hash_ids(pool=pool, sim_tag="gfs_d0_18", source_id=WRF_E_4_0,
                                                variable_id=None, unit_id=None, station_id=None,
                                                start=None, end=None)

        flush_run_entry_outdated(pool=pool, hash_ids=wrf_E_hash_ids)

        wrf_SE_hash_ids = get_curw_fcst_hash_ids(pool=pool, sim_tag="gfs_d0_18", source_id=WRF_SE_4_0,
                                                variable_id=None, unit_id=None, station_id=None,
                                                start=None, end=None)

        flush_run_entry_outdated(pool=pool, hash_ids=wrf_SE_hash_ids)


    except Exception as e:
        print('An exception occurred.')
        traceback.print_exc()
    finally:
        print("Process finished")
        destroy_Pool(pool=pool)
