#!/home/uwcc-admin/curw_fcst_db_utils/venv/bin/python3
import traceback
from datetime import datetime, timedelta

from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.curw_fcst.common import get_distinct_fgts_for_given_id, get_curw_fcst_hash_ids
from db_adapter.curw_fcst.timeseries import Timeseries

# from db_adapter.constants import CURW_FCST_USERNAME, CURW_FCST_PORT, CURW_FCST_PASSWORD, CURW_FCST_HOST, \
#     CURW_FCST_DATABASE

WRF_A_ID_4_1_2 = 19
WRF_C_ID_4_1_2 = 20
WRF_E_ID_4_1_2 = 22
WRF_SE_ID_4_1_2 = 21

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

WRF_T5_4_0 = 23



def select_fgts_older_than_month(fgts):

    select_fgts = []

    deadline = datetime.now() - timedelta(days=60)

    for fgt in fgts:
        if fgt < deadline:
            select_fgts.append(fgt)

    return select_fgts


def select_fgts_within_a_range(fgts, start, end):

    select_fgts = []

    for fgt in fgts:
        if end >= fgt >= start:
            select_fgts.append(fgt)

    return select_fgts


def flush_timeseries_outdated(pool, hash_ids):

    TS = Timeseries(pool=pool)

    ###################################################################################
    # delete a specific timeseries defined by a given hash id and fgt from data table #
    ###################################################################################
    count = 0
    for id in hash_ids:
        fgts = get_distinct_fgts_for_given_id(pool=pool, id_=id)

        if fgts is not None:
            outdated_fgts = select_fgts_older_than_month(fgts)
            count += 1

            if outdated_fgts is not None:
                for fgt in outdated_fgts:
                    fgts.remove(fgt)
                    TS.delete_timeseries(id_=id, fgt=fgt)
                    print(count, id, fgt)

                TS.update_start_date(id_=id, start_date=min(fgts), force=True)

    print("{} of hash ids are deleted.".format(count))


def flush_timeseries_given_range(pool, hash_ids, start, end):

    TS = Timeseries(pool=pool)

    ###################################################################################
    # delete a specific timeseries defined by a given hash id and fgt from data table #
    ###################################################################################
    count = 0
    for id in hash_ids:
        fgts = get_distinct_fgts_for_given_id(pool=pool, id_=id)

        if fgts is not None:
            in_range_fgts = select_fgts_within_a_range(fgts, start, end)
            count += 1

            if in_range_fgts is not None:
                for fgt in in_range_fgts:
                    fgts.remove(fgt)
                    TS.delete_timeseries(id_=id, fgt=fgt)
                    print(count, id, fgt)

                TS.update_start_date(id_=id, start_date=min(fgts), force=True)

    print("{} of hash ids are deleted.".format(count))


"""
gfs_d0_00      
gfs_d0_18  
dwrf_gfs_d1_06    @22
dwrf_gfs_d1_12    @02
dwrf_gfs_d1_18    @09
dwrf_gfs_d1_00    @17
mwrf_gfs_d0_18    @02
"""

if __name__=="__main__":

    try:

        set_db_config_file_path('/home/uwcc-admin/curw_fcst_db_utils/db_adapter_config.json')

        pool = get_Pool(host=connection.CURW_FCST_HOST, port=connection.CURW_FCST_PORT, user=connection.CURW_FCST_USERNAME,
                        password=connection.CURW_FCST_PASSWORD, db=connection.CURW_FCST_DATABASE)

        # pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT,
        #                 user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD, db=CURW_FCST_DATABASE)


        sim_tag_list = ["dwrf_gfs_d1_06", "dwrf_gfs_d1_12", "dwrf_gfs_d1_18", "dwrf_gfs_d1_00",
                        "mwrf_gfs_d0_18",
                        "gfs_d0_00", "gfs_d0_18"]

        for sim_tag in sim_tag_list:
            print("sim_tag:", sim_tag)
            wrf_hash_ids = get_curw_fcst_hash_ids(pool=pool, sim_tag=sim_tag, source_id=None,
                                                    variable_id=None, unit_id=None, station_id=None,
                                                    start=None, end=None)

            flush_timeseries_outdated(pool=pool, hash_ids=wrf_hash_ids)


    except Exception as e:
        print('An exception occurred.')
        traceback.print_exc()
    finally:
        print("Process finished")
        destroy_Pool(pool=pool)
