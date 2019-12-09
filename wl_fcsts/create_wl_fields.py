#!/home/uwcc-admin/curw_fcst_db_utils/venv/bin/python3
import traceback
import os
import csv
from datetime import datetime, timedelta

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.constants import CURW_FCST_PASSWORD, CURW_FCST_USERNAME, CURW_FCST_HOST, CURW_FCST_PORT, \
    CURW_FCST_DATABASE
from db_adapter.curw_fcst.station import StationEnum
from db_adapter.curw_fcst.station import get_flo2d_output_stations

MODEL = 'FLO2D'
VERSION = '250'
SIM_TAG = "hourly_run"
STATION_TYPE = StationEnum.FLO2D_250
wl_series_home = '/home/uwcc-admin/flo2d_wl_series/250'
bucket_wl_series_home = '/mnt/disks/wrf_nfs/flo2d_wl_series/250'


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


def create_csv(file_name, data):
    with open(file_name, 'w+') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(data)


def create_wl_series(connection, lat, lon, station_name, station_id, expected_fgt, start_time):
    # wl_field = [['time', 'value']]
    wl_field = [['time', 'value']]
    with connection.cursor() as cursor0:
        cursor0.callproc('getNearestFcstWLforGivenStation', (MODEL, VERSION, station_id, SIM_TAG, expected_fgt, start_time))
        results = cursor0.fetchall()
        for result in results:
            wl_field.append([result.get('time'), result.get('value')])

    create_csv('{}/{}_{}_{}_wl_series.csv'.format(wl_series_home, lat, lon, station_name), wl_field)


def gen_flo2d_wl_series():

    expected_fgt = (datetime.now() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:00:00")
    timestamp = (datetime.strptime(expected_fgt, COMMON_DATE_TIME_FORMAT)).strftime('%Y-%m-%d_%H-%M')
    start_time = (datetime.now() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d 00:00:00")

    # remove outdated wl series files
    try:
        os.system("rm -f {}/*".format(wl_series_home))
    except Exception as e:
        traceback.print_exc()

    # Connect to the database
    curw_fcst_pool = get_Pool(host=CURW_FCST_HOST, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                              port=CURW_FCST_PORT, db=CURW_FCST_DATABASE)
    connection = curw_fcst_pool.connection()

    flo2d_stations = get_flo2d_output_stations(pool=curw_fcst_pool, flo2d_model=STATION_TYPE)

    try:
        for station in flo2d_stations.keys():
            station_name = flo2d_stations.get(station)[3]
            lat = flo2d_stations.get(station)[1]
            lon = flo2d_stations.get(station)[2]
            station_id = flo2d_stations.get(station)[0]

            create_wl_series(connection=connection, lat=lat, lon=lon, station_name=station_name, station_id=station_id,
                             expected_fgt=expected_fgt, start_time=start_time)

        return timestamp
    except Exception as ex:
        traceback.print_exc()
        return False
    finally:
        connection.close()
        destroy_Pool(pool=curw_fcst_pool)
        print("Process finished")


# def usage():
#     usageText = """
#     Usage: ./gen_SL_d03_rfield.py -m WRF_X1,WRF_X2,WRF_X3 -v vX -s "evening_18hrs"
#
#     -h  --help          Show usage
#     -m  --model         List of WRF models (e.g. WRF_A, WRF_E). Compulsory arg
#     -v  --version       WRF model version (e.g. v4, v3). Compulsory arg
#     -s  --sim_tag       Simulation tag (e.g. evening_18hrs). Compulsory arg
#     -f  --fgt           fgt pattern (e.g."2019-09-10%"). Default is today (UTC+5:30)
#     """
#     print(usageText)


if __name__=="__main__":

    try:
        # wrf_models = None
        # version = None
        # sim_tag = None
        # fgt = '{}%'.format((datetime.now() + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d'))
        #
        # try:
        #     opts, args = getopt.getopt(sys.argv[1:], "h:m:v:s:f:",
        #             ["help", "wrf_model=", "version=", "sim_tag=", "fgt="])
        # except getopt.GetoptError:
        #     usage()
        #     sys.exit(2)
        # for opt, arg in opts:
        #     if opt in ("-h", "--help"):
        #         usage()
        #         sys.exit()
        #     elif opt in ("-m", "--wrf_model"):
        #         wrf_models = arg.strip()
        #     elif opt in ("-v", "--version"):
        #         version = arg.strip()
        #     elif opt in ("-s", "--sim_tag"):
        #         sim_tag = arg.strip()
        #     elif opt in ("-f", "--fgt"):
        #         fgt = arg.strip()
        #
        # wrf_model_list = wrf_models.split(',')
        #
        # for wrf_model in wrf_model_list:
        #     if wrf_model is None or wrf_model not in VALID_MODELS:
        #         usage()
        #         exit(1)
        # if version is None or version not in VALID_VERSIONS:
        #     usage()
        #     exit(1)
        # if sim_tag is None or sim_tag not in SIM_TAGS:
        #     usage()
        #     exit(1)
        #
        # sim_tag_parts = re.findall(r'\d+', sim_tag)
        # gfs_run = "d{}".format(sim_tag_parts[0])
        # gfs_data_hour = sim_tag_parts[1]
        # day = fgt.split("%")[0]
        #
        # if sim_tag.split("_")[0] == 'dwrf':
        #     rfield_home = "{}/dwrf/{}/{}/{}/d03/rfield".format(root_directory, version, gfs_run, gfs_data_hour)
        #     bucket_rfield_home = "{}/dwrf/{}/{}/{}/{}/rfield/d03".format(bucket_root, version, gfs_run, gfs_data_hour,
        #                                                                 day)
        # else:
        #     rfield_home = "{}/wrf/{}/{}/{}/d03/rfield".format(root_directory, version, gfs_run, gfs_data_hour)
        #     bucket_rfield_home = "{}/wrf/{}/{}/{}/{}/rfield/d03".format(bucket_root, version, gfs_run, gfs_data_hour,
        #                                                                 day)

        try:
            os.makedirs(wl_series_home)
        except FileExistsError:
            # directory already exists
            pass

        try:
            os.makedirs(bucket_wl_series_home)
        except FileExistsError:
            # directory already exists
            pass

        timestamp = gen_flo2d_wl_series()

        os.system("tar -C {} -czf {}/{}.tar.gz 250".format('/250'.join(wl_series_home.split('/250')[:-1]),
                                                           bucket_wl_series_home, timestamp))

    except Exception as e:
        print('JSON config data loading error.')
        traceback.print_exc()

