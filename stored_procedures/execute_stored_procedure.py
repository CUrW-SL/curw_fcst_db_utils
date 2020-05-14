#!/home/uwcc-admin/curw_fcst_db_utils/venv/bin/python3

import traceback, os, csv

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params

ROOT_DIR = "/home/uwcc-admin/curw_fcst_db_utils"
output_file_dir = "/mnt/disks/curwsl_nfs/data/Nov2010Event/flo2d_150/with_interventions"


def create_csv(file_name, data):
    """
    Create new csv file using given data
    :param file_name: <file_path/file_name>.csv
    :param data: list of lists
    e.g. [['Person', 'Age'], ['Peter', '22'], ['Jasmine', '21'], ['Sam', '24']]
    :return:
    """
    with open(file_name, 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(data)


if __name__=="__main__":

    set_db_config_file_path(os.path.join(ROOT_DIR, 'db_adapter_config.json'))


    try:
        curw_fcst_pool = get_Pool(host=con_params.CURW_FCST_HOST, user=con_params.CURW_FCST_USERNAME,
                                 password=con_params.CURW_FCST_PASSWORD,
                                 port=con_params.CURW_FCST_PORT, db=con_params.CURW_FCST_DATABASE)

        procedure_inputs = [['510', 3, "event_2010_Nov_with_intervention", "2020-05-14 13:17:57"],
                            ['581', 3, "event_2010_Nov_with_intervention", "2020-05-14 13:17:57"],
                            ['651', 3, "event_2010_Nov_with_intervention", "2020-05-14 13:17:57"],
                            ['2561', 3, "event_2010_Nov_with_intervention", "2020-05-14 13:17:57"],
                            ['10195', 3, "event_2010_Nov_with_intervention", "2020-05-14 13:17:57"],
                            ['1782', 3, "event_2010_Nov_with_intervention", "2020-05-14 13:17:57"]]
                            # [['1782', 3, "event_2010_Nov", "2020-05-14 13:17:57"],
                            # ['10195', 3, "event_2010_Nov", "2020-05-14 13:17:57"],
                            # ['2561', 3, "event_2010_Nov", "2020-05-14 13:17:57"],
                            # ['651', 3, "event_2010_Nov", "2020-05-14 13:17:57"],
                            # ['581', 3, "event_2010_Nov", "2020-05-14 13:17:57"],
                            # ['510', 3, "event_2010_Nov", "2020-05-14 13:17:57"]]

        output_file_names = ["wellawatta_canal", "dehiwala_canal", "mutwal_outfall", "kittampahuwa", "ambatale_outfall", "nagalagam_street"]

        procedure_output = [['time', 'value']]

        connection = curw_fcst_pool.connection()

        for i in range(len(procedure_inputs)):

            with connection.cursor() as cursor1:
                cursor1.callproc('get_150_fcsts_for_given_grid', tuple(procedure_inputs[i]))
                results = cursor1.fetchall()

                for result in results:
                    procedure_output.append([result.get('time'), result.get('value')])

            file_name= "flo2d_150_discharge_{}.cdv".format(output_file_names[i])

            create_csv(os.path.join(output_file_dir, file_name), procedure_output)


    except Exception as e:
        print('An exception occurred.')
        traceback.print_exc()
    finally:
        print("Process finished")
        destroy_Pool(pool=curw_fcst_pool)