import os.path

from read_data_from_kingbase import read_data
import ksycopg2
import pandas as pd
import re

class save_usetime_data(read_data):
    def __init__(self, farm_type, farm_name, database_data, database_name, user, password, host, port):
        super().__init__(farm_type, farm_name, database_data, database_name, user, password, host, port)


    def read_all_station(self):
        conn_EMS = ksycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host, port=self.port)
        cur_EMS_WINDTOWER = conn_EMS.cursor()

        EMS_executor_WINDTOWER = f'''
        SELECT TOWER_NAME, WINFARM_CODE
        FROM WPFS_EMSDB_SX.WP_CFG_WINDTOWER
        '''

        cur_EMS_WINDTOWER.execute(EMS_executor_WINDTOWER)
        row_tower_code = cur_EMS_WINDTOWER.fetchall()
        columns_tower_code = [desc[0] for desc in cur_EMS_WINDTOWER.description]
        df_tower_code = pd.DataFrame(row_tower_code, columns=columns_tower_code, dtype=object)

        conn_EMS.close()
        cur_EMS_WINDTOWER.close()

        return df_tower_code
    def filter(self):
        df_tower_code_ori = self.read_all_station()
        df_tower_code = df_tower_code_ori.copy(deep=True)
        df_tower_code['mask'] = 0
        df_tower_code.reset_index(drop=True, inplace=True)
        for index in df_tower_code.index:

            patten = r'(分布式)'
            match = re.search(patten, df_tower_code.loc[index, 'TOWER_NAME'])
            if match:
                df_tower_code.loc[index, 'mask'] = 1
            else:
                pass
        df_tower_code.drop(df_tower_code[df_tower_code['mask'] == 1].index, inplace=True)
        df_tower_code.reset_index(drop=True, inplace=True)
        return df_tower_code

    def save_all_station(self):
        station = self.filter()
        unique_station = station['WINFARM_CODE'].unique()
        unique_station = pd.DataFrame(unique_station)
        unique_station = unique_station.sort_values(by=unique_station.columns[0])
        unique_station.reset_index(drop=True, inplace=True)
        dir = 'D:/pythonProject_work/NARI_prediction_work'
        filename = '所有场站编号'
        path = os.path.join(dir, filename)
        unique_station.to_csv(path, index=True)
        print('所有场站已读取')
        return unique_station
