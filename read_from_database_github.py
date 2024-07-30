import ksycopg2
import os
import re

import pandas as pd


class read_data:
    def __init__(self, farm_type, farm_name, database_data, database_name, user, password, host, port):
        self.farm_code = farm_name
        self.farm_type = farm_type
        self.database_data = database_data
        self.database_name = database_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def read_data_from_EMS_and_HISDB(self):


        conn_HISDB = ksycopg2.connect(database=self.database_data, user=self.user, password=self.password,
                                      host=self.host,
                                      port=self.port)
        conn_EMS = ksycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host,
                                    port=self.port)
        cur_HISDB_RTLOAD = conn_HISDB.cursor()
        cur_HISDB_RTTOWER = conn_HISDB.cursor()
        cur_EMS_WINDSTATION = conn_EMS.cursor()
        cur_EMS_WINDTOWER = conn_EMS.cursor()

        EMS_executor_WINDSTATION = f'''
        SELECT WINDFARM_CODE, WINDFARM_NAME, LONGITUDE, LAITITUDE, RATED_POWER
        FROM WPFS_EMSDB_SX.WP_CFG_WINDSTATION 
        WHERE SUBSTRING("WINDFARM_CODE",3,3)='{self.farm_code}' 
        AND SUBSTRING("WINDFARM_CODE",1,1)='{self.farm_type}'
        '''

        EMS_executor_WINDTOWER = f'''
        SELECT TOWER_CODE, TOWER_NAME, WINFARM_CODE
        FROM WPFS_EMSDB_SX.WP_CFG_WINDTOWER
        WHERE SUBSTRING("WINFARM_CODE",3,3)='{self.farm_code}'
        AND SUBSTRING("WINFARM_CODE",1,1)='{self.farm_type}'
        '''

        HISDB_RTLOAD_executor = f'''
        SELECT * 
        FROM WPFS_HISDB.WP_RUN_RTLOAD 
        WHERE SUBSTRING("WINDFARM_CODE",3,3)='{self.farm_code}' 
        AND SUBSTRING("WINDFARM_CODE",1,1)='{self.farm_type}'
        '''

        HISDB_RTTOWER_executor = f'''
        SELECT 
        "TOWER_CODE","TIME","LAYER",CAST("TYPE" AS DECIMAL) AS TYPE_CODE,"POINT1","POINT2","POINT3","POINT4","POINT5","POINT6","POINT7","POINT8","POINT9","POINT10","POINT11","POINT12","POINT13","POINT14","POINT15","POINT16","POINT17","POINT18","POINT19","POINT20","POINT21","POINT22","POINT23","POINT24","POINT25","POINT26","POINT27","POINT28","POINT29","POINT30","POINT31","POINT32","POINT33","POINT34","POINT35","POINT36","POINT37","POINT38","POINT39","POINT40","POINT41","POINT42","POINT43","POINT44","POINT45","POINT46","POINT47","POINT48","POINT49","POINT50","POINT51","POINT52","POINT53","POINT54","POINT55","POINT56","POINT57","POINT58","POINT59","POINT60","POINT61","POINT62","POINT63","POINT64","POINT65","POINT66","POINT67","POINT68","POINT69","POINT70","POINT71","POINT72","POINT73","POINT74","POINT75","POINT76","POINT77","POINT78","POINT79","POINT80","POINT81","POINT82","POINT83","POINT84","POINT85","POINT86","POINT87","POINT88","POINT89","POINT90","POINT91","POINT92","POINT93","POINT94","POINT95","POINT96","POINT97","POINT98","POINT99","POINT100","POINT101","POINT102","POINT103","POINT104","POINT105","POINT106","POINT107","POINT108","POINT109","POINT110","POINT111","POINT112","POINT113","POINT114","POINT115","POINT116","POINT117","POINT118","POINT119","POINT120","POINT121","POINT122","POINT123","POINT124","POINT125","POINT126","POINT127","POINT128","POINT129","POINT130","POINT131","POINT132","POINT133","POINT134","POINT135","POINT136","POINT137","POINT138","POINT139","POINT140","POINT141","POINT142","POINT143","POINT144","POINT145","POINT146","POINT147","POINT148","POINT149","POINT150","POINT151","POINT152","POINT153","POINT154","POINT155","POINT156","POINT157","POINT158","POINT159","POINT160","POINT161","POINT162","POINT163","POINT164","POINT165","POINT166","POINT167","POINT168","POINT169","POINT170","POINT171","POINT172","POINT173","POINT174","POINT175","POINT176","POINT177","POINT178","POINT179","POINT180","POINT181","POINT182","POINT183","POINT184","POINT185","POINT186","POINT187","POINT188","POINT189","POINT190","POINT191","POINT192","POINT193","POINT194","POINT195","POINT196","POINT197","POINT198","POINT199","POINT200","POINT201","POINT202","POINT203","POINT204","POINT205","POINT206","POINT207","POINT208","POINT209","POINT210","POINT211","POINT212","POINT213","POINT214","POINT215","POINT216","POINT217","POINT218","POINT219","POINT220","POINT221","POINT222","POINT223","POINT224","POINT225","POINT226","POINT227","POINT228","POINT229","POINT230","POINT231","POINT232","POINT233","POINT234","POINT235","POINT236","POINT237","POINT238","POINT239","POINT240","POINT241","POINT242","POINT243","POINT244","POINT245","POINT246","POINT247","POINT248","POINT249","POINT250","POINT251","POINT252","POINT253","POINT254","POINT255","POINT256","POINT257","POINT258","POINT259","POINT260","POINT261","POINT262","POINT263","POINT264","POINT265","POINT266","POINT267","POINT268","POINT269","POINT270","POINT271","POINT272","POINT273","POINT274","POINT275","POINT276","POINT277","POINT278","POINT279","POINT280","POINT281","POINT282","POINT283","POINT284","POINT285","POINT286","POINT287","POINT288" 
        FROM WPFS_HISDB.WP_RUN_RTTOWER
        WHERE 
            SUBSTRING("TOWER_CODE",3,3)='{self.farm_code}' 
            AND SUBSTRING("TOWER_CODE",1,1)='{self.farm_type}'
        '''

        cur_EMS_WINDTOWER.execute(EMS_executor_WINDTOWER)
        if len(cur_EMS_WINDTOWER.fetchall()) < 1:
            print('场站不存在与WIND_TOWER中')
            return [], [], [], [], []
        else:
            print('场站存在')


        cur_EMS_WINDSTATION.execute(EMS_executor_WINDSTATION)
        # 获取表名，经纬度,装机容量
        row_code_name_longitude_lattitude = cur_EMS_WINDSTATION.fetchall()

        if len(row_code_name_longitude_lattitude) == 0:
            print('')
            print('error: 不存在这个站点，或错误的场站编码')
            return [], [], [], [], []
        else:
            pass

        columns_code_name_longitude_lattitude = [desc[0] for desc in cur_EMS_WINDSTATION.description]
        # 将查询结果转换为Pandas DataFrame
        df_code_name_longitude_lattitude = pd.DataFrame(row_code_name_longitude_lattitude,
                                                        columns=columns_code_name_longitude_lattitude, dtype=object)
        column_name = df_code_name_longitude_lattitude.loc[0, 'WINDFARM_NAME']
        clip = r'(分布)'
        match = re.search(clip, column_name)
        if match:
            print(f'{column_name}为分布式场站，不做处理')
            return [], [], [], [], []
        else:
            pass



        try:
            print('正在读取')
            cur_EMS_WINDTOWER.execute(EMS_executor_WINDTOWER)
            cur_HISDB_RTTOWER.execute(HISDB_RTTOWER_executor)
            cur_HISDB_RTLOAD.execute(HISDB_RTLOAD_executor)
            print('数据读取完成')

        except:
            print('不存在这个站点，或程序出现异常')
            return [], [], [], [], []







        # 获取功率
        row_power = cur_HISDB_RTLOAD.fetchall()
        columns_power = [desc[0] for desc in cur_HISDB_RTLOAD.description]
        df_power = pd.DataFrame(row_power, columns=columns_power, dtype=object)
        if len(df_power) < 30:
            print('RTLOAD/功率数据缺失')
            return [], [], [], [], []
        else:
            print(f'电厂编号，功率已读取')
        # 获取特征TYPE
        row_tower = cur_HISDB_RTTOWER.fetchall()
        columns_tower = [desc[0] for desc in cur_HISDB_RTTOWER.description]
        df_tower = pd.DataFrame(row_tower, columns=columns_tower, dtype=object)
        if len(df_tower) < 30:
            print('RTTOWER/特征数据缺失')
            return [], [], [], [], []
        else:
            print(f'电厂编号，特征TYPE已读取')

        # 获取测风塔编号
        row_tower_code = cur_EMS_WINDTOWER.fetchall()
        columns_tower_code = [desc[0] for desc in cur_EMS_WINDTOWER.description]
        df_tower_code = pd.DataFrame(row_tower_code, columns=columns_tower_code, dtype=object)

        if self.farm_type == '5':
            print(f'测风塔编号已读取')
        else:
            pass

        unique_tower_code = df_tower_code['TOWER_CODE'].unique()
        dfs = {}

        if self.farm_type == '5':
            if len(unique_tower_code) > 1:
                for code_type in unique_tower_code:
                    dfs[f'{code_type}'] = df_tower[df_tower['TOWER_CODE'] == f'{code_type}']
                use_fea_data = max(dfs.values(), key=len)
                for code_type, df in dfs.items():
                    if df is use_fea_data:
                        print(f'测风塔数量有{len(unique_tower_code)}个，已选择{code_type}')
                        break

            elif len(unique_tower_code) == 1:
                column_code = df_tower_code.loc[0, 'TOWER_CODE']
                use_fea_data = df_tower[df_tower['TOWER_CODE'] == f'{column_code}']
                print('测风塔数量有1个')
            else:
                print('RTTOWER 数据缺失')
                return [], [], [], [], []


        elif self.farm_type == '7':
            if len(unique_tower_code) > 1:
                print('error,光伏存在多个RTTOWER数据')
                return [], [], [], [], []
            else:
                column_code = df_tower_code.loc[0, 'TOWER_CODE']
                use_fea_data = df_tower[df_tower['TOWER_CODE'] == f'{column_code}']
        else:
            print('未输入正确的电厂类型')

        if len(use_fea_data) < 30:
            print('fea_data数据缺失')
            return [], [], [], [], []
        else:
            print('已选择特征数据集')
            print('')


        cur_EMS_WINDSTATION.close()
        cur_EMS_WINDTOWER.close()
        conn_EMS.close()
        cur_HISDB_RTLOAD.close()
        cur_HISDB_RTTOWER.close()
        conn_HISDB.close()
        cap = df_code_name_longitude_lattitude.loc[0, 'RATED_POWER']
        cap = round(cap, 2)
        return df_code_name_longitude_lattitude, df_power, use_fea_data, df_tower_code, cap
class read_data_from_root:
    def __init__(self, farm_type, farm_name):


        self.type = farm_type
        self.name = farm_name

    def get_filename(self):
        if self.type == '5':
            dir_1 = 'D:/pythonProject_work/NARI_prediction_work/场站/可用日期风电数据'
            filenames = [os.path.join(dir_1, filename) for filename in os.listdir(dir_1)]
        elif self.type == '7':
            dir_2 = 'D:/pythonProject_work/NARI_prediction_work/场站/可用日期光伏数据'
            filenames = [os.path.join(dir_2, filename) for filename in os.listdir(dir_2)]

        return filenames

    def search_file(self):
        all_file = self.get_filename()
        match_file = [filename for filename in all_file if re.search(r'({type}.{name})'.format(type=self.type, name=self.name), filename)]

        if match_file:
            data = pd.read_csv(match_file[0])
            cap = match_file[0].split('cap=')[1].split('.csv')[0]
            return data, cap
        else:
            return [], []

    def get_data_name(self):

        conn_EMS = ksycopg2.connect(database='EMS', user='SYSTEM', password='a', host='172.16.160.130',
                                    port='54321')

        cur_EMS_WINDSTATION = conn_EMS.cursor()

        EMS_executor_WINDSTATION = f'''
        SELECT WINDFARM_CODE, WINDFARM_NAME, LONGITUDE, LAITITUDE, RATED_POWER
        FROM WPFS_EMSDB_SX.WP_CFG_WINDSTATION 
        WHERE SUBSTRING("WINDFARM_CODE",3,3)='{self.name}' 
        AND SUBSTRING("WINDFARM_CODE",1,1)='{self.type}'
        '''
        cur_EMS_WINDSTATION.execute(EMS_executor_WINDSTATION)
        # 获取表名，经纬度,装机容量
        row_code_name_longitude_lattitude = cur_EMS_WINDSTATION.fetchall()

        if len(row_code_name_longitude_lattitude) == 0:
            print('')
            print('error: 不存在这个站点，或错误的场站编码')
            return [], [], [], [], []
        else:
            pass

        columns_code_name_longitude_lattitude = [desc[0] for desc in cur_EMS_WINDSTATION.description]
        # 将查询结果转换为Pandas DataFrame
        df_code_name_longitude_lattitude = pd.DataFrame(row_code_name_longitude_lattitude,
                                                        columns=columns_code_name_longitude_lattitude, dtype=object)

        conn_EMS.close()
        cur_EMS_WINDSTATION.close()

        return df_code_name_longitude_lattitude