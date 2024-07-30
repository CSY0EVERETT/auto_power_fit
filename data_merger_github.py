import os.path
import pandas as pd
from tqdm import tqdm
from django.utils.text import slugify
class data_merge:
    def __init__(self, type, data_RTLOAD, data_RTTOWER, cap):
        self.data_power = data_RTLOAD
        self.data_fea = data_RTTOWER
        self.type = type
        self.cap = cap

    # 创建时间戳。每五分钟为间隔，每天共288个，与power，feature对齐
    def create_timestampe(self, data_time):
        dates = data_time.unique()
        all_times = []

        for date in dates:
            times = pd.date_range(start=f"{date} 00:00:00",
                                  end=f"{date} 23:55:00",
                                  freq="5min")
            all_times.extend(times)

        timestamp = pd.DataFrame({'TIME': all_times})
        return timestamp

    #  准备功率表
    def power_data_pre(self):
        data = self.data_power.sort_values(by='TIME')
        data_drop = data.reset_index(drop=True)
        data_set_index = data_drop.fillna(-99).infer_objects(copy=False)

        return data_set_index

    #  准备特征表
    def fea_data_pre(self, data):
        data = data.sort_values(by='TIME')
        data_drop = data.reset_index(drop=True)
        data_set_index = data_drop.fillna(-99).infer_objects(copy=False)
        # fea_data_pre = data_set_index.loc[:, 'POINT1':]
        return data_set_index

    #  创建功率表
    def create_table_power(self):

        power_data_pred = self.power_data_pre()
        power_data_pre = power_data_pred.set_index('TIME')
        time_index = power_data_pre.index
        power = []
        Time = []
        for index_1 in time_index:


        return new_power

    #  创建特征表
    def create_table(self):
        param_dic = {
            0: 'temp',
            1: 'hum',
            2: 'press',
            3: 'speed',
            4: 'dir',
            5: 'rad'
        }

        print('功率数据已准备')
        a = 0

        #for fea in feature:

        for fea in tqdm(feature, desc='数据正在生成', colour='green', bar_format='{l_bar}{bar:50}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'):
\
        return data_merger


    def create_folder(self, data_name, result):
        column_name = data_name.loc[0, 'WINDFARM_NAME']
        column_name_ = slugify(column_name, allow_unicode=True)
        farm_code = data_name.loc[0, 'WINDFARM_CODE']
        farm_code_ = slugify(farm_code)
        # cap = slugify(self.cap, allow_unicode=True)
        # timestamp = int(time.time())

        file_name = f'{column_name_}_{farm_code_}_cap={self.cap}.csv'


        if self.type == '5':
            dir_1 = 'D:/pythonProject_work/NARI_prediction_work/场站/风电原始数据集合'
            path_1 = os.path.join(dir_1, file_name)
            if os.path.exists(path_1):
                pass
            else:
                result.to_csv(path_1, index=False)
        elif self.type == '7':
            dir_2 = 'D:/pythonProject_work/NARI_prediction_work/场站/光伏原始数据集合'
            path_2 = os.path.join(dir_2, file_name)
            if os.path.exists(path_2):
                pass
            else:
                result.to_csv(path_2, index=False)
        print(f'{column_name_} csv文件已存储到本地')













    # def merge(self, table1, table2):
    #     result_data = pd.merge(table1, table2, on='Time', how='inner')
    #     return result_data
    #
    # def merge_all(self):
    #     fea_data = self.create_table_fea()
    #     power_data = self.create_table_power()
    #     result_data = self.merge(power_data, fea_data)
    #     return result_data





