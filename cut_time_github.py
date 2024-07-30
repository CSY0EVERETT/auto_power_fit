import os
import matplotlib.pyplot as plt
import pandas as pd
from django.utils.text import slugify
from datetime import datetime
class cut_time:
    def __init__(self, data, farm_type, data_name, cap):
        self.data = data
        self.type = farm_type
        self.cap = cap
        self.data_name = data_name
    def generate_polt(self):
        self.data['Time'] = pd.to_datetime(self.data['Time'])
        plt.figure(figsize=(30, 8))
        plt.plot(self.data['Time'], self.data['rtpower'], c='b')
        plt.xticks(self.data['Time'][::4320])
        plt.xticks(rotation=90)
        plt.show()
        plt.close()
        # plt.ioff()
        return 1
    def cut(self):
        date = input('开始的日期为：')
        # date = '2020-01-01'
        date = datetime.strptime(date, '%Y-%m-%d')
        # date = pd.to_datetime(date)
        data_copy = self.data.copy(deep=True)
        data_copy['Time'] = pd.to_datetime(data_copy['Time'])
        data_cut = data_copy.loc[data_copy['Time'] >= date, :]
        data_cut.reset_index(drop=True, inplace=True)
        return data_cut

    def save_cut_data_to_csv(self):
        self.generate_polt()
        data_cut = self.cut()
        column_name = self.data_name.loc[0, 'WINDFARM_NAME']
        column_name_ = slugify(column_name, allow_unicode=True)
        farm_code = self.data_name.loc[0, 'WINDFARM_CODE']
        farm_code_ = slugify(farm_code)

        file_name = f'{column_name_}_{farm_code_}_cap={self.cap}.csv'

        if self.type == '5':
            dir_1 = 'D:/pythonProject_work/NARI_prediction_work/场站/可用日期风电数据'
            path_1 = os.path.join(dir_1, file_name)
            if os.path.exists(dir_1):
                pass
            else:
                os.makedirs(dir_1)
            data_cut.to_csv(path_1, index=False)
        elif self.type == '7':
            dir_2 = 'D:/pythonProject_work/NARI_prediction_work/场站/可用日期光伏数据'
            path_2 = os.path.join(dir_2, file_name)
            if os.path.exists(dir_2):
                pass
            else:
                os.makedirs(dir_2)
            data_cut.to_csv(path_2, index=False)

        print(f'已保存 {column_name_} 截取后数据的csv文件')
