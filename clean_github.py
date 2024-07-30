import pandas as pd
import os.path
from django.utils.text import slugify
import matplotlib.pyplot as plt

class clean_data:

    def __init__(self, data, fea, label, Time, cap, farm_type, farm_name):

        self.data = data
        # self.train_data = data.iloc[:int(len(data)*0.8), :]
        self.fea = fea
        self.label = label
        self.Time = Time
        # self.test_data = data.iloc[len(self.train_data):, :]
        self.cap = cap
        self.type = farm_type
        self.name = farm_name
    def clean_null(self):
        data_copy = self.data.copy(deep=True)
        data_copy.drop(self.data[self.data.isnull().any(axis=1)].index, inplace=True)
        data_copy.reset_index()
        return data_copy

    def clean_over_limit_fd_data(self, data_copy):
        if self.type == '5':



            data_copy.reset_index(drop=True, inplace=True)
        elif self.type == '7':



            data_copy.reset_index(drop=True, inplace=True)

        return data_copy

    def clean_constant(self, data_copy):
        if self.type == '5':

        elif self.type == '7':
            data_copy = data_copy
        return data_copy

    def clean_alldayzero(self, data_copy):

        data_copy['mask'] = 0
        data_copy['Time'] = pd.to_datetime(data_copy['Time'])
        data_copy.set_index('Time', inplace=True)

        return data_copy

    def save_clean_pic(self, data_name, data_copy):
        column_name = data_name.loc[0, 'WINDFARM_NAME']
        column_name_ = slugify(column_name, allow_unicode=True)
        farm_code = data_name.loc[0, 'WINDFARM_CODE']
        farm_code_ = slugify(farm_code)

        file_name = f'{column_name_}_{farm_code_}_cap={self.cap}.csv'
        clean_filename = f'清洗后的'

        if self.type == '5':
            dir_1 = 'D:/pythonProject_work/NARI_prediction_work/场站/风电站'
            path_1 = os.path.join(dir_1, file_name)
            path_1_pic = os.path.join(path_1, clean_filename)
            if os.path.exists(path_1):
                pass
            else:
                os.makedirs(path_1)
            plt.figure(figsize=(40, 20))
            plt.scatter(data_copy[self.fea], data_copy[self.label], c='b')
            plt.title(f'{farm_code_}')
            plt.savefig(f'{path_1_pic}散点图')

            plt.figure(figsize=(80, 10))
            plt.plot(data_copy['Time'], data_copy[self.label], c='b')
            plt.title(f'{farm_code_}')
            plt.savefig(f'{path_1_pic}折线图')

            plt.close()
        elif self.type == '7':
            dir_2 = 'D:/pythonProject_work/NARI_prediction_work/场站/光伏电站'
            path_2 = os.path.join(dir_2, file_name)
            path_2_pic = os.path.join(path_2, clean_filename)
            if os.path.exists(path_2):
                pass
            else:
                os.makedirs(path_2)
            plt.figure(figsize=(40, 20))
            plt.scatter(data_copy[self.fea], data_copy[self.label], c='b')
            plt.title(f'{farm_code_}')
            plt.savefig(f'{path_2_pic}散点图')

            plt.figure(figsize=(80, 10))
            plt.plot(data_copy['Time'], data_copy[self.label], c='b')
            plt.title(f'{farm_code_}')
            plt.savefig(f'{path_2_pic}折线图')


        print(f'已生成 {column_name_} 清洗后数据的散点和折线图')
    def clean_all(self):

        data_copy = self.clean_null()
        data_copy = self.clean_over_limit_fd_data(data_copy)
        data_copy = self.clean_constant(data_copy)
        data_cleaned = self.clean_alldayzero(data_copy)

        return data_cleaned


    # def clean_power_limit(self):
