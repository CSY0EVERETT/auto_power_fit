from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt
import time
import pandas as pd
import os
from django.utils.text import slugify

class multi_cluster:
    def __init__(self,
                 data, feature, label, eps_min, eps_max, min_samples_min, min_samples_max,
                 eps_step, samples_step, farm_type, cap, cluster=True
                 ):
        self.ori_data = data
        self.fea = feature
        self.label = label
        self.cluster_eps_min = eps_min
        self.cluster_eps_max = eps_max
        self.cluster_min_samples_min = min_samples_min
        self.cluster_min_samples_max = min_samples_max
        self.eps_step = eps_step
        self.min_samples_step = samples_step
        self.cluster = cluster
        self.type = farm_type
        self.cap = cap
    #############################################
    # 获取半径eps的取值区间
    def get_eps_range(self):
        eps_range = []
        eps = self.cluster_eps_min
        while eps <= self.cluster_eps_max:
            eps_range.append(eps)
            eps += self.eps_step

        return eps_range

    #############################################
    # 获取最小样本量 min_samples 的取值区间
    def get_min_samples_range(self):
        min_samples_range = []
        min_samples = self.cluster_min_samples_min
        while min_samples <= self.cluster_min_samples_max:
            min_samples_range.append(min_samples)
            min_samples += self.min_samples_step

        return min_samples_range

    #############################################
    # 自动聚类函数，输入量依次为，data全部数据；feature聚类输入特征，风电一般为speed，光伏一般为rad；
    # eps_min为最小聚类半径，eps_max为最大聚类半径；min_samples_min为最小的最小样本数，min_samples_max为最大的最小样本数。
    def autom_cluster(self, min_batch_num):
        if self.cluster:
            #  拷贝源数据
            data_for_clip = self.ori_data.sort_values(by=self.fea)
            data_mo = self.ori_data.copy(deep=True)
            #  获得半径的值集合
            eps_range = multi_cluster.get_eps_range(self)
            #  获得最小样本量的值集合
            min_samples_range = multi_cluster.get_min_samples_range(self)
            #  聚类目标数据

            a = 0
            del_data_sum = 0
            # 有效的聚类参数
            useful_params = []
            #  总聚类次数
            length = len(eps_range) * len(min_samples_range) * (min_batch_num)
            #  遍历半径和最小样本量的所有组合
            min_batch_size = pd.to_numeric(self.cap) / min_batch_num

            for batch in range(min_batch_num):
                print(f'切片{batch + 1} ****************开始聚类*****************')
                if batch == min_batch_num - 1:

                else:


                for eps in eps_range:
                    for min_samples in min_samples_range:
                        start_time = time.time()

                        dbscan = DBSCAN(eps=eps, min_samples=min_samples)
                        aim_data = data_copy[[self.fea, self.label]]
                        #  返回聚类的类标签
                        try:
                            cluster_result = dbscan.fit_predict(aim_data)
                        except:
                            print(f'切片{batch+1}数据不足')
                        #  生成两列，一列是聚类标签，即每个数据对应哪一类

                        #  计算每一类的样本数量

                        #  计算类的数量

                        #  计算每个类别的数量，并将其给到每个数据，即获得每个数据对应所属类的大小。
                        #  删除类的数量小于规定阈值大小的数据
                        if len(data_copy['cluster_result'].unique()) > 1:

                        else:
                            index = []
                            index_outline = []
                            index_outline_type = []


                        a = a + 1
                        print(
                            f'聚类总次数为{length}，第{a}次聚类结束，eps={eps}，min_samples={min_samples}，获得{category}类，去除{sum_}个数据用时----------{(end_time - start_time):.2f}秒')
                #                         if sum_ > 0:
                #                             useful_params.append({
                #                                 'eps': eps,
                #                                 'min_samples': min_samples
                #                             })  # 保存有效的参数
                #                             print('参数有效，已保存')
                #                             print(' ')
                data_copy = pd.DataFrame()

            print(' ')
            print(f'总共去除{del_data_sum}个数据,原数据量为{len(self.ori_data)},清理后数据量为{(len(data_mo))}')

            return data_mo, useful_params
        else:
            print('聚类禁用')
            return self.ori_data, []

    def save_cluster_pic(self, data_name, data_mo):
        column_name = data_name.loc[0, 'WINDFARM_NAME']
        column_name_ = slugify(column_name, allow_unicode=True)
        farm_code = data_name.loc[0, 'WINDFARM_CODE']
        farm_code_ = slugify(farm_code)

        file_name = f'{column_name_}_{farm_code_}_cap={self.cap}.csv'
        filename = f'清洗加聚类后的'

        if self.type == '5':
            dir_1 = 'D:/pythonProject_work/NARI_prediction_work/场站/风电站'
            path_1 = os.path.join(dir_1, file_name)
            path_1_pic = os.path.join(path_1, filename)
            if os.path.exists(path_1):
                pass
            else:
                os.makedirs(path_1)
            plt.figure(figsize=(40, 20))
            plt.scatter(data_mo[self.fea], data_mo[self.label], c='b')
            plt.title(f'{farm_code_}')
            plt.savefig(f'{path_1_pic}散点图')


            plt.close()
        elif self.type == '7':
            dir_2 = 'D:/pythonProject_work/NARI_prediction_work/场站/光伏电站'
            path_2 = os.path.join(dir_2, file_name)
            path_2_pic = os.path.join(path_2, filename)
            if os.path.exists(path_2):
                pass
            else:
                os.makedirs(path_2)
            plt.figure(figsize=(40, 20))
            plt.scatter(data_mo[self.fea], data_mo[self.label], c='b')
            plt.title(f'{farm_code_}')
            plt.savefig(f'{path_2_pic}聚类散点图')


        print(f'已生成 {column_name_} 聚类后数据的散点图')
