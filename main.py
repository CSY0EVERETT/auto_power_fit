# from create_folders import create_folders
import os.path
import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from clean import clean_data
from read_data_from_kingbase import read_data
from data_merge import data_merge
from read_data_from_kingbase import read_data_from_root
from cut_time import cut_time
from data_loader import data_loader
from sklearn.preprocessing import MinMaxScaler
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
from XGboost_fit import xgboost
from cluster import multi_cluster
from picture_generate import picture_generate
# #  创建文件夹
# create_folders = create_folders()
# create = create_folders.create_all_folders()

#  文件参数
acc_result = pd.DataFrame()
j = 0
for i in range(705, 706):
    farm_type = '5'  # 风电为5，光伏为7
    farm_name = f'{i}'
    database_data =
    database_name =
    user =
    password =
    host =
    port =
    search_name = f'r({farm_type}.{farm_name})'
    if farm_type == '5':

        fea = 'speed80'
    elif farm_type == '7':
        fea = 'rad'
    print(f'执行到{i}')


    data_root_read = read_data_from_root(farm_type, farm_name)
    result, cap = data_root_read.search_file()
    filename = data_root_read.get_filename()
    data_name = data_root_read.get_data_name()
    #  如果本地存在，则读取本地读取数据和装机容量，反之本地不存在，则在数据库中获取数据和装机容量
    if len(result) > 0:
        result = result
        print('已在本地读取数据')
    else:
        data_reader = read_data(farm_type, farm_name, database_data, database_name, user, password, host, port)
        # data_name:读取到的场站编号，场站名，经纬度，装机容量。
        # data_power:为读取到的场站编号，时间，功率。
        # data_tower:为读取到的场站编号，时间，特征，如风速，温湿度。
        # data_tower_code:为读取到的场站编号对应的
        data_name, data_power, data_tower, data_tower_code, cap = data_reader.read_data_from_EMS_and_HISDB()
        if len(data_power) < 1:
            pass
        else:

            #  文件整合
            data_merger = data_merge(farm_type, data_power, data_tower, cap)
            result = data_merger.create_table()
            data_merger.create_folder(data_name, result)


    label = 'rtpower'
    # try:

    data_clean = clean_data(result, fea, label, result['Time'], cap, farm_type, farm_name)
    data_cleaned = data_clean.clean_all()
        # cut_time_ = cut_time(data_cleaned, farm_type, data_name, cap)
        # cut_time_.save_cut_data_to_csv()
        # data_clean.save_clean_pic(data_name, data_cleaned)
        # plt.close()
    # except:
    #     print(f'跳过{i}场站')




    #  特征选择
    try:
        data_loader_ = data_loader(data_cleaned, farm_type,
                                  x_speed10=False, x_speed30=False, x_speed50=False, x_speed70=False, x_speed80=True,
                                  x_dir10=False, x_dir30=False, x_dir50=False, x_dir70=False, x_dir80=False,
                                  x_press10=False, x_temp10=False, x_hum10=False,

                                  x_rad=False, speed=False, dir=False, temp=False, hum=False, press=False
                                  )
        train_data, train_x, train_x_scaler, train_y, data_test_x, data_test_y = data_loader_.fd_fea_select()
        #  归一化
        y_scaler = MinMaxScaler()
        train_y_scaler = y_scaler.fit_transform(np.array(train_y).reshape(-1, 1))
        #  归一化
        x_scaler = MinMaxScaler()
        if len(data_test_x.columns) == 1:
            x_test_scaler = x_scaler.fit_transform(np.array(data_test_x).reshape(-1, 1))
        else:
            x_test_scaler = x_scaler.fit_transform(data_test_x)

        xgboost_ = xgboost(train_x_scaler, train_y_scaler, x_test_scaler, farm_type, cap)
        pre = xgboost_.xgb_fit()
        pre_inverse = y_scaler.inverse_transform(np.array(pre).reshape(-1, 1))
        acc, name = xgboost_.fit_plot(pre_inverse, data_test_x, data_test_y, data_name, 'speed80', cluster=False)
        plt.cla()
        plt.close("all")
        print(f'场站{i}成功拟合')
        print()
        #
        #
        # acc_result.loc[j, '场站'] = name
        # acc_result.loc[j, '准确度'] = acc
        # j += 1
    except:
        print(f'场站{i}有问题，或不存在')
        print()

    # i += 1

# dir = 'D:/pythonProject_work/NARI_prediction_work'
# filename = '简单清理后，光伏电场站拟合结果.csv'
#
# path = os.path.join(dir, filename)
#
# acc_result.to_csv(path, index=False, mode='a')

# data_cleaned_ = data_cleaned.drop(data_cleaned[(data_cleaned['rtpower']>2)&(data_cleaned['speed80']<2)].index)
# data_cleaned_1 = data_cleaned_.drop(data_cleaned[(data_cleaned['rtpower']<15)&(data_cleaned['speed80']>10)&(data_cleaned['speed80']<13)].index)
# data_cleaned_ = data_cleaned.drop(data_cleaned[data_cleaned['speed80']>20].index)
eps_min = 1
eps_max = 1
min_samples_min = 100
min_samples_max = 200
if len(train_data) > 150000:
    min_samples_min = min_samples_min
    min_samples_max = min_samples_max
else:
    min_samples_min -= 100
    min_samples_max -= 100
eps_step = 0.25
samples_step = 50

label = 'rtpower'
# save_dir = 'D:/pythonProject_work/NARI_prediction_work/cluster_picture'
# filename = '082.png'
data_cluster = multi_cluster(
    train_data, fea, label, eps_min, eps_max, min_samples_min, min_samples_max, eps_step, samples_step, farm_type, cap, cluster=True
)

cluster_data, useful_params = data_cluster.autom_cluster(15)
data_cluster.save_cluster_pic(data_name, cluster_data)
cluster_path = 'D:/pythonProject_work/NARI_prediction_work/场站'
filename = '聚类后.png'
path = os.path.join(cluster_path, filename)
plt.figure(figsize=(40, 20))
plt.scatter(cluster_data[fea], cluster_data[label], c='b')
plt.savefig(path)

# try:
data_loader_ = data_loader(cluster_data, farm_type,
                           x_speed10=False, x_speed30=False, x_speed50=False, x_speed70=False, x_speed80=True,
                           x_dir10=False, x_dir30=False, x_dir50=False, x_dir70=False, x_dir80=False,
                           x_press10=False, x_temp10=False, x_hum10=False,

                           x_rad=False, speed=False, dir=False, temp=False, hum=False, press=False
                           )
_, train_x, train_x_scaler, train_y, _, _ = data_loader_.fd_fea_select()
#  归一化
y_scaler = MinMaxScaler()
train_y_scaler = y_scaler.fit_transform(np.array(train_y).reshape(-1, 1))
#  归一化
# x_scaler = MinMaxScaler()
# if len(data_test_x.columns) == 1:
#     x_test_scaler = x_scaler.fit_transform(np.array(data_test_x).reshape(-1, 1))
# else:
#     x_test_scaler = x_scaler.fit_transform(data_test_x)

xgboost_ = xgboost(train_x_scaler, train_y_scaler, x_test_scaler, farm_type, cap)
pre = xgboost_.xgb_fit()
pre_inverse = y_scaler.inverse_transform(np.array(pre).reshape(-1, 1))
acc, name = xgboost_.fit_plot(pre_inverse, data_test_x, data_test_y, data_name, 'speed80', cluster=True)
# plt.show()
plt.cla()
plt.close("all")
print(f'场站{i}成功拟合')
print()
#
#
# # print(data_test_x)
#     # print(data_test_y)
#
#
#     # # print(data_cleaned)
#     # print(cut_data)
