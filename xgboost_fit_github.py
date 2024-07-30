import os.path
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import root_mean_squared_error, mean_squared_error, mean_absolute_error
from django.utils.text import slugify
class xgboost:
    def __init__(self, train_data_x, train_data_y, test_data_x, farm_type, cap):
        self.train_x = train_data_x
        self.train_y = train_data_y
        self.test_x = test_data_x
        self.type = farm_type
        self.cap = cap

    def xgb_fit(self):
        xgb = XGBRegressor(
            objective='reg:squarederror',
            n_estimators=1000,
            learning_rate=0.015,
            max_depth=8,
            min_child_weight=1,
            subsample=0.7,
            colsample_bytree=1,
            gamma=0,
            reg_alpha=0,
            reg_lambda=0
        )
        xgb.fit(self.train_x, self.train_y)
        pre = xgb.predict(self.test_x)
        return pre
    def fit_plot(self, pre_inverse, test_data_x, test_data_y, data_name, scatter_x, cluster: bool):
        y_test = np.array(test_data_y)
        MSE = mean_squared_error(pre_inverse, y_test) / pd.to_numeric(self.cap)
        RMSE = root_mean_squared_error(pre_inverse, y_test) / pd.to_numeric(self.cap)
        MAE = mean_absolute_error(pre_inverse, y_test) / pd.to_numeric(self.cap)

        print('均方误差为', MSE)
        print('均方根误差为', RMSE)
        print('平均绝对误差', MAE)
        print(f'准确率为: {((1-MAE)*100):.2f}%')

        if cluster:
            filename = f'清洗加聚类后的'
        else:
            filename = f'清洗后的'
        if self.type == '5':
            dir_1 = 'D:/pythonProject_work/NARI_prediction_work/场站/风电站'
            path_1 = os.path.join(dir_1, file_name)
            path_1_pic = os.path.join(path_1, filename)
            if os.path.exists(path_1):
                pass
            else:
                os.makedirs(path_1)

            print(f'已生成 {column_name_} XGboost拟合后数据的散点和折线图')


        elif self.type == '7':
            dir_2 = 'D:/pythonProject_work/NARI_prediction_work/场站/光伏电站'
            path_2 = os.path.join(dir_2, file_name)
            path_2_pic = os.path.join(path_2, filename)
            if os.path.exists(path_2):
                pass
            else:
                os.makedirs(path_2)
            plt.figure(figsize=(40, 20))

            print(f'已生成 {column_name_} XGboost拟合后数据的散点和折线图')
        plt.close()
        return acc, name
    # def processing(self):
    #     input_X_train_scaler, ytrain_scaler, input_X_test_scaler, y_test_scaler, y_test = self.dataloder()
    #     pre = self.xgb_fit(input_X_train_scaler, ytrain_scaler, input_X_test_scaler)
    #     self.fit_plot(pre, y_test, y_test_scaler, data_name='cluster_data')
