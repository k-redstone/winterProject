import numpy as np
import pandas as pd
from sklearn import preprocessing
from pyarrow import csv
import pyarrow.parquet as pq


class RFM:
    def raw_data(self):
        transaction = csv.read_csv(
            "./src/data/transactions_train.csv").to_pandas()
        data_merge = transaction[['customer_id',
                                  'article_id', 'price', 't_dat']]

        find_du = data_merge.value_counts(
            sort=False, subset=['customer_id', 'article_id'])
        find_du.reset_index()
        find_du = pd.DataFrame(find_du, columns=['count'])
        find_du = find_du.reset_index()

        final_df = data_merge.drop_duplicates(
            ['customer_id', 'article_id'], keep='first', inplace=False)
        final_df = final_df.merge(
            find_du, on=['customer_id', 'article_id'], how='right')
        final_df['total'] = final_df['price'] * final_df['count']

        last_day = pd.to_datetime(final_df['t_dat'])
        current_day = pd.to_datetime('2018-09-20')
        time_diff = last_day - current_day
        time_in_second = [x.total_seconds() for x in time_diff]
        final_df['t_dat'] = time_in_second
        self.save_data(final_df, './src/rfm_result/rfm.parquet')

    def get_data(self, path):
        data = pq.read_pandas(path).to_pandas()
        return data

    def save_data(self, data, path):
        data.to_parquet(path, engine='pyarrow', index=False)

    def get_score(self, level, data):
        score = []
        for j in range(len(data)):
            for i in range(len(level)):
                if data[j] <= level[i]:
                    score.append(i+1)
                    break
                elif data[j] > max(level):
                    score.append(len(level)+1)
                    break
                else:
                    continue
        return score

    def get_rfm_grade(self, df, num_class, rfm_point, rfm_col_name, suffix=None):
        for k in rfm_point:
            scale = preprocessing.StandardScaler()
            temp_data = np.array(df[rfm_col_name[k]])
            temp_data = temp_data.reshape((-1, 1))
            temp_data = scale.fit_transform(temp_data)
            temp_data = temp_data.squeeze()
            quantiles_level = np.linspace(0, 1, num_class+1)[1:-1]
            quantiles = []
            for ql in quantiles_level:
                quantiles.append(np.quantile(temp_data, ql))

            score = self.get_score(quantiles, temp_data)
            new_col_name = rfm_col_name[k] + '_' + k
            df[new_col_name] = score
            self.save_data(df, './src/rfm_result/rfm_grade.parquet')
