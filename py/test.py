import RFM
import common

rfm = RFM.RFM()

rfm.raw_data()
rfm_point = {'R', 'F', 'M'}
rfm_col_name = {'R': 't_dat', 'F': 'count', 'M': 'total'}
df = common.get_data('./src/rfm_result/rfm.parquet')

rfm.get_rfm_grade(
    df=df, num_class=10, rfm_point=rfm_point, rfm_col_name=rfm_col_name)
