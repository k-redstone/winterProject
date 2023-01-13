import RFM

rfm = RFM.RFM()

print(rfm.get_data('./src/rfm_result/rfm_grade.parquet'))
