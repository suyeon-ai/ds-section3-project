import sqlite3
import json

import pandas as pd
import numpy as np
import datetime

# from sklearn.model_selection import train_test_split

def free_table_api(big_category = '냉동/간편조리식품',mid_category = '만두'):
    # DB api
    conn = sqlite3.connect('search.db')
    cur = conn.cursor()

    # big_category = '냉동/가공조리식품'
    # mid_category = '만두'

    get_api = f"SELECT * FROM get_site_list WHERE CATEGORY2 LIKE '{big_category}' and CATEGORY3 LIKE '{mid_category}'"
    cur.execute(get_api)
    response = cur.fetchall()

    results = [] 

    for r in response:
        results.append({
            'product_name': r[3],
            'price' : r[9],
            'link' : r[12]
        })

    # results2 = modeling(mid_category)
    
    cur.close()
    conn.close()
    
    # return {'category':[{"big_category":big_category, "mid_category":mid_category}],
    #         'results': [{"product_detail":results, "predict_values":results2}]}
    return {'category':[{"big_category":big_category, "mid_category":mid_category}],
            'results': [{"product_detail":results}]}
    

# def modeling(mid_category):
    
#     # model api
#     # EDA
#     cur.execute("SELECT * FROM data_lab")
#     c = cur.fetchall()
    
#     df = pd.DataFrame(c).drop(0, axis=1)
#     df.columns = ['section', 'date', 'ratio', 'div']
    
#     df['date'] = pd.to_datetime(df['date'])
#     df['year'] = df['date'].dt.year 
#     df['month'] = df['date'].dt.month 

#     # predict
#     target = 'ratio'
#     features = ['year','month']

#     X = df[features]
#     y = df[target]
    
#     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
#     import statsmodels.api as sm

#     fit_df = sm.OLS(y_train, X_train).fit() #회귀분석 계산
#     pred_tr = fit_df.predict(X_train).values
#     pred_te = fit_df.predict(X_test).values
    
#     pred_date = []
#     pred_ratio = []

#     for y in range(2023,2025):
#         for m in range(1,13):
#             extrapolate_x_m = [[y, m]]
#             extrapolate_y_m = fit_df.predict(extrapolate_x_m)[0]  

#             if m < 10:
#             pred_date.append(f"{y}-0{m}-01")
#             pred_ratio.append(extrapolate_y_m)
#             else:
#             pred_date.append(f"{y}-{m}-01")
#             pred_ratio.append(extrapolate_y_m)
    
#     results = []  
    
#     for r in range(24):
#         results.append({
#             'predict_date' : pred_date[r],
#             'predict_ratio' : pred_ratio[r]
#         })
              
#     cur.close()
#     conn.close()
    
#     return results

print(free_table_api(big_category = '냉동/간편조리식품',mid_category = '만두'))
