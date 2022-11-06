
import requests
import json

import sqlite3

from multiprocessing import Process, Queue, Pool

import pdb

class base_work:
    def __init__(self):
        self.conn = sqlite3.connect("raw_data.db")
        self.cur = self.conn.cursor()
        
        
        self.key = 'self.key'
        self.d_type = 'json'

    def create_table(self, create_table_name):
        conn = self.conn
        cur = self.cur
        table_name = create_table_name
        
        create_table_raw_data_item = """CREATE TABLE raw_data_item(
                                                                    ID INTEGER NOT NULL PRIMARY KEY,
                                                                    ITEM_ID VARCHAR(100),
                                                                    REPORT_DATE VARCHAR(100),
                                                                    COMPANY_ID VARCHAR(100),
                                                                    COMPANY_NAME VARCHAR(100),
                                                                    ITEM_NAME VARCHAR(100),
                                                                    ITEM_TYPE VARCHAR(100),
                                                                    ITEM_INGREDIENT VARCHAR(100)
                                                                    )"""

        create_table_raw_data_company = """CREATE TABLE raw_data_company(
                                                                    ID INTEGER NOT NULL PRIMARY KEY,
                                                                    COMPANY_ID VARCHAR(100),
                                                                    COMPANY_NAME VARCHAR(100),
                                                                    PERMIT_DATE VARCHAR(100),
                                                                    OWENER_NAME VARCHAR(100),
                                                                    COMPANY_CITY VARCHAR(100),
                                                                    COMPANY_ADDRESS VARCHAR(100),
                                                                    COMPANY_TEL VARCHAR(100),
                                                                    COMPANY_TYPE VARCHAR(150),
                                                                    FOREIGN KEY (COMPANY_ID) REFERENCES raw_data_item(COMPANY_ID)
                                                                    )"""
                                                                    
        create_table_raw_data_item_non_split = """CREATE TABLE raw_data_item_non_split(
                                                                    ID INTEGER NOT NULL PRIMARY KEY,
                                                                    ITEM_ID VARCHAR(100),
                                                                    REPORT_DATE VARCHAR(100),
                                                                    COMPANY_ID VARCHAR(100),
                                                                    COMPANY_NAME VARCHAR(100),
                                                                    ITEM_NAME VARCHAR(100),
                                                                    ITEM_TYPE VARCHAR(100),
                                                                    ITEM_INGREDIENT VARCHAR(100)
                                                                    )"""

        if table_name == 'raw_data_item':
            cur.execute("DROP TABLE IF EXISTS raw_data_item")
            cur.execute(create_table_raw_data_item)
            conn.commit()
        elif table_name == 'raw_data_company':
            cur.execute("DROP TABLE IF EXISTS raw_data_company")
            cur.execute(create_table_raw_data_company)
            conn.commit()
        elif table_name == 'raw_data_item_non_split':
            cur.execute("DROP TABLE IF EXISTS raw_data_item_non_split")
            cur.execute(create_table_raw_data_item_non_split)   
            conn.commit()         
        else : EOFError

        conn.commit()

    
# {'PRDLST_REPORT_NO': '2011001517149',
#  'PRMS_DT': '20141215',
#  'LCNS_NO': '20110015171',
#  'PRDLST_NM': 'GLOBE ULTRA DRY(글로브 울트라 드라이)',
#  'BSSH_NM': '하이트진로㈜강원공장',
#  'PRDLST_DCNM': '맥주',
#  'RAWMTRL_NM': '정제수,글루코아밀라아제,알파아밀라아제,효소제(베타글루카나아제),산도조절제(황산칼슘),이산화탄소,홉(펠렛+즙),전분,맥아'}

    def insert_items(self):
        conn = self.conn
        cur = self.cur
        API_KEY = self.key
        FILE_TYPE = self.d_type

        start_num = 0
        end_num = 899999 #'total_count': '890339'
                
        for NUM in range(start_num, int(end_num/1000)+1):
            print(f"insert items...... {NUM}")
            SERVICE_NAME = 'C002'

            if NUM < 899:
                try: 
                    API_URL_items = f"https://openapi.foodsafetykorea.go.kr/api/{API_KEY}/{SERVICE_NAME}/{FILE_TYPE}/{NUM*1000}/{(NUM*1000)+999}"

                    raw_data = requests.get(API_URL_items)
                    json_data = json.loads(raw_data.text)
                    
                    data = json_data[SERVICE_NAME]['row']   

                    for i in range(1000):
                        ITEM_INGREDIENT_list = []
                        ITEM_INGREDIENT_list = data[i]['RAWMTRL_NM'].split(',')

                        for j in range(len(ITEM_INGREDIENT_list)):                        
                            cur.execute("INSERT INTO raw_data_item (ITEM_ID, REPORT_DATE, COMPANY_ID, COMPANY_NAME, ITEM_NAME, ITEM_TYPE, ITEM_INGREDIENT) VALUES (?,?,?,?,?,?,?)", (data[i]['PRDLST_REPORT_NO'], "{}-{}-{}".format(data[i]['PRMS_DT'][:4], data[i]['PRMS_DT'][4:6], data[i]['PRMS_DT'][6:]), data[i]['LCNS_NO'], data[i]['BSSH_NM'], data[i]['PRDLST_NM'], data[i]['PRDLST_DCNM'], ITEM_INGREDIENT_list[j]))
                            conn.commit()
                except: continue
            else:
                try: 
                    API_URL_items = f"https://openapi.foodsafetykorea.go.kr/api/{API_KEY}/{SERVICE_NAME}/{FILE_TYPE}/{NUM*1000}/{(NUM*1000)+int(NUM%1000)}"

                    raw_data = requests.get(API_URL_items)
                    json_data = json.loads(raw_data.text)
                    
                    data = json_data[SERVICE_NAME]['row']   

                    for i in range(int(NUM%1000)):
                        ITEM_INGREDIENT_list = []
                        ITEM_INGREDIENT_list = data[i]['RAWMTRL_NM'].split(',')

                        for j in range(len(ITEM_INGREDIENT_list)):                        
                            cur.execute("INSERT INTO raw_data_item (ITEM_ID, REPORT_DATE, COMPANY_ID, COMPANY_NAME, ITEM_NAME, ITEM_TYPE, ITEM_INGREDIENT) VALUES (?,?,?,?,?,?,?)", (data[i]['PRDLST_REPORT_NO'], "{}-{}-{}".format(data[i]['PRMS_DT'][:4], data[i]['PRMS_DT'][4:6], data[i]['PRMS_DT'][6:]), data[i]['LCNS_NO'], data[i]['BSSH_NM'], data[i]['PRDLST_NM'], data[i]['PRDLST_DCNM'], ITEM_INGREDIENT_list[j]))
                            conn.commit()
                except: break

    def insert_items_non_split(self):
        conn = self.conn
        cur = self.cur
        API_KEY = self.key
        FILE_TYPE = self.d_type

        start_num = 0
        end_num = 899999 #'total_count': '890339'
                
        for NUM in range(start_num, int(end_num/1000)+1):
            # print(f"insert items...... {NUM}")
            SERVICE_NAME = 'C002'

            if NUM < 899:
                try: 
                    print(f"insert items...... {NUM}......")
                    API_URL_items = f"https://openapi.foodsafetykorea.go.kr/api/{API_KEY}/{SERVICE_NAME}/{FILE_TYPE}/{(NUM*1000)}/{(NUM*1000)+999}"

                    raw_data = requests.get(API_URL_items)
                    json_data = json.loads(raw_data.text)
                    
                    data = json_data[SERVICE_NAME]['row']
                    # pdb.set_trace()
                    for i in range(1000):
                        cur.execute("INSERT INTO raw_data_item_non_split (ITEM_ID, REPORT_DATE, COMPANY_ID, COMPANY_NAME, ITEM_NAME, ITEM_TYPE, ITEM_INGREDIENT) VALUES (?,?,?,?,?,?,?)", (data[i]['PRDLST_REPORT_NO'], "{}-{}-{}".format(data[i]['PRMS_DT'][:4], data[i]['PRMS_DT'][4:6], data[i]['PRMS_DT'][6:]), data[i]['LCNS_NO'], data[i]['BSSH_NM'], data[i]['PRDLST_NM'], data[i]['PRDLST_DCNM'], data[i]['RAWMTRL_NM']))
                        conn.commit()                        
                except: continue
            else:
                try: 
                    print(f"insert items...... {NUM}......")
                    API_URL_items = f"https://openapi.foodsafetykorea.go.kr/api/{API_KEY}/{SERVICE_NAME}/{FILE_TYPE}/{NUM*1000}/{(NUM*1000)+int(NUM%1000)}"

                    raw_data = requests.get(API_URL_items)
                    json_data = json.loads(raw_data.text)
                    
                    data = json_data[SERVICE_NAME]['row']   

                    for i in range(int(NUM%1000)):
                        cur.execute("INSERT INTO raw_data_item_non_split (ITEM_ID, REPORT_DATE, COMPANY_ID, COMPANY_NAME, ITEM_NAME, ITEM_TYPE, ITEM_INGREDIENT) VALUES (?,?,?,?,?,?,?)", (data[i]['PRDLST_REPORT_NO'], "{}-{}-{}".format(data[i]['PRMS_DT'][:4], data[i]['PRMS_DT'][4:6], data[i]['PRMS_DT'][6:]), data[i]['LCNS_NO'], data[i]['BSSH_NM'], data[i]['PRDLST_NM'], data[i]['PRDLST_DCNM'], data[i]['RAWMTRL_NM']))
                        conn.commit()
                except: break

# {'PRSDNT_NM': '이*환',
#  'PRMS_DT': '20061215',
#  'LCNS_NO': '20060169056',
#  'INSTT_NM': '대구광역시 북구',
#  'BSSH_NM': '(New)금정상사',
#  'TELNO': '',
#  'LOCP_ADDR': '대구광역시 북구 유통단지로7길 31-9(1층 산격동)',
#  'INDUTY_NM': '식품제조가공업'}

    def insert_companies(self):
        conn = self.conn
        cur = self.cur
        API_KEY = self.key
        FILE_TYPE = self.d_type
        
        start_num = 0
        end_num = 30255 #'total_count': '30029'
        
        for NUM in range(start_num, int(end_num/1000)+1):
            print(f"insert company...... {NUM}")
            SERVICE_NAME = 'I1220'
            if NUM < 30:
                try: 
                    API_URL_company = f"https://openapi.foodsafetykorea.go.kr/api/{API_KEY}/{SERVICE_NAME}/{FILE_TYPE}/{NUM*1000}/{(NUM*1000)+999}"

                    raw_data = requests.get(API_URL_company)
                    json_data = json.loads(raw_data.text)
                    
                    data = json_data[SERVICE_NAME]['row']        

                    for i in range(1000):
                        cur.execute("INSERT INTO raw_data_company (COMPANY_ID, COMPANY_NAME, PERMIT_DATE, OWENER_NAME, COMPANY_CITY, COMPANY_ADDRESS, COMPANY_TEL, COMPANY_TYPE) VALUES (?,?,?,?,?,?,?,?)", (data[i]['LCNS_NO'], data[i]['BSSH_NM'], "{}-{}-{}".format(data[i]['PRMS_DT'][:4], data[i]['PRMS_DT'][4:6], data[i]['PRMS_DT'][6:]), data[i]['PRSDNT_NM'], data[i]['INSTT_NM'], data[i]['LOCP_ADDR'], data[i]['TELNO'], data[i]['INDUTY_NM']))
                        conn.commit()
                except: continue
            else:
                try: 
                    API_URL_company = f"https://openapi.foodsafetykorea.go.kr/api/{API_KEY}/{SERVICE_NAME}/{FILE_TYPE}/{NUM*1000}/{(NUM*1000)+int(NUM%1000)}"

                    raw_data = requests.get(API_URL_company)
                    json_data = json.loads(raw_data.text)
                    
                    data = json_data[SERVICE_NAME]['row']        

                    for i in range(int(NUM%1000)):
                        cur.execute("INSERT INTO raw_data_company (COMPANY_ID, COMPANY_NAME, PERMIT_DATE, OWENER_NAME, COMPANY_CITY, COMPANY_ADDRESS, COMPANY_TEL, COMPANY_TYPE) VALUES (?,?,?,?,?,?,?,?)", (data[i]['LCNS_NO'], data[i]['BSSH_NM'], "{}-{}-{}".format(data[i]['PRMS_DT'][:4], data[i]['PRMS_DT'][4:6], data[i]['PRMS_DT'][6:]), data[i]['PRSDNT_NM'], data[i]['INSTT_NM'], data[i]['LOCP_ADDR'], data[i]['TELNO'], data[i]['INDUTY_NM']))
                        conn.commit()
                except: continue                

#####################################################################################
# multiprocessing 구현 실패............................................
    # def multi_processing(self):
    #     # conn = connect
    #     # cur = cursor
    #     # API_KEY = key
    #     # FILE_TYPE = d_type   
        
    #     target = input("target_name: ")
    #     num = int(input("num: "))
    #     cpu = 10
        
    #     for i in range(1,cpu+1):
    #         globals()['pc_{}'.format(i)] = Process(target=target, args=(self.conn, self.cur, self.key, self.d_type, int(round(num/cpu,0))*i, int(round(num/cpu,0))*i+int(round(num/cpu,0))))   

    #         globals()['pc_{}'.format(i)].start()
    #         globals()['pc_{}'.format(i)].join()
    
     
    # def pools(self):
    #     # Pool 객체 초기화
    #     pool = Pool()
    #     pool = Pool(processes=10)
        
    #     # target = input("target_name: ")
    #     # num = int(input("num: "))
        
    #     # Pool.map
    #     pool.map('insert_company', range(100))
    
#####################################################################################

# from project import base_work
# b=base_work()
# b.create_table('raw_data_item_non_split')
# b.insert_companies()
# b.insert_items()
# b.insert_items_non_split()