# check box와 input된 검색어에 따라 최저가를 찾아 리턴해주는 서비스
import sqlite3
import random

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time

import requests
from bs4 import BeautifulSoup

import urllib.request
import urllib
import os
import sys

import pandas as pd
import numpy as np
import json

from flask import request



################################# 각 사이트별 데이터 받아오기 #################################

def get_item_name(product_name):
    item_list = [] 
    
    # raw data DB에서 검색된 값 가져오기
     
    conn = sqlite3.connect('raw_data.db')
    cur = conn.cursor()
    cur.execute(f"SELECT ITEM_ID, ITEM_TYPE, ITEM_NAME, ITEM_INGREDIENT, COMPANY_ID, COMPANY_NAME  FROM raw_data_item_non_split WHERE ITEM_NAME LIKE '%{product_name}%'")
      
    X = cur.fetchall()
    for data in X:
      item_list.append([data[0],data[1],data[2],data[3],data[4],data[5]])
    
    random.shuffle(item_list)
    random.shuffle(item_list)
    
    item_list = item_list[:10000] 
    
    cur.close()
    conn.close()
    
    return item_list

# selenium으로 return이 오래 걸림
# def get_danawa(item_list):
#     product_list = []
    
#     for item in item_list:
#         item_name = item

#         ser = Service('./chromedriver.exe')
#         driver = webdriver.Chrome(service=ser)

#         # 다나와 (11번가, 쿠팡, 티몬, 인터파크, G마켓, G9, 옥션) 가격비교
#         danawa_url = ('https://search.danawa.com/dsearch.php?k1=' + item_name.replace(' ','+') + '&module=goods&act=dispMain')

#         driver.get(danawa_url)
#         time.sleep(1)

#         try:
#             html = driver.page_source
#             soup = BeautifulSoup(html, 'html.parser')

#             link = soup.select('div.thumb_image')[0].select('a')[0].attrs['href']
#             name = soup.select('div.thumb_image')[0].select('img')[0].attrs['alt']
#             image = "https:" + soup.select('div.thumb_image')[0].select('img')[0].attrs['src']
#             price = soup.select('a.click_wish_prod')[0].attrs['price']
#         except: pass

#         product_list.append({'site' : "danawa",
#                              'name' : name,
#                              'price' : price,
#                              'image' : image,
#                              'link' : link})

#     return product_list

def get_naver(item_list):
    product_list = []
    
    # 하드 코딩 문제 해결해야 함
    client_id = 'client_id'
    client_secret = 'client_secret'
            
    for item in item_list:
        try:
            query = urllib.parse.quote(item[2]) #item_name만 가져옴
            
            url = "https://openapi.naver.com/v1/search/shop?query=" + query 
            
            request = urllib.request.Request(url)
            request.add_header('X-Naver-Client-Id', client_id)
            request.add_header('X-Naver-Client-Secret', client_secret)

            response = urllib.request.urlopen(request)
            res = response.read().decode('utf-8')
            res = res.replace('\t','').replace('\r','').replace('\\','').replace('\n','').replace('<b>','')
            res = json.loads(res)
            
            name = res['items'][0]['title']
            lprice = res['items'][0]['lprice']
            hprice = res['items'][0]['hprice']
            image = res['items'][0]['image']
            link = res['items'][0]['link']
            mallName = res['items'][0]['mallName']
            productId = res['items'][0]['productId']
            brand = res['items'][0]['brand']
            maker = res['items'][0]['maker']
            category1 = res['items'][0]['category1']
            category2 = res['items'][0]['category2']
            category3 = res['items'][0]['category3']

            append_data = {'db_item_id': item[0],
                        'db_item_type': item[1],
                        'db_item_name': item[2],
                        'db_item_ing': item[3],
                        'db_company_id': item[4],
                        'db_company_name': item[5],
                        'site_name' : "naver",
                        'item_name' : name,
                        'lprice' : lprice,
                        'hprice' : hprice,
                        'image' : image,
                        'link' : link,
                        'mallName' : mallName,
                        'productId' : productId,
                        'brand' : brand,
                        'maker' : maker,
                        'category1' : category1,
                        'category2' : category2,
                        'category3' : category3} 
                                 
            product_list.append(append_data)    
        except: continue 
    return product_list

# def all_get(item_list):    
# 	# danawa = get_danawa(item_list)
#     # google = get_google(item_list)
#     naver = get_naver(item_list)
#     # product_list = danawa + google + naver
# 	return naver

################################################################################################


######################################## search.db 저장 ########################################

def reset_store():
    conn = sqlite3.connect('search.db')
    cur = conn.cursor()

    create_table_get_site_list = """CREATE TABLE get_site_list(
                            ID INTEGER NOT NULL PRIMARY KEY,
                            DB_ITEM_ID VARCHAR(150) NOT NULL UNIQUE,
                            DB_ITEM_TYPE VARCHAR(150),
                            DB_ITEM_NAME VARCHAR(150),
                            DB_ITEM_ING VARCHAR(300),
                            DB_COMPANY_ID VARCHAR(150),
                            DB_COMPANY_NAME VARCHAR(150),
                            SITE_NAME VARCHAR(150),
                            ITEM_NAME VARCHAR(150),
                            LPRICE INTEGER,
                            HPRICE INTEGER,
                            IMAGE_ADDR VARCHAR(200),
                            LINK_ADDR VARCHAR(200),
                            MAllNAME VARCHAR(200),
                            PRODUCTID VARCHAR(200),
                            BRAND VARCHAR(200),
                            MAKER VARCHAR(200),
                            CATEGORY1 VARCHAR(200),
                            CATEGORY2 VARCHAR(200),
                            CATEGORY3 VARCHAR(200)
                            )"""                                         

    cur.execute("DROP TABLE IF EXISTS get_site_list")
    cur.execute(create_table_get_site_list)
    
    cur.close()
    conn.close()

def get_store(product_list):
    conn = sqlite3.connect('search.db')
    cur = conn.cursor()
    
    for item in product_list:
        try:
            insert_order = """INSERT INTO get_site_list (DB_ITEM_ID, DB_ITEM_TYPE, DB_ITEM_NAME, DB_ITEM_ING, DB_COMPANY_ID, DB_COMPANY_NAME, SITE_NAME, ITEM_NAME, LPRICE, HPRICE, IMAGE_ADDR, LINK_ADDR, MALLNAME, PRODUCTID, BRAND, MAKER, CATEGORY1, CATEGORY2, CATEGORY3) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            cur.execute(insert_order, (item['db_item_id'], item['db_item_type'], item['db_item_name'], item['db_item_ing'], item['db_company_id'], item['db_company_name'], item['site_name'], item['item_name'], item['lprice'], item['hprice'], item['image'], item['link'], item['mallName'], item['productId'], item['brand'], item['maker'], item['category1'], item['category2'], item['category3']))
            conn.commit()
        except: continue
    cur.close()
    conn.close()  
      

def reset_rating_store():
    conn = sqlite3.connect('search.db')
    cur = conn.cursor()

    create_table_get_rating_list = """CREATE TABLE get_rating_list(
                            ID INTEGER NOT NULL PRIMARY KEY,
                            DB_ITEM_ID VARCHAR(150) NOT NULL UNIQUE,
                            REVIEWS INTEGER,
                            RATINGS FLOAT,
                            FOREIGN KEY (DB_ITEM_ID) REFERENCES get_site_list(DB_ITEM_ID)
                            )"""                                         

    cur.execute("DROP TABLE IF EXISTS get_rating_list")
    cur.execute(create_table_get_rating_list)
    
    cur.close()
    conn.close()
    

def get_rating_store(product_names):
    # product_names = ['양념', '분말', '국', '탕', '찜', '볶음', '잼', '밥', '면', '롤', '빵', '도시락', '김치', '사탕', '만두']
    
    for product_name in product_names:
        try:
            item_list = get_item_name(product_name) 
            product_list = get_naver(item_list)
            get_store(product_list)
            print(product_name)
        except: continue
    
    # conn = sqlite3.connect('search.db')
    # cur = conn.cursor()
    
    # SEL = "SELECT DB_ITEM_ID, LINK_ADDR FROM get_site_list"
    # cur.execute(SEL)
    # link_list = cur.fetchall()

    # seq = 0
    # for link in link_list[:2]:
    #     try:
    #         url = link[1]
    #         time.sleep(3)
            
    #         # 네이버 감지로 block 돼서 headers 추가했으나 해결불가
    #         # headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}

    #         # 네이버 감지로 block 돼서 IP 우회도 실패
    #         proxies = {
    #             'http' : 'socks5://127.0.0.1:7680',
    #             'https' : 'socks5://127.0.0.1:7680',
    #         }
            
    #         data = requests.get(url, proxies = proxies)
    #         html = data.text
    #         soup = BeautifulSoup(html, 'html.parser')
            
    #         inner_link = soup.find_all('a', {'class':'product_btn_link__XRWYu'})[1]['href']
            
    #         time.sleep(3)
            
    #         raw_data = requests.get(inner, proxies = proxies)
    #         html = raw_data.text
    #         soup = BeautifulSoup(html, 'html.parser')
            
    #         reviews = int(soup.find_all('strong', {'class':'_2pgHN-ntx6'})[0].text.replace(',',''))
    #         ratings = float(soup.find_all('strong', {'class':'_2pgHN-ntx6'})[1].text[:3])
            
    #         cur.execute("INSERT INTO get_rating_list (DB_ITEM_ID, REVIEWS, RATINGS) VALUES (?,?,?)", (link[0], reviews, ratings))
    #         conn.commit()   
             
    #         time.sleep(3)
            
    #         print(f"link.............{seq}")
    #         seq += 1  
    #     except: continue
    # cur.close()
    # conn.close()     
################################################################################################       
        
        
################################# 데이터랩에서 분석할 데이터 추출 #################################

def reset_data_lab():
    conn = sqlite3.connect('search.db')
    cur = conn.cursor()

    create_table_data_lab = """CREATE TABLE data_lab(
                            ID INTEGER NOT NULL PRIMARY KEY,
                            KEYWORD VARCHAR(150),
                            _PERIOD DATE,
                            RATIO FLOAT,
                            _GROUP VARCHAR(100)
                            )"""                                         

    cur.execute("DROP TABLE IF EXISTS data_lab")
    cur.execute(create_table_data_lab)
    
    cur.close()
    conn.close()
    
def data_lab(): # 나중에 input_keyword로 검색한 품목의 자료를 서치
    url_input_list = ['device', 'gender', 'age']

    conn = sqlite3.connect('search.db')
    cur = conn.cursor()
        
    for url_input in url_input_list:
        client_id = 'client_id'
        client_secret = 'client_secret'

        url = f"https://openapi.naver.com/v1/datalab/shopping/category/keyword/{url_input}";
        
        # keyword 하드코딩문제 해결 필요
        body = "{\"startDate\":\"2017-08-01\",\"endDate\":\"2022-10-31\",\"timeUnit\":\"month\",\"category\":\"50000006\", \"keyword\": \"만두\", \"device\":\"\",\"ages\":[\"10\",\"20\",\"30\",\"40\",\"50\"],\"gender\":\"\"}";

        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id",client_id)
        request.add_header("X-Naver-Client-Secret",client_secret)
        request.add_header("Content-Type","application/json")
        response = urllib.request.urlopen(request, data=body.encode("utf-8"))
        rescode = response.getcode()
        if(rescode==200):
            response_body = response.read()
            res = response_body.decode('utf-8')
            res = res.replace('\t','').replace('\r','').replace('\\','').replace('\n','').replace('<b>','').replace('</b>','')
            res = json.loads(res)
            res_list = res['results'][0]['data']
            
            seq = 0
            for r in res_list:
                cur.execute("INSERT INTO data_lab (KEYWORD, _PERIOD, RATIO, _GROUP) VALUES (?,?,?,?)", (url_input, r['period'], r['ratio'], r['group']))
                conn.commit() 
                print(f"...............{seq}")
                seq += 1 
        else:
            print("Error Code:" + rescode)
###############################################################################################
    
# reset_store()
# reset_rating_store()
# get_rating_store()

# product_names = ['만두']
# get_rating_store(product_names)

# reset_data_lab()
# data_lab()