# import xml
# from xml.etree import ElementTree
#
# from simple_salesforce import Salesforce
# import requests
# import pandas as pd
# from io import StringIO
# from dotenv import dotenv_values
# import csv
#
# config = dotenv_values("Creds.env")
#
# my_id =config['username']
# pwd=config["password"]
# my_secret_key = config["secret_key"]
#

#
# sf = Salesforce(username=my_id,password=pwd, security_token=my_secret_key)
#
# print("Connection Sucessful")


# sf_instance = 'https://icims1.lightning.force.com/' #Your Salesforce Instance URL
# reportId = '00O8X000008upZPUAY' # add report id
# export = '?isdtp=p1&export=1&enc=UTF-8&xf=csv'
# sfUrl = sf_instance + reportId + export
# response = requests.get(sfUrl,headers=sf.headers)
# download_report = response.content.decode('utf-8')
# df1 = pd.read_csv(StringIO(download_report))


# sf = Salesforce(username=my_id,password=pwd, security_token=my_secret_key)
# print("Connection Sucessful")
# descri=sf.SBQQ__Quote__c.describe()
# results=sf.query_all("""
#     Select
#     FIELDS(STANDARD)
#
#     from SBQQ__Quote__c
#     limit 20
#
#     """)

import os,sys,traceback
from datetime import datetime
#


path=r"C:\Users\hanish.chebrole\OneDrive - Icims, Inc\Hanish_test_2"
os.chdir(path)
# file = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "x")

file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
file1.write(datetime.now().strftime( "%d/%m/%Y %H:%M:%S") + " Please Close file \n")
file1.close()
#
# try:
#     c
# except Exception as e:
# exc_type, exc_obj, exc_tb = sys.exc_info()
#     fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#     file.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S")+"< Type:"+str(exc_type)+"< Error:"+str(exc_obj)+"> < Line Number:"+ str(exc_tb.tb_lineno)+"> < File:"+str(fname)+" >")
#     file.close()
# import pandas as pd
# SQL_QL_data_missing=pd.read_csv(r"C:\Users\hanish.chebrole\OneDrive - Icims, Inc\Hanish_test_2\Compare_Ql_SF.csv")
