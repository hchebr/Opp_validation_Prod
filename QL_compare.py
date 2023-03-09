import sys
import pandas as pd
import numpy as np
from datetime import timedelta,datetime
import re,os
from glob import glob
from tkinter import messagebox,HORIZONTAL,Label,Tk
import time

class compare_quote_line_SF():
    def __init__(self,path,root,msg,msg_remaining,SF_data,pdf_data):

        self.SF_report=SF_data
        self.datanet = pdf_data
        self.path=path
        self.root=root
        self.msg=msg
        self.msg_remaining = msg_remaining
        self.start=time.time()
        self.msg.config(text="", fg='blue')
        self.msg_remaining.config(text="")
        print(self.SF_report)
        os.chdir(path)
        try:
            self.Quoteline()
        except Exception as e:
            self.msg_remaining.config(text=e)
            os.chdir(self.path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " <Type:" + str(exc_type) + "< Error:" + str( exc_obj) + "> < Line Number:" + str(exc_tb.tb_lineno) + "> < File:" + str(fname) + " > /n")
            file1.close()
            raise

    def Quoteline(self):

            self.msg.config(text="Comparing SF Quoteline Data!!")
            SF_report = self.SF_report
            # TSF should be included
            sql_ql_data = SF_report[['Product: CASESAFEID','Product: Product Name','Output Quantity','CASESAFEID']]

            sql_ql_data.columns=['product_id','Name','Quantity','Quote_id']

            pdf = []
            folder = r"*.pdf"
            for pdf_file in glob(folder):
                pdf.append(os.path.basename(pdf_file).split('_')[0])

            sql_ql_data=pd.merge(sql_ql_data, pd.DataFrame(pdf, columns=["Quote_id"]), on="Quote_id", how='inner')
            sql_data = sql_ql_data
            sql_data['Name'] = sql_data['Name'].str.replace('iCIMS Text Engagement For Recruiting', 'icims text engagement')
            sql_data['Name'] = sql_data['Name'].str.replace('iCIMS Level Up User Admin Training - Essentials','trainingessentials')
            sql_data['Name'] = np.where(sql_data['Name'].str.contains('Implementation'), 'implementationservices',sql_data['Name'])
            sql_data['Name'] = np.where(sql_data['Name'].str.contains('iCIMS Digital Assistant'),'iCIMS Digital Assistant', sql_ql_data['Name'])
            # sql_data['Quantity'] = np.where(sql_data['Name'] == 'implementationservices', 0, sql_data['Quantity'])
            sql_data['join_id'] = sql_data['Name'].apply(lambda x: ''.join(filter(str.isalnum, x.lower()))) + sql_data['Quote_id']
            # sql_ql_data = pd.read_csv(r"sql_quote_lines.csv")
            # QL_pdf = pd.read_csv(r"Quote_lines_pdf.csv")

            sql_ql_data = sql_data.groupby(['Name','Quote_id','join_id','product_id'])['Quantity'].max().reset_index()



            # sql_ql_data.to_csv(r"ql_data_sf.csv")

            QL_pdf=self.datanet

            QL_pdf = QL_pdf.drop(QL_pdf.columns[0], axis=1)
            QL_pdf['Name'] = QL_pdf['Name'].str.replace('iCIMS Text Engagement For Recruiting', 'icims text engagement')
            QL_pdf['Name'] = QL_pdf['Name'].str.replace('iCIMS Level Up User Admin Training - Essentials','trainingessentials')
            QL_pdf['Name'] = np.where(QL_pdf['Name'].str.contains('Implementation'), 'implementationservices', QL_pdf['Name'])

            # QL_pdf['Quantity'] = np.where(QL_pdf['Name'] == 'implementationservices', 0, QL_pdf['Quantity'])
            QL_pdf = QL_pdf.groupby(['Name', 'Quote_id'])['Quantity'].sum().reset_index()
            QL_pdf['join_id'] = QL_pdf['Name'].apply(lambda x: ''.join(filter(str.isalnum, x.lower()))) + QL_pdf['Quote_id']
            # QL_pdf.to_csv('QL_pdf.csv')

                # QL_data = pd.merge(QL_pdf, sql_ql_data, on=['join_id'], how='right')
            QL_data = pd.merge(QL_pdf, sql_ql_data, on=['join_id'], how='left')
            QL_data.columns = [col.replace('_x', '_pdf').replace('_y', '_SF') for col in QL_data.columns]
            QL_list = []
            for i in range(0, len(QL_data)):
                if QL_data['Quantity_pdf'][i] == QL_data['Quantity_SF'][i]:
                    QL_list.append(1)
                else:
                    QL_list.append(0)

            QL_data['Check'] = QL_list

            QL_data_false = QL_data[QL_data['Check'] == 0][['Name_pdf', 'Quote_id_pdf', 'Quantity_pdf', 'Quantity_SF']]
            QL_data_false.rename(columns={'Name_pdf': 'Field', 'Quote_id_pdf': 'Quote_id', 'Quantity_pdf': 'Pdf_value',
                                          'Quantity_SF': 'SF_value'}, inplace=True)

            # QL_data_false.to_csv(r'QL_data_false.csv')

            rare = ['01t1V00000LFMZJQA5', '01t1V00000LFMZ8QAP', '01t1V00000LFMZBQA5', '01t1V00000LFMZHQA5',
                    '01t1V00000LFMZIQA5', '01t1V00000LFMaqQAH']
            rare_df = sql_ql_data[sql_ql_data['product_id'].isin(rare)].reset_index().drop('index', axis=1)
            rare_df.drop(['product_id','join_id'], axis=1, inplace=True)
            rare_df['Field']=rare_df['Name']
            rare_df['SF_value'] = rare_df['Quantity']
            rare_df=rare_df.drop(['Name','Quantity'],axis=1)
            rare_df['type'] = 'Rare_not_in_OF'
            rare_df = rare_df.groupby(['Field','Quote_id', 'type'])['SF_value'].sum().reset_index().sort_values(by='Quote_id').reset_index().drop('index', axis=1)

            sql_data_1=pd.merge(sql_data,sql_ql_data,on=['join_id'], how='inner')
            drop_col=sql_data_1.columns[sql_data_1.columns.str.contains('_x')]
            sql_data_1=sql_data_1.drop(drop_col,axis=1)
            sql_data_1.columns=['join_id','Name', 'Quote_id', 'product_id', 'Quantity']
            # sql_data_1.to_csv('sql_data_1.csv')
            print(sql_data_1.columns)


            SQL_QL_data_missing = pd.merge(QL_pdf, sql_data_1, on=['join_id'], how='right')
            # SQL_QL_data_missing.to_csv('missing_sql_ql_data.csv')
            SQL_QL_data_missing.columns = [col.replace('_x', '_pdf').replace('_y', '_SF') for col in SQL_QL_data_missing.columns]
            SQL_QL_data_missing = SQL_QL_data_missing[SQL_QL_data_missing['Name_pdf'].isna() & ~ SQL_QL_data_missing['product_id'].isin(rare)]
            SQL_QL_data_missing = SQL_QL_data_missing[[col for col in SQL_QL_data_missing.columns if '_SF' in col]]
            SQL_QL_data_missing = SQL_QL_data_missing.drop_duplicates()
            SQL_QL_data_missing = SQL_QL_data_missing.reset_index().drop('index', axis=1)
            SQL_QL_data_missing.columns=[col.replace('_SF','') for col in SQL_QL_data_missing.columns]
            SQL_QL_data_missing.columns = ['Field','Quote_id', 'SF_value']
            SQL_QL_data_missing['type'] = 'Not_in_OF_but_in_SQL'
            SQL_QL_data_missing=SQL_QL_data_missing[(SQL_QL_data_missing['SF_value'] != 0 ) & (SQL_QL_data_missing['type'] == 'Not_in_OF_but_in_SQL')]


            QL_data = pd.concat([QL_data_false, rare_df, SQL_QL_data_missing],axis=0)
            QL_data = QL_data.reset_index().drop('index', axis=1)
            QL_data.to_csv(r'Compare_Ql_SF.csv')
            self.msg.config(text="Quote line Comparision completed and downloaded Successfully")
