import math
from concurrent.futures import ThreadPoolExecutor
import aspose.words as aw
from tqdm import tqdm as tqdm
import glob, os
import pandas as pd
import numpy as np
import re
import lxml
from tkinter import messagebox,HORIZONTAL,Label
from tkinter.ttk import Progressbar
from tkinter import  Tk
from Progress_bar import bar
import time
import logging
from datetime import datetime
import sys

class Quote_lines():

    def __init__(self, path,root,msg,remaining,progress_msg,start):

        self.rsf_cont_list = ['ADDITIONAL SERVICES INCLUDED AT NO ADDITIONAL INVESTMENT', 'One Time Fees',
                              'Tiered Subscription Fees', 'Contact Type', 'Activity', 'Frais ponctuels', 'Nom',
                              'SERVICES SUPPLÉMENTAIRES INCLUS SANS INVESTISSEMENT SUPPLÉMENTAIRE',
                              'Frais d’abonnement différenciés', 'None', 'Implementation Consultant', 'Employee Bucket',
                              '7,501-15,000 EE', 'SUMMARY OF FEES', 'Issue Severity', 'Company', 'Service',
                              'Deliverable']

        self.SOF_list = ['SUMMARY OF FEES', 'One Time Fees']

        self.path = path
        self.html_path = path
        self.root=root
        self.msg=msg
        self.msg_remaining = remaining
        self.progress_msg = progress_msg
        self.msg_remaining.config(text="", fg='blue')
        self.msg.config(text="", fg='blue')
        self.progress_msg.config(text="")
        self.msg.config(text="Fetching Quote-lines data !!")

        self.start = start


    def pdf_to_html(self):

            try:
            # Change default directory to path

                os.chdir(self.path)
                folder = r"*.pdf"

                list = []
                for pdf_file in glob.glob(folder):
                    list.append(os.path.basename(pdf_file))
                # bar(self.root)
                self.msg_remaining.config(text="")
                self.msg.config(text="Converting pdf files to HTML... !!")

                p=0
                for i in tqdm(list):

                    # Load the PDF file
                    doc = aw.Document(self.path + "\{}".format(i))
                    # Save the document as HTML in html_path
                    doc.save(self.html_path + "\{}.html".format(i.replace(".pdf", "")))
                    p = p + 1
                    # percent=math.floor(p/ len(list))*100
                    hash='#'*p
                    self.msg_remaining.config(text="{}/{} Completed".format(p,len(list)))


                self.msg.config(text="Pdf files converted to html successfully !!")
                print("Pdf files converted to html successfully")
                # html_msg = Label(self.root, text='Converted to HTML Successfully !!', fg='blue', bg='white')
                # html_msg.pack(side='left', pady=(5, 5))
                # html_msg.config(font=('verdana', 12))
            except Exception as e:
                self.progress_msg.config(text=e)
                os.chdir(self.path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " <Type:" + str(exc_type) + "< Error:" + str(exc_obj) + "> < Line Number:" + str(exc_tb.tb_lineno) + "> < File:" + str(fname) + " > /n")
                file1.close()
                raise

    def html_to_table(self):


        # Change default directory to html_path
        try:

            print("Tables Creating")
            self.msg_remaining.config(text="")
            self.msg.config(text="Getting Tables data !!")
            os.chdir(self.html_path)

            folder = r"*.html"

            list_html = []

            for html_file in glob.glob(folder):
                list_html.append(os.path.basename(html_file))

            table = []
            invalid_pdfs = []
            p=0
            for i in tqdm(list_html):
                try:
                    for j in range(len(pd.read_html(i))):
                        table.append((pd.read_html(i)[j].iloc[0, 0], pd.read_html(i)[j].iloc[0, 1], i, j))
                    p = p + 1
                    # percent = math.floor(p / len(list_html)) * 100
                    # hash = '#' * p
                    self.msg_remaining.config(text="{}/{} Completed ".format( p, len(list_html)))
                except:
                    invalid_pdfs.append(i)

            pd.DataFrame(invalid_pdfs).to_csv('invalid_pdfs_ql.csv')
            df = pd.DataFrame(table)
            df.columns = ['Table', 'Table1', 'pdf', 'index']
            df.iloc[:, 0] = df.iloc[:, 0].fillna('None')
            df.to_csv('Tables.csv')
            print(df)
            self.msg.config(text="Tables data Created & downloaded successfully !!")

        except Exception as e:
            self.progress_msg.config(text=e)
            os.chdir(self.path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " <Type:" + str(exc_type) + "< Error:" + str(exc_obj) + "> < Line Number:" + str(exc_tb.tb_lineno) + "> < File:" + str(fname) + " > /n")
            file1.close()
            raise

        return df

    def rsf_otf(self, df):
        try:
            RSF = pd.DataFrame()
            self.msg_remaining.config(text="")
            self.msg.config(text="Fetching Quote_lines data of pdfs !!")
            p=0
            os.chdir(self.path)
            print(df)


            for i in tqdm(range(len(df))):
                if df['Table'][i] not in self.rsf_cont_list:
                    RSF1 = pd.read_html(df['pdf'][i])[df['index'][i]]
                    RSF1['3'] = df['pdf'][i].replace('.html', '')
                    RSF1['4'] = df['pdf'][i].split("_")[0]
                    RSF1 = RSF1[RSF1[0].str.contains("Name|Recurring Subscription Fees|Recurring Integration Fees|Total|Recurring Service Subscription Fees") == False]
                    RSF = pd.concat([RSF, RSF1])
                    p = p + 1
                    # percent = math.floor(p / len(df)) * 100
                    # hash = '#' * p
                    self.msg_remaining.config(text="{}/{} Completed ".format( p, len(df)))

                # bar(self.root)
            RSF = RSF.T.reset_index(drop=True).T
            RSF = RSF.reset_index().drop(['index', 1], axis=1)

            i = len(RSF.columns)

            while i > 4:
                RSF = RSF.drop(RSF[RSF[i].notna()].index, axis=0)
                RSF = RSF.drop(i, axis=1)
                i -= 1

            RSF = RSF.reset_index(drop=True)

            RSF.columns = ['Name', 'Quantity', 'pdf','Quote_id']
            RSF['Quantity'] = RSF['Quantity'].fillna(1)
            RSF = RSF[(RSF['Quantity'].str.contains(r'[$A-Za-z]') == False) | (RSF['Quantity'].str.contains(r'[$A-Za-z]').isna())].reset_index().drop('index', axis=1)
            RSF['Quantity'] = RSF['Quantity'].astype(float)
            RSF['Name']=RSF['Name'].apply(lambda x: 'Connector' if 'Configurability Integration' in x else 'Connector' if 'Prime Connector' in x else 'Connector' if 'Standard Integration' in x else x)

            RSF.to_csv('Quote_lines_pdf.csv')
            print("RSF_OTF data processed sucessfully")



        except Exception as e:
            self.progress_msg.config(text=e)
            os.chdir(self.path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " <Type:" + str(exc_type) + "< Error:" + str(exc_obj) + "> < Line Number:" + str(exc_tb.tb_lineno) + "> < File:" + str(fname) + " > /n")
            file1.close()
            raise
        return RSF

    def SOF_func(self, df):
        try:
            SOF = pd.DataFrame()
            p = 0
            os.chdir(self.path)
            self.msg.config(text="Fetching Summary of Fees data of pdfs!!")
            for i in tqdm(range(len(df))):
                if df['Table'][i] in self.SOF_list:
                    SOF1 = pd.read_html(df['pdf'][i])[df['index'][i]]
                    SOF1['pdf'] = df['pdf'][i]
                    SOF1 = SOF1[
                        SOF1[0].str.contains("One Time Fees|Recurring Subscription Fees|SUMMARY OF FEES") == False]
                    SOF = pd.concat([SOF, SOF1])
                p = p + 1
                percent = math.floor(p / len(df)) * 100
                # hash = '#' * p
                self.msg_remaining.config(text="{}/{} Completed ".format(p, len(df)))

            SOF = SOF.reset_index().drop("index", axis=1)

            SOF['OTF'] = ''
            SOF['Net'] = ''
            SOF['Billing'] = ''

            for i in tqdm(range(len(SOF))):
                # for even&odd number index
                if i % 2 == 0:
                    try:
                        SOF['OTF'][i] = SOF[1][i].split(":")[2]
                    except:
                        SOF['OTF'][i] = 'None'
                else:
                    try:
                        SOF['Net'][i] = float(re.findall(r'\d+', ''.join(SOF[1][i].split(":")[1].split(" ")))[0])
                    except:
                        SOF['Net'][i] = np.NaN
                    try:
                        SOF['Billing'][i] = SOF[1][i].split(":")[2].split(" ")[1]
                    except:
                        SOF['Billing'][i] = 'None'

            SOF['OTF'] = SOF['OTF'].replace('', np.NaN).fillna(method='ffill')
            SOF['Billing'] = SOF['Billing'].replace('', np.NaN).fillna(method='bfill')
            SOF['Net'] = SOF['Net'].replace('', np.NaN).fillna(method='bfill')
            SOF = SOF.drop([0, 1], axis=1)
            SOF['pdf'] = SOF['pdf'].str.replace(".html", "")
            SOF['Quote_id'] = SOF['pdf'].apply(lambda x: x.split("_")[0])
            SOF = SOF.drop_duplicates().reset_index().drop("index", axis=1)
            # SOF_msg = Label(self.root, text='Downloaded SOF of pdfs Successfully !!', fg='blue', bg='white')
            # SOF_msg.pack(side='top', pady=(5, 5))
            # SOF_msg.config(font=('verdana', 12))
            print(SOF)
            # self.msg.config(text="Fetched Quote & Quote_lines data of pdfs Successfully !!")
            # self.msg.config(text="Data successfully downloaded to {}!!".format(self.path))
            # end=time.time()
            # self.msg_remaining.config(text="Completed in {0:.1f} minutes" .format((end - self.start)/60))
            print("SOF data processed sucessfully")
            SOF.to_csv('SOF_pdf.csv')
        except Exception as e:
            self.progress_msg.config(text=e)
            os.chdir(self.path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " <Type:" + str(exc_type) + "< Error:" + str(exc_obj) + "> < Line Number:" + str(exc_tb.tb_lineno) + "> < File:" + str(fname) + " > /n")
            file1.close()
            raise
        return SOF

    def TSF_func(self, df):
        try:
            TSF = pd.DataFrame()
            p = 0
            os.chdir(self.path)
            self.msg.config(text="Fetching ARR of pdfs!!")
            for i in tqdm(range(len(df))):
                if df['Table'][i] == "Tiered Subscription Fees":
                    TSF1 = pd.read_html(df['pdf'][i])[df['index'][i]]
                    TSF1.columns = TSF1.iloc[1, :]
                    TSF1 = TSF1.drop([0, 1], axis=0)
                    TSF1['pdf'] = df['pdf'][i]
                    TSF1['pdf'] = TSF1['pdf'].str.replace(".html", "")
                    TSF1['Quote_id'] = TSF1['pdf'].apply(lambda x: x.split("_")[0])
                    try:
                        TSF1 = TSF1[['Total Fees by Tier', 'Quote_id']]
                        TSF1 = TSF1.groupby('Quote_id')['Total Fees by Tier'].max().reset_index()
                        # TSF1['Top Tier ARR Currency']=TSF1['Total Fees by Tier'].apply(lambda x: x.split(" ")[0])
                        TSF1['Top Tier ARR'] = TSF1['Total Fees by Tier'].apply(lambda x: x.replace(",",""))
                        TSF1=TSF1.drop(['Total Fees by Tier'],axis=1)
                        TSF = pd.concat([TSF, TSF1])
                    except:
                        continue
                p = p + 1
                percent = math.floor(p / len(df)) * 100
                # hash = '#' * p
                self.msg_remaining.config(text="{}/{} Completed ".format(p, len(df)))


            print(TSF)

            print("TSF data processed sucessfully")
            TSF.to_csv('ARR_pdf.csv')
            if TSF.empty:

                self.msg.config(text="Fetched Quote & Quote_lines data of pdfs Successfully !!")
                self.msg.config(text="Data successfully downloaded to {}!!".format(self.path))
                end = time.time()
                self.msg_remaining.config(text="Completed in {0:.1f} minutes".format((end - self.start) / 60))

            else:
                data = pd.read_csv('Quote_data_pdf.csv')
                data = pd.merge(data, TSF, on="Quote_id", how="left")
                data.to_csv('Quote_data_pdf.csv')
                self.msg.config(text="Fetched Quote & Quote_lines data of pdfs Successfully !!")
                self.msg.config(text="Data successfully downloaded to {}!!".format(self.path))
                end = time.time()
                self.msg_remaining.config(text="Completed in {0:.1f} minutes".format((end - self.start) / 60))

        except Exception as e:
            self.progress_msg.config(text=e)
            os.chdir(self.path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " <Type:" + str(exc_type) + "< Error:" + str(exc_obj) + "> < Line Number:" + str(exc_tb.tb_lineno) + "> < File:" + str(fname) + " > /n")
            file1.close()
            raise


    def clean_folder(self):
        try:
            test = os.listdir(self.path)
            for item in test:
                if item.endswith(".png") | item.endswith(".html") | item.endswith(".jpeg") :#| item.endswith(".pdf")
                    os.remove(os.path.join(self.path, item))
        except Exception as e:
            self.progress_msg.config(text=e)
            os.chdir(self.path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " <Type:" + str(exc_type) + "< Error:" + str(exc_obj) + "> < Line Number:" + str(exc_tb.tb_lineno) + "> < File:" + str(fname) + " > /n")
            file1.close()



