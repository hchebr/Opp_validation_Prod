import logging
import math
from concurrent.futures import ThreadPoolExecutor
import PyPDF2 as pypdf
import pandas as pd
import numpy as np
from pdfminer.high_level import extract_text
import os,sys
from datetime import datetime
import datefinder
import dateutil.parser as dparser
import warnings
warnings.filterwarnings("ignore")
from difflib import SequenceMatcher
import glob
from tqdm import tqdm as tqdm
import lxml
from tkinter import messagebox,HORIZONTAL,Label
from Progress_bar import bar
from tkinter import  Tk

class Quote():
    def __init__(self,path,root,msg,msg_remaining,progress_msg):
        self.path = path
        self.root=root
        self.msg=msg
        self.msg_remaining=msg_remaining
        self.progress_msg=progress_msg
        self.msg_remaining.config(text="", fg='blue')
        self.msg.config(text="", fg='blue')
        self.progress_msg.config(text="")
        os.chdir(self.path)
        folder = r"*.pdf"
        self.list = []


        for pdf_file in glob.glob(folder):
            self.list.append(os.path.basename(pdf_file))

        self.values = []
        self.pdf1 = []
        self.invalid_pdfs = []
        self.columns = ['Sold_To_Company', 'Sales_Representative', 'Bill_To_Address', 'Sold_To_Address', 'Bill_To_Contact',
                   'Primary_Contact' \
            , 'Bill_To_Email', 'Primary_Contact_Email', 'Bill_To_Phone', 'Primary_Contact_Phone', \
                   'SUBSCRIPTIONDETAILS', 'Subscription_Period', 'Subscription_StartDate', 'Effective_Date',
                   'Total_Subscription_Fees', \
                   'Total_OneTimeFees', 'Currency', 'Total_Contract_Value', 'Bill_To_Address_updated', 'Subscriber',
                   'Company_inc', 'Subscription_End_Date', 'Signature', 'Envelope_id']

        self.keys= ['SoldToCompany', 'SalesRepresentative', 'BillToAddress', 'SoldToAddress', 'BillToContact',
                    'PrimaryContact', 'BillToEmail', \
                    'PrimaryContactEmail', 'BillToPhone', 'PrimaryContactPhone', 'SUBSCRIPTIONDETAILS', \
                    'SubscriptionPeriod', 'SubscriptionStartDate', 'EffectiveDate', 'TotalSubscriptionFees', \
                    'TotalOne-TimeFees', 'Currency', 'RecurringSubscriptionFees']

    def convert(self,i,text1):
        try:
            pdfFileObject = open(r'' + i, 'rb')
            pdfReader = pypdf.PdfFileReader(pdfFileObject)
            pageObject = pdfReader.getPage(0)
            text = "".join([pageObject.extractText()])
            text = text.replace("\n", "").replace(" ", "")
        except:
            print(i)
        finally:
            list2 = []
            for i in self.keys:
                try:
                    list2.append(text.index(i))
                except:
                    list2.append(text1.index(i))

            list3 = []
            for i in range(0, len(list2) - 1):
                try:
                    list3.append(text[list2[i]:list2[i + 1]].split(":")[1])

                except:
                    try:
                        list3.append(text1[list2[i]:list2[i + 1]].split(":")[1])
                    except:
                        list3.append(np.NaN)
            #                 continue

            try:
                list3.append(text1[text1.index('TotalContractValue:'):text1.index('.', text1.index(
                    'TotalContractValue:')) + 3].split(":")[1])
            except:
                list3.append(np.NaN)
            try:
                list3.append((text1[text1.index(
                    text[text.index('BillToAddress'):text.index('SoldToAddress')].split(":")[1]):text1.index(
                    text[text.index('BillToContact'):text.index('PrimaryContact')].split(":")[1])].replace(
                    'SalesRepresentative:SoldToAddress:BillToContact:BillToEmail:BillToPhone:', '')))
            except:
                list3.append(np.NaN)
            try:
                list3.append(text1[text1.index('Subscriber:'):text1.index('Signature:')].split(':')[1])
            except:
                list3.append(np.NaN)
            try:
                list3.append(text1[text1.index('OrderFormbackto'):text1.index(',Subscriber')].split('OrderFormbackto')[1])
            except:
                list3.append(text1[text1.index('OrderFormbackto'):].split(',')[0].replace('OrderFormbackto', ''))
            try:
                list3.append([match.date() for match in datefinder.find_dates(
                    text1[text1.index('SubscriptionEndDate:'):].replace(',', ' ').split(":")[1])][0])
            except:
                list3.append(dparser.parse(text1[text1.index('SubscriptionEndDate:'):].split(":")[1].replace(",", "-"),
                                           fuzzy=True).date())

            try:
                list3.append('Yes' if len(text1[
                                          text1.index('DocuSignEnvelopeID:', text1.index('Signature:\\')) + 55:text1.index(
                                              '\x0c', text1.index('Signature:\\'))]) > 90 else 'Partial')
            except:
                list3.append('No')
            try:
                list3.append(
                    text1[text1.index('DocuSignEnvelopeID'):(text1.index('DocuSignEnvelopeID') + 55)].split(":")[1])
            except:
                list3.append(np.NaN)

            return list3

    def Quote_data(self):
            try:
                self.msg.config(text="Syncing...")

                self.msg.config(text="Fetching Quote Data from PDF's...")
                p=0
                for i in tqdm(self.list):
                    try:
                        text1 = extract_text(r"" + i).replace("\n", "").replace(" ", "")
                        self.values.append(self.convert(i,text1))
                        self.pdf1.append(i)
                        # percent = math.floor(p / len(self.list)) * 100
                        # hash = '#' * p
                    except:
                        self.invalid_pdfs.append(i)
                    p = p + 1
                    self.msg_remaining.config(text="{}/{} Completed ".format(p, len(self.list)))

                # bar(self.root)

                data = pd.DataFrame(columns=[each_string.lower() for each_string in self.columns], index=range(0, len(self.values)))

                for i in tqdm(range(0, len(self.values))):
                    data.iloc[i] = self.values[i]

                data['pdf'] = pd.unique(self.pdf1)

                quote = []
                for i in pd.unique(self.pdf1):
                    quote.append(i.split('_')[0])

                data['Quote_id'] = quote

                pdf_name = []
                for i in pd.unique(self.pdf1):
                    pdf_name.append(i[19:])

                data['pdf_name'] = pdf_name
                data['subscription_startdate'] = data['subscription_startdate'].apply(lambda x: str(x).replace('SubscriptionEndDate', ''))
                data['subscription_startdate'] = data['subscription_startdate'].fillna('').apply(lambda x: datetime.strptime(x, "%B%d,%Y").date() if x != '' else x)
                data.drop('bill_to_address', axis=1, inplace=True)
                data.rename(columns={'bill_to_address_updated': 'bill_to_address'}, inplace=True)
                period = []
                for i in range(0, len(data)):
                    period.append(np.ceil((pd.to_datetime(data['subscription_end_date'])[i] - pd.to_datetime(data['subscription_startdate'])[i]).days / 30.417))
                data['subscription_period_months'] = period
                data['bill_to_address']=data['bill_to_address'].str.replace("BillToAddress","")
                data.drop(['bill_to_contact','subscriptiondetails'],axis=1,inplace=True)
                # invalid_data = []

                # for i in range(0, len(data)):
                #     print(max(data.isna().sum(axis=1)))
                #     print(data.isna().sum(axis=1)[i])
                #     print((SequenceMatcher(a=str(data['sold_to_company'][i].lower()), b=str(data['pdf_name'][i].lower().replace(' ', '').replace('.pdf', ''))).ratio() < 0.90))
                #     if max(data.isna().sum(axis=1)) > 0:
                #         if (max(data.isna().sum(axis=1)) == data.isna().sum(axis=1)[i]):
                #             invalid_data.append(i)
                #             self.invalid_pdfs.append(data['pdf'].iloc[i])
                # #
                # print(invalid_data)
                # # data = data.drop(invalid_data, axis=0).reset_index().drop('index', axis=1)
                data.to_csv("Quote_data_pdf.csv")
                pd.DataFrame(self.invalid_pdfs).to_csv("invalid_pdfs_quote.csv")
                self.msg.config(text="Fetched Quote Data from PDF's !!")
            except Exception as e:
                self.progress_msg.config(text=e)
                os.chdir(self.path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " <Type:" + str(exc_type) + "< Error:" + str(exc_obj) + "> < Line Number:" + str(exc_tb.tb_lineno) + "> < File:" + str(fname) + " > /n")
                file1.close()
                raise
            # return data

