import sys
import pandas as pd
import numpy as np
from datetime import timedelta,datetime
import re,os
from tkinter import messagebox,HORIZONTAL,Label
import time


class compare_quote_sql():
    def __init__(self,path,root,msg,remaining,SF_data,pdf_data):

        self.sql_quote=SF_data
        self.datanet = pdf_data
        self.path=path
        self.root=root
        self.msg=msg
        self.msg_remaining=remaining

        self.msg.config(text="", fg='blue')
        self.msg_remaining.config(text="")
        self.start=time.time()
        print(self.sql_quote)
        os.chdir(path)
        self.Quote()

    def Quote(self):
        try:
            self.msg.config(text="Comparing Quote Data!!")
            sql_data = self.sql_quote

            sql_data['bill_to_address'] = sql_data['bill_to_address'].apply(lambda x: ''.join(filter(str.isalnum, str(x).lower())))
            # sql_data['bill_to_address_old']=sql_data['bill_to_address_old'].apply(lambda x:''.join(filter(str.isalnum, x.lower())))
            sql_data['sold_to_address'] = sql_data['sold_to_address'].apply(lambda x: ''.join(filter(str.isalnum, str(x).lower())))
            sql_data['sold_to_address_rep'] = sql_data['sold_to_address_rep'].apply(lambda x: ''.join(filter(str.isalnum, str(x).lower())))
            sql_data['Quote_id'] = sql_data['Quote_id'].apply(lambda x: str(x))
            period1 = []
            for i in range(0, len(sql_data)):
                period1.append(np.ceil((pd.to_datetime(sql_data['subscription_end_date'])[i] -
                                        pd.to_datetime(sql_data['subscription_startdate'])[i]).days / 30.417))
            sql_data['subscription_period_months'] = period1
            sql_data = sql_data.astype(object)
            sql_data['subscription_startdate'] = pd.to_datetime(sql_data['subscription_startdate'])
            sql_data['subscription_end_date'] = pd.to_datetime(sql_data['subscription_end_date'])
            sql_data['total_onetimefees'] = sql_data['total_onetimefees'].astype(float)
            sql_data['total_subscription_fees'] = sql_data['total_subscription_fees'].astype(float)
            sql_data['sold_to_company'] = sql_data['sold_to_company'].apply(lambda x: ''.join(filter(str.isalnum, str(x).lower())))
            sql_data['subscription_period_months'] = sql_data['subscription_period_months'].astype(float)
            sql_data['Master_Contract_Start_Date'] = pd.to_datetime(sql_data['Master_Contract_Start_Date'])
            sql_data['Master_Contract_End_Date'] = pd.to_datetime(sql_data['Master_Contract_End_Date'])
            sql_data['Revenue_Entity_Name'] = sql_data['Revenue_Entity_Name'].apply(
                lambda x: ''.join(filter(str.isalnum, str(x).lower())))
            sql_data['OTF'] = sql_data['OTF'].apply(lambda x: ''.join(filter(str.isalnum, str(x).lower())))
            sql_data['Renewal_End_Date']=pd.to_datetime(sql_data['Renewal_End_Date'])
            sql_data['Net']=sql_data['Net'].str.replace('Net',"").astype(float)

            if len(self.datanet)!=0:
                data_net = self.datanet
                data_net['Quote_id'] = data_net['Quote_id'].apply(lambda x: str(x))

                data_net['sold_to_address'] = data_net['sold_to_address'].astype(str).apply(lambda x:' '.join(dict.fromkeys(x.split())))
                data_net['sold_to_address'] = data_net['sold_to_address'].astype(str).apply(lambda x: ''.join(filter(str.isalnum, str(x).lower())))
                data_net['subscription_period_months'] = data_net['subscription_period_months'].astype(float)
                data_net['bill_to_address'] = data_net['bill_to_address'].astype(str).apply(lambda x:' '.join(dict.fromkeys(x.split())))
                data_net['bill_to_address'] = data_net['bill_to_address'].apply(lambda x: ''.join(filter(str.isalnum, str(x).lower().replace('billtoaddress', '').replace('soldtoaddresssalesrepresentativebilltocontactbilltoemailbilltophone', ''))))
                data_net['subscription_startdate'] = pd.to_datetime(data_net['subscription_startdate'])
                data_net['subscription_end_date'] = pd.to_datetime(data_net['subscription_end_date'])
                data_net['total_onetimefees'] = data_net['total_onetimefees'].apply(lambda x: re.findall(r"[-+]?\d+\.\d+", re.sub("[A-Z]", "", str(x).upper().replace(",", "")))[0]).astype(float)
                data_net['total_subscription_fees'] = data_net['total_subscription_fees'].apply(lambda x: re.findall(r"[-+]?\d+\.\d+", re.sub("[A-Z]", "", str(x).upper().replace(",", "")))[0]).astype(float)
                data_net['sold_to_company'] = data_net['sold_to_company'].apply(lambda x: ''.join(filter(str.isalnum, str(x).lower())))
                data_net['Revenue_Entity_Name'] = (data_net['subscriber'] + data_net['company_inc']).apply(lambda x: ''.join(filter(str.isalnum, str(x).lower())))
                data_net['Billing'] = data_net['Billing'].replace('Annually', 'Annual')
                data_net['OTF'] = data_net['OTF'].apply(lambda x: ''.join(filter(str.isalnum, str(x).lower())))
                data_net['Net'] = data_net['Net'].astype(float)
            else:
                data_net=pd.DataFrame(columns=['sold_to_company','Revenue_Entity_Name','sales_representative',	'sold_to_address',	'primary_contact',	'bill_to_email',	'primary_contact_email',	'bill_to_phone',	'primary_contact_phone',	'subscription_period',	'subscription_startdate',	'effective_date',	'total_subscription_fees',	'total_onetimefees',	'currency',	'total_contract_value',	'bill_to_address',	'subscriber',	'company_inc',	'subscription_end_date',	'signature',	'envelope_id',	'pdf',	'Quote_id',	'pdf_name',	'subscription_period_months',	'OTF',	'Billing',	'Net'])



            fse = []
            for i in range(0, len(pd.unique(data_net['Quote_id']))):
                try:
                    if data_net['subscription_period_months'][i] < 12:
                        fse.append(data_net['subscription_end_date'][i])
                    else:
                        if pd.DatetimeIndex(data_net['subscription_startdate']).month[i] > \
                                pd.DatetimeIndex(data_net['subscription_end_date']).month[i]:
                            fse.append(
                                data_net['subscription_end_date'][i].replace(year=(data_net['subscription_startdate'][i].year) + 1))
                        elif (pd.DatetimeIndex(data_net['subscription_startdate']).month[i] ==
                              pd.DatetimeIndex(data_net['subscription_end_date']).month[i]) & (
                                pd.DatetimeIndex(data_net['subscription_startdate']).day[i] >
                                pd.DatetimeIndex(data_net['subscription_end_date']).day[i]):
                            fse.append(
                                data_net['subscription_end_date'][i].replace(year=(data_net['subscription_startdate'][i].year) + 1))
                        else:
                            fse.append(
                                data_net['subscription_end_date'][i].replace(year=(data_net['subscription_startdate'][i].year)))
                except:
                    fse.append(pd.NaT)

            data_net['first_segment_term'] = fse
            data_net['currency'] = data_net['currency'].apply(lambda x: x[0:3] if len(x) > 3 else x)

            merge1 = pd.merge(data_net, sql_data, on='Quote_id', how='left')

            merge1['Amendment_check'] = np.where(
                (merge1['Type'] == 'Amendment') & (merge1['Master_Contract_Start_Date'] <= merge1['subscription_startdate_x']) & (
                            merge1['Master_Contract_End_Date'] == merge1['subscription_end_date_x']), 1,
                np.where(merge1['Type'] != 'Amendment', 'NA', 0))
            merge1['first_segment_end_date_check'] = np.where(
                (merge1['Type'] != 'Amendment') & (merge1['subscription_period_months_x'] % 12 == 0) & (
                    np.isnat(pd.to_datetime(merge1['first_segment_term_date']))), 1, np.where(
                    (merge1['subscription_period_months_x'] < 12) & (np.isnat(pd.to_datetime(merge1['first_segment_term_date']))),
                    1,
                    np.where(pd.to_datetime(merge1['first_segment_term']) == pd.to_datetime(merge1['first_segment_term_date']), 1,
                             0)))
            merge1['Renewal_End_Date'] = pd.to_datetime(merge1['Renewal_End_Date'])
            Renew_check = []
            for i in range(0, len(merge1)):
                if merge1['Type'][i] != 'Renewal':
                    Renew_check.append('NA')
                else:
                    Renew_check.append(merge1['Renewal_End_Date'][i] + timedelta(days=1) == merge1['subscription_startdate_x'][i])

            merge1['Renewal_date_check'] = Renew_check
            merge1['Renewal_date_check'] = merge1['Renewal_date_check'].apply(
                lambda x: str(x).replace('False', '0').replace('True', '1'))

            cols = [col.replace('_x', '') for col in merge1.columns if '_x' in col]
            for i in cols:
                    print(i, merge1[i + '_x'].dtype, merge1[i + '_y'].dtype)
                    merge1[i + '_x'] = merge1[i + '_x'].astype(merge1[i + '_y'].dtype)
                    merge1[i + '_y'] = merge1[i + '_y'].astype(merge1[i + '_y'].dtype)
                    print(i, merge1[i + '_x'].dtype, merge1[i + '_y'].dtype)

            check1 = pd.DataFrame(columns=cols, index=range(0, len(merge1['Quote_id'])))
            for i in cols:
                for j in range(0, len(merge1['Quote_id'])):
                    if (i == 'bill_to_address') and (str(merge1[i + '_y'][j]) in str(merge1[i + '_x'][j])):
                        check1[i][j] = 1
                    elif (i == 'sold_to_address') and (str(merge1[i + '_y'][j]) in str(merge1[i + '_x'][j])):
                        check1[i][j] = 1
                    #         elif (i!='bill_to_address') and (i!='sold_to_address') and (merge1[i+'_x'].dtype== 'O' or merge1[i+'_y'].dtype== 'O') and (i!='Net') and (SequenceMatcher(a=str(merge1[i+'_x'][j]),b=str(merge1[i+'_y'][j])).ratio() > 0.80):
                    # #             print(i)
                    #             check1[i][j]=1
                    elif (i != 'bill_to_address') and (i != 'sold_to_address') and (merge1[i + '_x'][j] == merge1[i + '_y'][j]):
                        check1[i][j] = 1
                    else:
                        check1[i][j] = 0

            check1

            final1 = pd.concat([check1, merge1[
                ['signature', 'Amendment_check', 'first_segment_end_date_check', 'Renewal_date_check', 'Quote_id']]], axis=1)
            final1

            table_zero = {'sold_to_company': [], 'sold_to_address': [], 'bill_to_email': [], 'subscription_startdate': [],
                          'total_subscription_fees': [], 'total_onetimefees': [], 'currency': [],
                          'bill_to_address': [], 'subscription_end_date': [],
                          'subscription_period_months': [], 'Net': [], 'OTF': [], 'Billing': [],
                          'Revenue_Entity_Name': [], 'signature': [], 'Amendment_check': [],
                          'first_segment_end_date_check': [], 'Renewal_date_check': [], 'Quote_id': []}

            for i in table_zero.keys():
                for j in range(0, len(final1)):
                    if (final1[i][j] == 0) | (final1[i][j] == 'No') | (final1[i][j] == 'Partial') | (final1[i][j] == '0'):
                        table_zero[i].append(final1['Quote_id'][j])

            table_zero = {k: v for k, v in table_zero.items() if v}

            unlist_cols = ['signature', 'Amendment_check', 'first_segment_end_date_check', 'Renewal_date_check', 'bill_to_address']

            table_zero


            def coalesce(x, y):
                if (len(x) == 0) & (y == None):
                    return None
                elif (len(x) != 0) & (y == None):
                    return x.to_list()[0]
                elif (len(x) == 0) & (y != None):
                    return None
                elif (len(x) == 0) & (y == 'Renewal_End_Date'):
                    return None
                elif x[y].to_list()[0] == None:
                    return None
                elif (len(x) != 0) & (y == 'Renewal_End_Date'):
                    return x[y].to_list()[0] + timedelta(days=1)
                elif (len(x) != 0) & (y == 'bill_to_address'):
                    return x[y].to_list()[0]
                else:
                    return x[y].to_list()[0]


            final_list = []
            for i in table_zero.keys():
                for j in range(0, len(table_zero[i])):
                    if i not in unlist_cols:
                        print(i, j)
                        final_list.append((table_zero[i][j], i, data_net[data_net['Quote_id'] == table_zero[i][j]][i].to_list()[0]
                                           , coalesce(sql_data[sql_data['Quote_id'] == table_zero[i][j]][i], None), None))
                    elif i == 'bill_to_address':
                        print(i, j)
                        final_list.append((table_zero[i][j], i, data_net[data_net['Quote_id'] == table_zero[i][j]][i].to_list()[0]
                                           , coalesce(sql_data[sql_data['Quote_id'] == table_zero[i][j]], 'bill_to_address')
                                           ))

                    elif i == 'Amendment_check':
                        final_list.append((table_zero[i][j], i
                                           , {'subscription_startdate': data_net[data_net['Quote_id'] == table_zero[i][j]][
                            'subscription_startdate'].to_list()[0]
                                               , 'subscription_end_date': data_net[data_net['Quote_id'] == table_zero[i][j]][
                                'subscription_end_date'].to_list()[0]
                                              },
                                           {'Master_Contract_Start_Date': coalesce(
                                               sql_data[sql_data['Quote_id'] == table_zero[i][j]], 'Master_Contract_Start_Date')
                                               ,
                                            'Master_Contract_End_Date': coalesce(sql_data[sql_data['Quote_id'] == table_zero[i][j]],
                                                                                 'Master_Contract_End_Date')
                                            },
                                           """if Type=Amendment & (Master_Contract_Start_Date <= subscription_startdate) & (Master_Contract_End_Date = subscription_end_date)
                                              then True else False"""
                                           ))
                    elif i == 'first_segment_end_date_check':
                        final_list.append((table_zero[i][j], i, {'pdf_first_segment_term':
                                                                     data_net[data_net['Quote_id'] == table_zero[i][j]][
                                                                         'first_segment_term'].to_list()[0]
                            , 'Subscription_period': data_net[data_net['Quote_id'] == table_zero[i][j]][
                                'subscription_period_months'].to_list()[0]
                            , 'subscription_startdate': data_net[data_net['Quote_id'] == table_zero[i][j]][
                                'subscription_startdate'].to_list()[0]
                            , 'subscription_end_date': data_net[data_net['Quote_id'] == table_zero[i][j]][
                                'subscription_end_date'].to_list()[0]}
                                           , {'SF_first_segment_term_date': coalesce(
                            sql_data[sql_data['Quote_id'] == table_zero[i][j]], 'first_segment_term_date')}
                                           , """if((Type!=Amendment) & (subscription_period_months % 12==0) & (np.isnat(pd.to_datetime(first_segment_term_date))) 
                                            then True
                                            else if((Type==Amendment) & (subscription_period_months_x<12) & (np.isnat(pd.to_datetime(first_segment_term_date))) 
                                            then True
                                            else if(pd.to_datetime(first_segment_term)==pd.to_datetime(first_segment_term_date) 
                                            then True
                                            else False"""
                                           ))

                    elif i == 'Renewal_date_check':
                        final_list.append((table_zero[i][j], i
                                           , {'subscription_startdate': data_net[data_net['Quote_id'] == table_zero[i][j]][
                            'subscription_startdate'].to_list()[0]}
                                           , {'Renewal_End_Date+timedelta(days=1)': coalesce(
                            sql_data[sql_data['Quote_id'] == table_zero[i][j]], 'Renewal_End_Date')}
                                           ,
                                           """if Type=Renewal & Renewal_End_Date+timedelta(days=1)=subscription_startdate then True else False"""

                                           ))
                    else:
                        final_list.append(
                            (table_zero[i][j], i, data_net[data_net['Quote_id'] == table_zero[i][j]]['signature'].to_list()[0]))

            data_false = pd.DataFrame(final_list, columns=['Quote_id', 'Field', 'Pdf_value', 'SF_value', 'Formula/Table'])
            data_false['Pdf_value'] = data_false['Pdf_value'].apply(
                lambda x: str(x).replace("{", "").replace("}", "").replace("'", ""))
            data_false['SF_value'] = data_false['SF_value'].apply(
                lambda x: str(x).replace("{", "").replace("}", "").replace("'", ""))
            print(data_false)
            data_false.to_csv(r'Compare_Quote_sql.csv')
            self.msg.config(text="Comparision completed and downloaded Successfully")
        except Exception as e:
            self.msg_remaining.config(text=e,fg='red')
            os.chdir(self.path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " <Type:" + str(exc_type) + "< Error:" + str(exc_obj) + "> < Line Number:" + str(exc_tb.tb_lineno) + "> < File:" + str(fname) + " > /n")
            file1.close()
            raise

