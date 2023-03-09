from datetime import datetime
from glob import glob
import pandas as pd
import numpy as np
import re,os,sys
import time


class final_sql():
    def __init__(self,path,root,compare_quote,compare_ql,msg,msg_remaining,start_compare,method,start):
        self.path=path
        self.root=root
        self.data_false=compare_quote
        self.QL_data_false=compare_ql
        self.msg=msg
        self.msg_remaining=msg_remaining
        self.method=method
        self.msg.config(text="", fg='blue')
        self.msg_remaining.config(text="", fg='blue')
        self.start_compare = start_compare
        self.start=start
        os.chdir(path)

        try:
            self.data_false['type'] = 'Quote'
            self.QL_data_false['type'] = 'Quote_Lines'
            final_data_false = pd.concat([self.data_false, self.QL_data_false])
            final_data_false.drop('Formula/Table', axis=1, inplace=True)
            final_data_false['SF_value'] = final_data_false['SF_value'].apply(lambda x: str(x))
            final_data_false['Pdf_value'] = final_data_false['Pdf_value'].apply(lambda x: str(x))
            final_data_false['category'] = np.where(((final_data_false['Pdf_value'] == '') | (final_data_false['SF_value'] == '') | (final_data_false['Pdf_value'] == np.NaN)
                                                                | (final_data_false['Pdf_value'].isna()) | (
                                                                 final_data_false['Pdf_value'] == 'No') | (
                                                                 final_data_false['Pdf_value'] == 'nan') | (
                                                                 final_data_false['SF_value'] == 'nan') | (
                                                                 final_data_false['SF_value'] == 'NaN') | (
                                                                 final_data_false['SF_value'] == 'None') | (
                                                                 final_data_false['SF_value'] == np.NaN) | (
                                                                 final_data_false['SF_value'].isna()) | (
                                                                 final_data_false['SF_value'].str.contains('None')) | (
                                                                 final_data_false['Pdf_value'].str.contains('None')) | (
                                                                 final_data_false['SF_value'].str.contains('NaT'))), "Manual", None)
            final_data_false = final_data_false.reset_index().drop('index', axis=1)
            pdf_quote = pd.read_csv("Quote_data_pdf.csv")
            pdf=[]
            folder = r"*.pdf"
            for pdf_file in glob(folder):
                pdf.append((os.path.basename(pdf_file).split('_')[0], os.path.basename(pdf_file).split('_')[1]))


            final_data_false = pd.merge(final_data_false, pd.DataFrame(pdf,columns=["Quote_id","pdf_name"]), on="Quote_id",how='left')

            if method == 'Step 3: SALESFORCE Report':
                final_data_false.to_csv(r'final_comparison_SF.csv')
            elif method=='Step 3: SQL Queries(ON VPN)':
                final_data_false.to_csv(r'final_comparison_SQL.csv')


            end = time.time()

            self.msg_remaining.config(text="Completed Comparision in {0:.1f} minutes".format((end - self.start_compare) / 60),bg='#F5F5F5')
            self.msg.config(text="Total Time: {0:.1f} minutes".format((end - self.start) / 60),bg='#F5F5F5')
        except Exception as e:
            self.msg_remaining.config(text=e,fg='red')
            os.chdir(self.path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " <Type:" + str(exc_type) + "< Error:" + str(exc_obj) + "> < Line Number:" + str(exc_tb.tb_lineno) + "> < File:" + str(fname) + " > /n")
            file1.close()


