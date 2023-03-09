import pyodbc
from Quote import *

class Queries():
    def __init__(self,username,password,child_root,data,msg,path):

        child_msg = Label(child_root, text="", fg='blue', bg='#F5F5F5')
        child_msg.pack(side='top', pady=(10, 5))
        child_msg.config(font=('verdana', 12))
        try:
            child_msg.config(text='')
            child_msg.config(text="Connecting to SQL Server...")
            self.path=path
            os.chdir(path)
            self.msg=msg
            self.data=data

            self.username=username
            self.__password=password
            self.server = 'Sqlsvr-eu2-iris-prod.database.windows.net'
            self.database = 'salesforce_icims'
            self.driver = '{ODBC Driver 17 for SQL Server}'
            self.azure = 'ActiveDirectoryPassword'

            child_msg.config(text="Connecting to SQL Server... It may take few minutes, please wait!!")
            self.cnxn = pyodbc.connect('DRIVER=' +  self.driver + ';PORT=1433;SERVER=' + self.server + ';PORT=1443;DATABASE=' + self.database + ';UID=' + self.username + ';PWD=' + self.__password + ';AUTHENTICATION=' + self.azure + ';ApplicationIntent=ReadOnly')
            print('Connection Successful !!')
            child_msg.config(text='Login Successful !!')
        except Exception as e:
            child_msg.config(text=e,fg='Red', bg='white',wraplength=500, justify="center")
            raise

        try:
            child_root.destroy()
            self.msg.config(text='')
            self.sql_quote()
        except Exception as e:
            self.msg.config(text=e,fg='Red', bg='white')
            os.chdir(self.path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " <Type:" + str(exc_type) + "< Error:" + str(exc_obj) + "> < Line Number:" + str(exc_tb.tb_lineno) + "> < File:" + str(fname) + " > /n")
            file1.close()


    def get_server(self):
        return self.server

    def get_database(self):
        return self.database

    def get_driver(self):
        return self.driver

    def get_azure(self):
        return self.azure

    def update_server(self,new_server):
        self.server=new_server

    def update_database(self,new_database):
        self.database=new_database

    def update_driver(self,new_driver):
        self.driver=new_driver

    def update_azure(self,new_azure):
        self.azure=new_azure


    def sql_quote(self):
            print('Reading Quote SQL Query !!')
            self.msg.config(text='Reading Quote SQL Query !!')
            sql="""SELECT distinct 
            vw_q.[ID] as Quote_id,
            vw_q.[SBQQ__Opportunity2__c] as Opportunity_id,
            vw_q.[CurrencyIsoCode] as currency,
            vw_q.[SBQQ__StartDate__c] as subscription_startdate,
            
            -- vw_q.[End Date] as subscription_end_date,
            vw_q.[SBQQ__FirstSegmentTermEndDate__c] as first_segment_term_date,
            vw_q.[SBQQ__MasterContract__c],
            vw_q.[Billing_Frequency__c] as Billing,
            vw_q.[SBQQ__PaymentTerms__c] as Net,
            Concat(B.[QS_Street_1__c],B.[QS_City__c],B.[QS_State__c],B.[QS_Zip_Postal_Code__c]) as bill_to_address,
            -- Concat(vw_q.[SBQQ__BillingStreet__c],vw_q.[SBQQ__BillingCity__c],vw_q.[SBQQ__BillingState__c],vw_q.[SBQQ__BillingPostalCode__c] ) as bill_to_address_old,
            -- Concat(vw_q.[SBQQ__ShippingStreet__c],vw_q.[SBQQ__ShippingCity__c],vw_q.[SBQQ__ShippingState__c],vw_q.[SBQQ__ShippingPostalCode__c],vw_q.[SBQQ__ShippingCity__c],vw_q.[SBQQ__ShippingState__c],vw_q.[SBQQ__ShippingPostalCode__c] ) as sold_to_address_rep,
            -- Concat(vw_q.[SBQQ__ShippingStreet__c],vw_q.[SBQQ__ShippingCity__c],vw_q.[SBQQ__ShippingState__c],vw_q.[SBQQ__ShippingPostalCode__c]) as sold_to_address,
            Concat(S.[QS_Street_1__c],S.[QS_City__c],S.[QS_State__c],S.[QS_Zip_Postal_Code__c]) as sold_to_address,
            Concat(S.[QS_Street_1__c],S.[QS_City__c],S.[QS_State__c],S.[QS_Zip_Postal_Code__c],S.[QS_City__c],S.[QS_State__c],S.[QS_Zip_Postal_Code__c] ) as sold_to_address_rep,
            -- vw_q.[Subscription Term] as subscription_period_months,
            vw_q.[SBQQ__Type__c] as Type,
            coalesce(ox.[Contract ID],vw_q.[SBQQ__MasterContract__c]) as Contract_ID,
            --c.[Contract ID],
            ox.[Renewed Contract ID] as Renewed_Contract_ID,
            case when vw_q.[SBQQ__MasterContract__c] is not null then  c.[Contract Start Date] else null end as Master_Contract_Start_Date,
            case when vw_q.[SBQQ__MasterContract__c] is not null then c.[Contract End Date] else null end as Master_Contract_End_Date,
            re.[Revenue Entity Name] as Revenue_Entity_Name,
            
            --c.[Contract Name] as Renewal_contract_name,
            case when vw_q.[SBQQ__Type__c] = 'Renewal' then c.[Contract End Date]  else null end as Renewal_End_Date,
            case when vw_q.[SBQQ__Type__c] = 'Renewal' then c.[Renewal Start Date] else null end as Renewal_Start_Date,
            case when vw_q.[SBQQ__Type__c] = 'Renewal' then o.[Close Date] else null end as Renewal_opp_end_date,
            case when vw_q.[SBQQ__Type__c] = 'Renewal' then o.[Opportunity Created Date] else null end as Renewal_opp_created_date,
            vw_q.[Total_Subscriptions_RU__c] as total_subscription_fees,
            vw_q.[Total_One_Time_Fees__c] as total_onetimefees,
            vw_q.[Account_Name_Value__c] as sold_to_company,
            vw_q.[Bill_to_Email__c] as bill_to_email,
            vw_q.[End_Date_Calculated__c] as subscription_end_date,
            vw_q.[One_Time_Payment_Plan_Detail__c] as OTF 
            
            
            FROM [dbo].[vw_SF_Quote_CUR] vw_q
            left join [dbo].[vw_Opportunity_Contract_Xref] ox on ox.[Quote ID]=vw_q.[ID] and ox.[Renewed Contract ID] is not Null
            left join [dmsf].[vw_dimOpportunity] o on o.[Opportunity ID]=vw_q.[SBQQ__Opportunity2__c]
            left join [dmsf].[vw_dimRevenueEntity] re on re.[Revenue Entity ID]=o.[Revenue Entity ID]
            left join [dmsf].[vw_dimContract] c on (coalesce(c.[Contract ID],ox.[Contract ID],vw_q.[SBQQ__MasterContract__c]) = ox.[Renewed Contract ID] or vw_q.[SBQQ__MasterContract__c] = c.[Contract ID])
            LEFT JOIN [dbo].[vw_SF_billtoshipto_CUR] B
            ON vw_q.[QS_Bill_To__c] = B.[Id]
            AND B.[QS_Bill_To__c] = 1
            LEFT JOIN [dbo].[vw_SF_billtoshipto_CUR] S
            ON vw_q.[QS_Ship_To__c] = S.[Id]
            AND S.[QS_Ship_To__c] = 1
            where vw_q.[ID] in ({})""".format(str(pd.unique(self.data['Quote_id'])).replace("[","").replace("]","").replace(" ",","))


            sql_data = pd.read_sql(sql, self.cnxn)
            print('Fetched Quote SQL Query Successfully !!')
            sql_data.to_csv('sql_Quote.csv')
            self.msg.config(text='Fetched & Downloaded Quote SQL Query Successfully !!')
            print('Reading Quote-Lines SQL Query !!')
            self.msg.config(text='Reading Quote-Lines SQL Query !!')



            sql1 = """WITH [Quote Line Detail] AS (
            SELECT
                 Q.[Id] AS [Quote ID]
                ,QL.[SBQQ__Product__c] AS [Product ID]
                ,CASE 
                    WHEN LOWER(QL.[SBQQ__ProductName__c]) LIKE '%implementation%' 
                    THEN 'implementationservices'
            
                    WHEN QL.[SBQQ__Product__c] = '01t1L00000LJxGCQA1' 
                    THEN 'trainingessentials' 
            
                    ELSE LOWER(QL.[SBQQ__ProductName__c])
                 END AS [Product Name]  
                ,CASE 
                    WHEN LOWER(QL.[SBQQ__ProductName__c]) LIKE 'implementation%' 
                    THEN 0 
            
                    ELSE ISNULL(QL.[Effective_Entitlement_Qty__c], 0) 
                 END AS [Quantity]
            
            
            FROM 
                [dbo].[vw_SF_Quoteline_CUR] QL
                LEFT JOIN [dbo].[vw_SF_Quote_CUR] Q
                    ON QL.[SBQQ__Quote__c] = Q.[Id]
            
            WHERE
                ISNULL(QL.[SBQQ__SegmentIndex__c], 0) < 2
            
                AND Q.[Id] in ({})""".format(str(pd.unique(self.data['Quote_id'])).replace("[", "").replace("]", "").replace(" ", ",")) + """
                and ISNULL(QL.[Effective_Entitlement_Qty__c], 0) !=0
            )
            
            SELECT
             [Quote ID] as Quote_id
            ,[Product ID] as product_id
            ,[Product Name] as Name
            ,sum([Quantity]) as Quantity
            
            FROM 
            [Quote Line Detail]
            
            
            group by 
            [Quote ID]
            ,[Product Name]
            ,[Product ID] 
            
            ORDER BY
            [Quote ID]
            ,[Product Name]
            """
            sql_ql_data = pd.read_sql(sql1, self.cnxn)
            print('Fetched Quote-Lines SQL Query Successfully !!')
            sql_ql_data.to_csv('sql_quote_lines.csv')
            self.msg.config(text='Fetched & Downloaded SQL Queries Successfully !!')
