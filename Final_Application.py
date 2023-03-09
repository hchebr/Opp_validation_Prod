from tkinter import *
from PIL import ImageTk,Image
import PIL.Image
from tkinter import filedialog, ttk
from Quote_lines import *
from Quote import *
from concurrent.futures import ThreadPoolExecutor
from Sql_queries import *
import threading
import sys,time
from Quote_Compare import *
from Sql_Quote_Compare import *
from Sql_Quoteline_compare import *
from final_data import *
from QL_compare import *
import logging
from os.path import exists

# log_file=logging.basicConfig(filename='error.log', encoding='utf-8',level=10,format="%(asctime)s %(message)s")


def open_file():
    file = filedialog.askdirectory()
    if file:
        path=file
        folder_select.config(text=" ", fg='blue')
        folder_select.configure(text="Folder Selected: " + file)
        path_text.delete(0, "end")
        path_text.insert(0, path)
        os.chdir(path)
        if os.path.exists("log_file_{}.txt".format(os.getlogin().replace(".", " ").title())):
            log = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            log.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S")+" Logging Started \n")
            log.close()
        else:
            log = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "x")
            log.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " Logging Started \n")
            log.close()
    else:
        folder_select.configure(text="Folder Not Selected ",foreground='#FF0000')




def open_file_name():
    file_name = filedialog.askopenfilename()
    if file_name:
        folder_select.config(text=" ", fg='blue')
        folder_select.configure(text="File Selected: " + file_name)
        path_file_text.delete(0, "end")
        path_file_text.insert(0, file_name)
        path = path_text.get()
        os.chdir(path)
        file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
        file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " SF_File: {}  \n".format(file_name))
        file1.close()
    else:
        folder_select.configure(text="SF Report File Not Selected ",foreground='#FF0000')
        # logging.info("SF Report File Not Selected ")
        path = path_text.get()
        os.chdir(path)
        file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
        file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " SF Report File Not Selected  \n")
        file1.close()

def Q():
    with ThreadPoolExecutor() as executor:
        path=path_text.get()
        folder_select.config(text="Folder Selected: " + path)
        Q_obj = Quote(path,bottomframe,msg,msg_remaining,progress_msg)
        # threading.Thread(target=).start()
        task=executor.submit(Q_obj.Quote_data())


def QL():
    with ThreadPoolExecutor() as executor:
        path = path_text.get()
        msg.config(text=" ", fg='blue')
        QL_obj = Quote_lines(path, bottomframe, msg, msg_remaining,progress_msg,start)
        html = executor.submit(QL_obj.pdf_to_html())
        df = QL_obj.html_to_table()
        rsf = QL_obj.rsf_otf(df)
        SOF = QL_obj.SOF_func(df)
        QL_obj.TSF_func(df)
        QL_obj.clean_folder()
        try:
            data=pd.read_csv(path+'\Quote_data_pdf.csv')
            data=pd.merge(data,SOF[['OTF','Billing','Net','Quote_id']],on='Quote_id',how='left')
            data.to_csv(path+'\Quote_data_pdf.csv')
            # invalid_q=pd.read_csv(path+'\invalid_pdfs_quote.csv')
            # invalid_ql = pd.read_csv(path + '\invalid_pdfs_ql.csv')
            # invalid = pd.concat([invalid_q, invalid_ql]).reset_index(drop=True)
            # invalid = invalid.drop(invalid.columns[0], axis=1)
            # invalid.columns = ['Quote_id']
            # invalid['Quote_id'] = invalid['Quote_id'].apply(lambda x: x.split("_")[0]).drop_duplicates()
            # invalid = invalid.dropna()
            # invalid.to_csv(path + '\invalid_quotes_final.csv')

        except Exception as e:

            msg.config(text="Successfully fetched Quote & Quote-lines data. But Error with Merging data & SOF. Error: {} ".format(e),fg='Red', bg='white')
            path = path_text.get()
            os.chdir(path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " Successfully fetched Quote & Quote-lines data. But Error with Merging data & SOF. Error: {}  \n".format(e))
            file1.close()
            # logging.exception("Successfully fetched Quote & Quote-lines data. But Error with Merging data & SOF. Error: {} ".format(e))
            raise



def pdf_fetch():
    try:
        with ThreadPoolExecutor() as executor:
            folder_select.config(text=" ", fg='blue')
            executor.submit(Q())
            # time.sleep(5)
            # msg.config(text='Sleeping for 5 Seconds, Please wait!!')

            executor.submit(QL())
    except PermissionError as e:

        folder_select.config(text="Please Close file {}".format(e), fg='red')
        path = path_text.get()
        os.chdir(path)
        path = path_text.get()
        os.chdir(path)
        file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
        file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "Please Close file {}".format(e) + " \n")
        file1.close()

        raise
    except OSError as e:

        folder_select.config(text="Please Select Folder",fg='red')

        raise
    except Exception as e:

        folder_select.config(text=e, fg='red')
        path = path_text.get()
        os.chdir(path)
        file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
        file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + str(e)+" \n")
        file1.close()
        raise


def compare_SF():
    with ThreadPoolExecutor() as executor:
        try:
            method = clicked.get()
            folder_select.config(text="", fg='blue')
            msg.config(text="")
            msg_remaining.config(text="")

            path_file = path_file_text.get()
            # path = path_text.get()
            path=os.path.dirname(os.path.abspath(path_file))
            os.chdir(path)
            SF_data=pd.read_csv(path_file)
            pdf_data = pd.read_csv(r'Quote_data_pdf.csv',encoding='latin-1')
            if len(pdf_data)!=0:
                 compare_q= compare_quote_SF(path,bottomframe,msg,msg_remaining,SF_data,pdf_data)
                 start_compare = compare_q.start
                 task = executor.submit(compare_q)

            else:
                folder_select.config(text="Quote-pdf data is Empty. Hence Quote Comparision Not successful", fg='Red')
                path = path_text.get()
                os.chdir(path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "Quote-pdf data is Empty. Hence Quote Comparision Not successful" + " \n")
                file1.close()
            # threading.Thread(target=).start()

            SF_data = pd.read_csv(path_file)
            pdf_data_ql = pd.read_csv(r'Quote_lines_pdf.csv')

            if len(pdf_data_ql)!=0:
                compare_ql = compare_quote_line_SF(path, bottomframe, msg, msg_remaining, SF_data, pdf_data_ql)
                task = executor.submit(compare_ql)
            else:
                folder_select.config(text="Quote line-pdf data is Empty. Hence Quoteline Comparision Not successful", fg='Red')
                path = path_text.get()
                os.chdir(path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "Quote line-pdf data is Empty. Hence Quoteline Comparision Not successful" + " \n")
                file1.close()


            if (len(pdf_data)!=0) and (len(pdf_data_ql)!=0):

                compare_quote = pd.read_csv(r'Compare_Quote_SF.csv')
                compare_ql = pd.read_csv(r'Compare_Ql_SF.csv')

                final = final_sql(path, bottomframe, compare_quote, compare_ql, msg, msg_remaining, start_compare,
                                  method, start)
                task = executor.submit(final)

            elif (len(pdf_data)!=0) and (len(pdf_data_ql)==0):
                msg_remaining.config(text="Refer to Compare_Quote_SF as Quoteline-pdf data is empty!!", fg='red',bg="white")
                path = path_text.get()
                os.chdir(path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "Refer to Compare_Quote_SF as Quoteline-pdf data is empty!!" + " \n")
                file1.close()

            elif (len(pdf_data)==0) and (len(pdf_data_ql)!=0):
                msg_remaining.config(text="For Final Comparision data, Refer to Compare_QL_SF as Quote-pdf data is empty!!", fg='red',bg="white")
                path = path_text.get()
                os.chdir(path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "For Final Comparision data, Refer to Compare_QL_SF as Quote-pdf data is empty!!" + " \n")
                file1.close()
            else:
                msg_remaining.config(text="Comparision Failed!!, As Both Quote_line PDF and Quote PDF are Empty!!", fg='Red',bg="white")
                path = path_text.get()
                os.chdir(path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "Comparision Failed!!, As Both Quote_line PDF and Quote PDF are Empty!!" + " \n")
                file1.close()

        except FileNotFoundError as e:
            folder_select.config(text=" ", fg='blue')
            folder_select.config(text=e, fg='red')
            raise
        except OSError as e:
            if '[Errno 22]' in str(e):
                folder_select.config(text=" ", fg='blue')
                folder_select.config(text="Restart App", fg='red')
                raise
        except Exception as e:
            folder_select.config(text=" ", fg='blue')
            folder_select.config(text=e, fg='red')
            raise


def compare_sql():
    with ThreadPoolExecutor() as executor:
        try:
            method = clicked.get()

            folder_select.config(text=" ", fg='blue')
            msg.config(text=" ")
            msg_remaining.config(text=" ")
            path=path_text.get()
            os.chdir(path)

            Sql_data=pd.read_csv(r'sql_Quote.csv')
            pdf_data=pd.read_csv(r'Quote_data_pdf.csv')

            if len(pdf_data)!=0:
                 compare_q= compare_quote_sql(path,bottomframe,msg,msg_remaining,Sql_data,pdf_data)
                 start_compare = compare_q.start
                 task = executor.submit(compare_q)
                 compare_q = compare_quote_sql(path, bottomframe, msg, msg_remaining, Sql_data, pdf_data)
                 task = executor.submit(compare_q)
                 start_compare = compare_q.start

            else:
                folder_select.config(text="Quote-pdf data is Empty. Hence Quote Comparision Not successful", fg='Red')
                path = path_text.get()
                os.chdir(path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "Quote-pdf data is Empty. Hence Quote Comparision Not successful" + " \n")
                file1.close()

            Sql_ql_data = pd.read_csv(r'sql_quote_lines.csv')
            pdf_ql_data = pd.read_csv(r'Quote_lines_pdf.csv')

            if len(pdf_ql_data)!=0:
                compare_ql = compare_quoteline_sql(path, bottomframe, msg, msg_remaining, Sql_ql_data, pdf_ql_data)
                task = executor.submit(compare_ql)
            else:
                folder_select.config(text="Quote line-pdf data is Empty. Hence Quoteline Comparision Not successful", fg='Red')
                path = path_text.get()
                os.chdir(path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "Quote line-pdf data is Empty. Hence Quoteline Comparision Not successful" + " \n")
                file1.close()

            if (len(pdf_data) != 0) and (len(pdf_ql_data) != 0):

                compare_quote = pd.read_csv(r'Compare_Quote_sql.csv')
                compare_ql = pd.read_csv(r'Compare_Ql_Sql.csv')

                final = final_sql(path, bottomframe, compare_quote, compare_ql, msg, msg_remaining, start_compare,method, start)
                task = executor.submit(final)

            elif (len(pdf_data) != 0) and (len(pdf_ql_data) == 0):
                msg_remaining.config(text="For Final Comparision data, Refer to Compare_Quote_Sql as Quoteline-pdf data is empty!!", fg='red',bg="white")
                path = path_text.get()
                os.chdir(path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "For Final Comparision data, Refer to Compare_Quote_Sql as Quoteline-pdf data is empty!!" + " \n")
                file1.close()

            elif (len(pdf_data) == 0) and (len(pdf_ql_data) != 0):
                msg_remaining.config(text="For Final Comparision data, Refer to Compare_QL_Sql as Quote-pdf data is empty!!", fg='red',bg="white")
                path = path_text.get()
                os.chdir(path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "For Final Comparision data, Refer to Compare_QL_Sql as Quote-pdf data is empty!!" + " \n")
                file1.close()
            else:
                msg_remaining.config(text="Comparision Failed!!, As Both Quote_line PDF and Quote PDF are Empty!!",fg='Red',bg="white")
                path = path_text.get()
                os.chdir(path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "Comparision Failed!!, As Both Quote_line PDF and Quote PDF are Empty!!" + " \n")
                file1.close()


        except FileNotFoundError as e:
            folder_select.config(text=e, fg='red')
            path = path_text.get()
            os.chdir(path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            file1.write(datetime.now().strftime( "%d/%m/%Y %H:%M:%S") +e + " \n")
            file1.close()
            raise
        except OSError as e:
            if '[Errno 22]' in str(e):
                folder_select.config(text="Restart App", fg='red')
                path = path_text.get()
                os.chdir(path)
                file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
                file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "Restart App" + " \n")
                file1.close()
                raise
        except Exception as e:
            folder_select.config(text=e, fg='red')
            path = path_text.get()
            os.chdir(path)
            file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
            file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + str(e) + " \n")
            file1.close()
            raise

def show():

    method=clicked.get()
    if method=='Step 3: SALESFORCE Report':
        folder_select.config(text=" ",fg='blue')
        for widget in bottomframe.winfo_children():
            widget.destroy()
        folder_select.configure(text="Process Selected: " + method)
        method_text.delete(0, "end")
        method_text.insert(0, method)
        open_file_name()
        button = Button(bottomframe, text="Step 5: Compare",command=lambda: threading.Thread(target=compare_SF).start())
        button.pack(side='top', anchor='s', pady=(5, 5))
        button.config(font=('verdana', 12))
        method_text.delete(0, "end")


    elif method=='Step 3: SQL Queries(ON VPN)':
        with ThreadPoolExecutor() as executor:
            try:
                folder_select.config(text=" ",fg='blue')
                for widget in bottomframe.winfo_children():
                    widget.destroy()
                folder_select.configure(text="Process Selected: " + method)
                method_text.delete(0, "end")
                method_text.insert(0, method)
                button = Button(bottomframe, text="Step 5: Compare",command=lambda: threading.Thread(target=compare_sql).start())
                button.pack(side='top', anchor='s', pady=(5, 5))
                button.config(font=('verdana', 12))
                executor.submit(child_window(msg, msg_remaining))
                method_text.delete(0, "end")
            except OSError as e:
                folder_select.config(text="Please Select Onedrive folder", fg='Red')
            except Exception as e:
                folder_select.config(text=e, fg='Red')

    else:
        folder_select.configure(text="Process Not Selected ", foreground='#FF0000')
        path = path_text.get()
        os.chdir(path)
        file1 = open("log_file_{}.txt".format(os.getlogin().replace(".", " ").title()), "a")
        file1.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "Process Not Selected" + " \n")
        file1.close()




def resource_path(relative_path):
# """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)



def child_window(msg,msg_remaining):

    def query(username_entry, pwd_entry, child_root, data, msg, path):
        user = username_entry.get()
        pwd = pwd_entry.get()
        Q = Queries(user, pwd, child_root, data, msg, path)


    def my_show():
      if (c_v1.get() == 1):
         pwd_entry.config(show='')
      else:
         pwd_entry.config(show='*')

    path = path_text.get()
    os.chdir(path)
    msg.config(text="Logging in.....")
    msg_remaining.config(text="Please wait untill you see message Fetched & Downloaded SQL Queries Successfully !! above", fg='black',bg='yellow')
    child_root = Toplevel(root)
    child_root.title("SSMS LOGIN")
    child_root.geometry("500x500")
    icon_path = resource_path("icims-logo.ico")
    child_root.iconbitmap(icon_path)
    child_root.configure(background='white')

    Label(child_root, text='SSMS LOGIN', font=('Bell MT', 20, 'bold'), bg="white").pack(pady=(10, 10))

    Label(child_root, text='Username', font=('Arial', 16), bg="white").pack(pady=(10, 5), side=TOP)
    username_entry = ttk.Entry(child_root, width=40)
    username_entry.pack(ipady=4, pady=5)

    Label(child_root, text='Password', font=('Arial', 16), bg="white").pack(pady=(10, 5), side=TOP)
    pwd_entry = ttk.Entry(child_root, width=40,show='*')
    pwd_entry.pack(ipady=4, pady=5)

    c_v1 = IntVar(value=0)
    c1 = Checkbutton(child_root, text='Show Password', variable=c_v1, onvalue=1, offvalue=0, command=my_show,bg="white")
    c1.pack(pady=(10, 10), side="top")


    data=pd.read_csv(path+"\Quote_data_pdf.csv")

    if len(data) == 0:
        data=pd.read_csv(path + '\Quote_lines_pdf.csv')
    else:
        data=pd.read_csv(path+"\Quote_data_pdf.csv")


    button_child = Button(child_root, text="Login", bg="white", command=lambda: threading.Thread(target=query(username_entry, pwd_entry, child_root, data, msg, path)))
    button_child.pack(side='top', anchor='s', pady=(0, 10),ipadx=10)


root = Tk()
root.geometry('1000x800')
root.resizable(0,10)
root.title('Opp_validation App')
icon_path = resource_path("icims-logo.ico")
root.iconbitmap(icon_path)
root.configure(background='white')


frame = Frame(root,bg='#F5F5F5',width=800,height=800)
frame.pack(pady=(10,10),ipadx=800,ipady=800)

# Create A Canvas
my_canvas = Canvas(frame)

# Add A Scrollbar To The Canvas
my_scrollbar_y = ttk.Scrollbar(frame, orient=VERTICAL, command=my_canvas.yview)
my_scrollbar_y.pack(side=RIGHT, fill=Y)

my_scrollbar_x = ttk.Scrollbar(frame, orient=HORIZONTAL, command=my_canvas.xview)
my_scrollbar_x.pack(side=BOTTOM, fill=X)

# packing canvas last makes sure that the horizontal/vertical scroll bar applies to full screen
my_canvas.pack(side=TOP, fill=BOTH, expand=TRUE)

# Configure The Canvas
my_canvas.configure(xscrollcommand=my_scrollbar_x.set)
my_canvas.configure(yscrollcommand=my_scrollbar_y.set)


my_canvas.bind('<Configure>', lambda e: my_canvas.configure(scrollregion = my_canvas.bbox("all")))

# Create ANOTHER Frame INSIDE the Canvas
second_frame = Frame(my_canvas,width=1000,bg='#F5F5F5')

# Add that New frame To a Window In The Canvas
my_canvas.create_window((500,100), window=second_frame)


# bottomframe_1=Frame(second_frame,bg='#F5F5F5')
# bottomframe_1.pack(side=BOTTOM,pady=(20,20),padx=(20,20))
#
bottomframe_2=Frame(second_frame,bg='#F5F5F5')
bottomframe_2.pack(side=BOTTOM,pady=(5,10))

bottomframe_b = Frame(second_frame,bg='#F5F5F5')
bottomframe_b.pack(side=BOTTOM,pady=(5,10))

bottomframe_t = Frame(second_frame,bg='#F5F5F5')
bottomframe_t.pack(side=BOTTOM)

bottomframe=Frame(bottomframe_2,bg='#F5F5F5')
bottomframe.pack(side=BOTTOM,padx=(20,20))

bottomframe_bb=Frame(bottomframe,bg='#F5F5F5')
bottomframe_bb.pack(side=BOTTOM,padx=(20,20))

bottomframe_L=Frame(bottomframe_2,bg='#F5F5F5')
bottomframe_L.pack(side=LEFT,pady=(20,20),padx=(10,10))

bottomframe_R=Frame(bottomframe_2,bg='#F5F5F5')
bottomframe_R.pack(side=RIGHT,pady=(20,20),padx=(0,10))

bottomframe_RR=Frame(bottomframe_R,bg='#F5F5F5')
bottomframe_RR.pack(side=RIGHT,padx=(10,10))

image_path = resource_path("icims-logo.png")

img = PIL.Image.open(image_path)
resized_img = img.resize((200,90))
img = ImageTk.PhotoImage(resized_img)

img_label = Label(second_frame,image=img,bg='#F5F5F5')
img_label.pack(pady=(20,20),side=TOP)




text_label = Label(second_frame,text='Opp Validation',fg='black',bg='#F5F5F5')
text_label.pack(side=TOP,anchor='center')
text_label.config(font=('verdana',24))

Welcome_text = Label(bottomframe_t, text="Welcome " + os.getlogin().replace(".", " ").title()+"  !! ", fg='green',  bg='#F5F5F5')
Welcome_text.pack(pady=(40, 20), side=TOP)
Welcome_text.config(font=('Arial bold', 15))

folder_select = Label(bottomframe_b,text='Select Onedrive Folder ',fg='black',bg='#F5F5F5')
folder_select.pack(side='top',pady=(10,10))
folder_select.config(font=('verdana',12))

msg = Label(bottomframe_b, text="", fg='blue', bg='#F5F5F5')
msg.pack(side='top', pady=(10, 5))
msg.config(font=('verdana', 12))

msg_remaining = Label(bottomframe_b, text="", fg='blue', bg='#F5F5F5')
msg_remaining.pack(side='top', pady=(10, 5))
msg_remaining.config(font=('verdana', 12),wraplength=1000, justify="center")

progress_msg = Label(bottomframe_bb, text='', fg='Red', bg='#F5F5F5')
progress_msg.pack(side='bottom', pady=(10, 5))
progress_msg.config(font=('verdana', 12), wraplength=500, justify="center")

path_text=Entry(bottomframe_b)
path=path_text.get()


path_file_text=Entry(bottomframe_b)
path_file=path_file_text.get()

method_text=Entry(bottomframe_b)
method_text_file=method_text.get()

# Change the label text


# Dropdown menu options
options = [
    "Step 3: SALESFORCE Report",
    "Step 3: SQL Queries(ON VPN)"]

# datatype of menu text
clicked = StringVar()

# initial menu text
clicked.set("Step 3: Choose a Process to Proceed")

# Create Dropdown menu
drop = OptionMenu(bottomframe_R, clicked, *options)
drop.pack(side='top',anchor='s',padx=(0,0),pady=5)
drop.config(font=('verdana',12))


button = Button(bottomframe_RR, text="Step 4: Run", command=show,fg='blue')
button.pack(side='top',anchor='s',padx=(0,0),pady=5)
button.config(font=('verdana',12))

start=time.time()
end=time.time()

button=Button(bottomframe_b, text="Step 1: Choose Onedirve Folder ",command= open_file )
button.pack(side='top',anchor='s',padx=(0,0),pady=5)
button.config(font=('verdana',12))

button=Button(bottomframe_L, text="Step 2: Fetch Data from PDF", command= lambda: threading.Thread(target=pdf_fetch).start())
button.pack(side='top',anchor='s',padx=(0,0))
button.config(font=('verdana',12))



# button=Button(bottomframe_R, text='Step 3: Select SALESFORCE Report', command= lambda: threading.Thread(target=open_file_name).start())
# button.pack(side='top',anchor='s')
# button.config(font=('verdana',12))
#
# button=Button(bottomframe_RR, text='Step 3: Run SQL Queries(ON VPN)', command= lambda: threading.Thread(target=child_window(msg,msg_remaining)).start())
# button.pack(side='right',anchor='e')
# button.config(font=('verdana',12))

#


# sys.exit(30)

root.mainloop()


