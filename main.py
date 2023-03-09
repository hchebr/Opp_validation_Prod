import time
from tkinter import *
from PIL import ImageTk,Image
import PIL.Image
from tkinter import messagebox, filedialog, ttk
import os
from tkinter.ttk import Progressbar
from Quote_lines import *
from Quote import *
from concurrent.futures import ThreadPoolExecutor
from Sql_queries import *
import threading
import sys
from Quote_Compare import *
import logging
from Sql_Quote_Compare import *
from Sql_Quoteline_compare import *
from final_data import *


def open_file():
    file = filedialog.askdirectory()
    if file:
        path=file
        folder_select.config(text=" ", fg='blue')
        folder_select.configure(text="Folder Selected: " + file)
        path_text.delete(0, "end")
        path_text.insert(0, path)
    else:
        folder_select.configure(text="Folder Not Selected ",foreground='#FF0000')


def open_file_name():
    file_name = filedialog.askopenfilename()
    if file_name:
        folder_select.config(text=" ", fg='blue')
        folder_select.configure(text="File Selected: " + file_name)
        path_file_text.delete(0, "end")
        path_file_text.insert(0, file_name)
    else:
        folder_select.configure(text="SF Report File Not Selected ",foreground='#FF0000')


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
        QL_obj.clean_folder()
        try:
            data=pd.read_csv(path+'\Quote_data_pdf.csv')
            data=pd.merge(data,SOF[['OTF','Billing','Net','Quote_id']],on='Quote_id',how='left')
            data.to_csv(path+'\Quote_data_pdf.csv')
        except Exception as e:
            msg.config(text="Successfully fetched Quote & Quote-lines data. But Error with Merging data & SOF. Error: {} ".format(e),fg='Red', bg='white')
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
        raise
    except OSError as e:
        folder_select.config(text="Please Select Folder",fg='red')
        raise
    except Exception as e:
        folder_select.config(text=e, fg='red')
        raise


def compare():
    with ThreadPoolExecutor() as executor:
        try:
            folder_select.config(text=" ", fg='blue')
            path=path_text.get()
            os.chdir(path)
            path_file = path_file_text.get()
            SF_data=pd.read_csv(path_file)
            pdf_data=pd.read_csv(r'Quote_data_pdf.csv')
            compare_q= compare_quote_SF(path,bottomframe,msg,progress_msg,SF_data,pdf_data)
            # threading.Thread(target=).start()
            task=executor.submit(compare_q)
        except FileNotFoundError as e:
            folder_select.config(text=" ", fg='blue')
            folder_select.config(text="Please Select SF Report", fg='red')
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

    msg_remaining.config(text="")
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

#
# leftframe_t=Frame(bottomframe_t,bg='#F5F5F5')
# leftframe_t.pack(side=LEFT,padx=(20,5),anchor='w')
#
# rightframe_t=Frame(bottomframe_t,bg='#F5F5F5')
# rightframe_t.pack(side=RIGHT,padx=(5,20),anchor='w')
#
# leftframe_b=Frame(bottomframe_b,bg='#F5F5F5')
# leftframe_b.pack(side=LEFT,padx=(20,35),anchor='w')
#
# rightframe_b=Frame(bottomframe_b,bg='#F5F5F5')
# rightframe_b.pack(side=RIGHT,padx=(35,20),anchor='w')

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

# stop_image_path = resource_path("stop.jpg")
#
# img_stop = PIL.Image.open(stop_image_path)
# resized_img_stop = img_stop.resize((40,40))
# img_stop = ImageTk.PhotoImage(resized_img_stop)

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
msg_remaining.config(font=('verdana', 12),wraplength=500, justify="center")

progress_msg = Label(bottomframe, text='', fg='Red', bg='#F5F5F5')
progress_msg.pack(side='top', pady=(10, 5))
progress_msg.config(font=('verdana', 12), wraplength=500, justify="center")

path_text=Entry(bottomframe_b)
path=path_text.get()


path_file_text=Entry(bottomframe_b)
path_file=path_file_text.get()


start=time.time()
end=time.time()
# step_label = Label(bottomframe_b,text='Step 1:',fg='black',bg='Yellow')
# step_label.pack(side=LEFT,anchor='center')
# step_label.config(font=('verdana',12))


button=Button(bottomframe_b, text="Step 1: Choose Onedirve Folder ",command= open_file )
button.pack(side='top',anchor='s',padx=(0,0),pady=5)
button.config(font=('verdana',12))

button=Button(bottomframe_L, text="Step 2: Fetch Order Form data", command= lambda: threading.Thread(target=pdf_fetch).start())
button.pack(side='top',anchor='s',padx=(0,0))
button.config(font=('verdana',12))

button=Button(bottomframe_R, text='Step 3: Select SALESFORCE Report', command= lambda: threading.Thread(target=open_file_name).start())
button.pack(side='top',anchor='s')
button.config(font=('verdana',12))

button=Button(bottomframe_RR, text='Step 3: Run SQL Queries(ON VPN)', command= lambda: threading.Thread(target=child_window(msg,msg_remaining)).start())
button.pack(side='right',anchor='e')
button.config(font=('verdana',12))

#
button=Button(bottomframe, text="Step 4: Compare", command= lambda: threading.Thread(target=compare).start())
button.pack(side='top',anchor='s',pady=(5,5))
button.config(font=('verdana',12))

# sys.exit(30)

root.mainloop()


