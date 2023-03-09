import tkinter
from tkinter import *
from tkinter import ttk
from PIL import ImageTk,Image
import PIL.Image
import sys,os
from Sql_queries import *
import threading
#Create an instance of tkinter frame or window
win= Tk()
#Set the geometry of tkinter frame
win.geometry("750x250")
win.title("Main Window")
#Define a function to Open a new window
def resource_path(relative_path):
# """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)



def child_window(msg, path):

   def my_show():
      if (c_v1.get() == 1):
         pwd_entry.config(show='')
      else:
         pwd_entry.config(show='*')

   child_root = Toplevel(win)
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
   pwd_entry = ttk.Entry(child_root, width=40)
   pwd_entry.pack(ipady=4, pady=5)

   c_v1 = IntVar(value=0)
   c1 = Checkbutton(child_root, text='Show Password', variable=c_v1, onvalue=1, offvalue=0, command=my_show,bg="white")
   c1.pack(pady=(10, 10), side="top")

   button_child = Button(child_root, text="Login", bg="white", command=lambda: threading.Thread(Queries(username_entry, pwd_entry, child_root, data, msg, path)))
   button_child.pack(side='top', anchor='s', pady=(0, 10),ipadx=10)

#

# content= entry.get()
# Label(child_root, text=content, font=('Bell MT', 20, 'bold')).pack()

# win.withdraw()
#Create an Entry Widget
entry=ttk.Entry(win, width= 40)
entry.pack(ipady=4,pady=20)

path=r'C:\Users\hanish.chebrole\OneDrive for Business\10_26_22'
data=pd.read_csv(r'C:\Users\hanish.chebrole\OneDrive for Business\10_26_22\Quote_data_pdf.csv')

msg = Label(win, text="", fg='blue', bg='#F5F5F5')
msg.pack(side='top', pady=(10, 5))
msg.config(font=('verdana', 12))

#Let us create a button in the Main window
button= ttk.Button(win, text="OK",command=lambda: threading.Thread((child_window(msg,path))))
button.pack(pady=20)
win.mainloop()