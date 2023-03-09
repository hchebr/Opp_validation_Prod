from tkinter import messagebox,HORIZONTAL
from tkinter.ttk import Progressbar
import time


def bar(root):
    progress = Progressbar(root, orient=HORIZONTAL, length=100, mode='determinate')
    progress.pack(pady=10)

    progress['value'] = 20
    root.update_idletasks()
    time.sleep(1)

    progress['value'] = 40
    root.update_idletasks()
    time.sleep(1)

    progress['value'] = 50
    root.update_idletasks()
    time.sleep(1)

    progress['value'] = 60
    root.update_idletasks()
    time.sleep(1)

    progress['value'] = 80
    root.update_idletasks()
    time.sleep(1)
    progress['value'] = 100