import tkinter as tk
from tkinter import ttk
from tkinter import *
from tkinter import messagebox

def add_label_frame(parent, title, frame_width, frame_height):
    frame = ttk.Labelframe(parent, text=title, width=frame_width, height=frame_height)
    frame.grid(column=0, row=0, sticky=(N, W, E, S))
    frame.columnconfigure(0, weight=1)
    frame.rowconfigure(0, weight=1)
    return frame

def add_data_entry(panel, entry_row, entry_var, entry_text, entry_len, hidden=False, readonly=False):
    tk.Label(panel, text=entry_text).grid(column=0, row=entry_row, sticky=W)

    if hidden == True:
        data_entry = tk.Entry(panel, width=entry_len, textvariable=entry_var, show="*")
    else:
        data_entry = tk.Entry(panel, width=entry_len, textvariable=entry_var)

    if readonly == True:
        data_entry.bind("<Key>", lambda e: "break")

    data_entry.grid(column=1, row=entry_row, sticky=W)

    return (entry_row + 1)

def add_command(panel, command_row, label_text, entry_var, cmd_text=None, cmd=None, readonly=False):
    tk.Label(panel, text=label_text).grid(column=0, row=command_row, sticky=W)

    # if entry_var is a widget already, we just need to grid it. otherwise, we create an input Entry to wrap it.
    if isinstance(entry_var, Widget):
        data_entry = entry_var
    else:
        data_entry = tk.Entry(panel, width=60, textvariable=entry_var)

    if readonly == True:
        data_entry.bind("<Key>", lambda e: "break")

    data_entry.grid(column=0, row=command_row + 1, sticky=W)

    if cmd:
        tk.Button(panel, text=cmd_text, command=cmd).grid(column=1, row=command_row+1, sticky=W)

    return (command_row + 2)

def add_separator(panel, row):
    ttk.Separator(panel).grid(row=row, columnspan=2, sticky="ew")

    return (row + 1)

def grid_panel(panel):
    for child in panel.winfo_children():
        child.grid_configure(padx=5, pady=5)

def popup_message(title, msg):
    messagebox.showinfo(title, msg)