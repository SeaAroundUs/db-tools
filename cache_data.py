import optparse
from functools import partial

import sqlprocessor as sp
from db import DBConnectionPane
from db import getDbConnection

from tkinter_util import *


class CacheDataCommandPane(tk.Frame):
    def __init__(self, parent, dbPane):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane

        self.processFrame = add_label_frame(parent, "Cache Data Generation/Refresh", 400, 100)

        add_buttons(self.processFrame,
                    data=[["RFMO csv cache", partial(self.process, 4), "blue", "Generate/Refresh RFMO csv cache"]],
                    row=0, column=0, direction="horizontal")

        parent.add(self.processFrame)

        for child in self.processFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

    def process(self, entity_layer_id):
        if not self.dbPane.isConnectionTestedSuccessfully():
            popup_message("Connection not yet tested",
                          "The DB Connection has not been tested successfully.\n" +
                          "Once the DB Connection has been tested successfully, you can click the Process button again.")
            return

        dbOpts = self.dbPane.getDbOptions()
        dbSession = getDbConnection(optparse.Values(dbOpts)).getSession()

        dbSession.execute("SELECT * FROM web_cache.maintain_catch_csv_partition(%s)" % entity_layer_id)

        dbOpts['sqlfile'] = "sql/populate_catch_data_in_csv.sql"
        dbOpts['sqlcmd'] = "select format('vacuum analyze web_cache.%s', table_name) from schema_v('web_cache') where table_name not like 'TOTAL%'"
        dbOpts['threads'] = 4
        sp.process(optparse.Values(dbOpts))


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)
        mainPane = ttk.Panedwindow(master, orient=VERTICAL)
        dbPane = DBConnectionPane(mainPane, "DB Connection", include_threads=True, include_sqlfile=False)
        CacheDataCommandPane(mainPane, dbPane)
        mainPane.pack(expand=1, fill='both')


# ===============================================================================================
# ----- MAIN
if __name__ == "__main__":
    tkinter_client_main(Application, "Cache Data Generator")