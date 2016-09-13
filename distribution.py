import os
import json
import traceback
import multiprocessing
import subprocess
import dill

from tkinter_util import *
from db import DBConnectionPane


class Namespace:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class DistributionCommandPane(tk.Frame):
    def __init__(self, parent, dbPane):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane
        self.dbConn = None

        self.taxonKeys = StringVar()
        self.numberOfProcesses = IntVar()
        self.forceOverwrite = IntVar()
        self.verboseOutput = IntVar()
        self.numpyException = IntVar()
        self.recordLimit = IntVar()

        # Setting up UI widgets
        paramFrame = add_label_frame(parent, 'Species Distribution', 100, 100)

        entry_row = add_data_entry(paramFrame, 0, self.taxonKeys, "Taxon keys (comma-separated)", 80)

        self.numberOfProcesses.set(1)
        entry_row = add_data_entry(paramFrame, entry_row, self.numberOfProcesses, "Number of concurrent processes", 5)

        entry_row = add_check_box(paramFrame, entry_row, "Force overwrite toggle", self.forceOverwrite)

        self.verboseOutput.set(1)
        entry_row = add_check_box(paramFrame, entry_row, "Verbose output toggle", self.verboseOutput)

        entry_row = add_check_box(paramFrame, entry_row, "Numpy exception toggle", self.numpyException)

        entry_row = add_data_entry(paramFrame, entry_row, self.recordLimit, "Number of taxa to process (0 = All)", 5)

        exeBt = tk.Button(paramFrame, text=" Start Species Distribution ", fg="blue", command=self.start_species_distribution)
        exeBt.place(relx=0.5, rely=0.5, anchor=CENTER)

        grid_panel(paramFrame)

        parent.add(paramFrame)

    def prepare_options(self):
        opts = Namespace()

        taxa = self.taxonKeys.get().strip(" ,\r\n\t")
        if taxa == "":
           opts.__dict__.update(taxon= None)
        else:
            opts.__dict__.update(taxon= [int(x.strip(" {}\r\n\t")) for x in taxa.split(",") if x.strip(" {}\r\n\t") != ""])

        if self.numberOfProcesses.get() > 0:
            opts.__dict__.update(processes= self.numberOfProcesses.get())
        else:
            opts.__dict__.update(processes= 1)

        opts.__dict__.update(force= bool(self.forceOverwrite.get()))

        opts.__dict__.update(verbose= bool(self.verboseOutput.get()))

        opts.__dict__.update(numpy_exception= bool(self.numpyException.get()))

        records = self.recordLimit.get()
        if records and records > 0:
            opts.__dict__.update(limit= records)
        else:
            opts.__dict__.update(limit= None)

        return opts

    def prepare_settings(self):
        dbOpts = self.dbPane.getDbOptions()

        settings = {
            "DB": {
                "host": dbOpts['server'],
                "port": dbOpts['port'],
                "db": dbOpts['dbname'],
                "username": dbOpts['username'],
                "password": dbOpts['password']
            },
            "NUMPY_WARNINGS": "warn",
            "PNG_DIR": "png",
            "DEBUG": False
        }

        user_settings_file = os.path.join(os.getcwd(), 'species_distribution', '.settings.json')

        with open(user_settings_file, 'w') as f:
            json.dump(settings, f, indent=4)

    def start_species_distribution(self):
        working_dir = os.path.join(os.getcwd(), 'species_distribution')

        self.prepare_settings()

        optionFileName = os.path.join(working_dir, 'sd_options_%s.dill' % os.getpid())

        with open(optionFileName, 'wb', 0) as f:
            dill.dump(self.prepare_options(), f)

        try:
            subprocess.call(["python", os.path.join(working_dir, 'species-distribution'), "-o", optionFileName], cwd= working_dir)
        finally:
            os.remove(optionFileName)


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)

        mainPane = ttk.Panedwindow(master, orient=VERTICAL)
        dbPane = DBConnectionPane(parent=mainPane, title="DB Connection", include_threads=False, include_sqlfile=False)

        dbPane.db_name.set('sau_int')
        dbPane.db_username.set('sau_int')

        DistributionCommandPane(mainPane, dbPane)
        mainPane.pack(expand=1, fill='both')


# ===============================================================================================
# ----- MAIN
def main():
    root = tk.Tk()
    root.title("Species Distribution")
    app = Application(master=root)
    app.mainloop()


if __name__ == "__main__":
    try:
        multiprocessing.freeze_support()
        main()
    except SystemExit as x:
        sys.exit(x)
    except Exception:
        strace = traceback.extract_tb(sys.exc_info()[2])[-1:]
        lno = strace[0][1]
        print('Unexpected Exception on line: {0}'.format(lno))
        print(sys.exc_info())
        sys.exit(1)