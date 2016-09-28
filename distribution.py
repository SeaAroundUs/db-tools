import os
import json
import subprocess
import dill

from tkinter_util import *


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

        grid_panel(paramFrame)

        parent.add(paramFrame)

        processFrame = add_label_frame(parent, "Process", 100, 100)

        exeBt = tk.Button(processFrame, text=" Start Species Distribution ", fg="blue", command=self.start_species_distribution)
        exeBt.place(relx=0.5, rely=0.5, anchor=CENTER)

        grid_panel(processFrame)

        parent.add(processFrame)

    def prepare_options(self):
        options = dict()

        taxa = self.taxonKeys.get().strip(" ,\r\n\t")
        if taxa == "":
            options['taxon'] = None
        else:
            options['taxon'] = [int(x.strip(" {}\r\n\t")) for x in taxa.split(",") if x.strip(" {}\r\n\t") != ""]

        if self.numberOfProcesses.get() > 0:
            options['processes'] = self.numberOfProcesses.get()
        else:
            options['processes'] = 1

        options['force'] = bool(self.forceOverwrite.get())

        options['verbose'] = bool(self.verboseOutput.get())

        options['numpy_exception'] = bool(self.numpyException.get())

        records = self.recordLimit.get()
        if records and records > 0:
            options['limit'] = records
        else:
            options['limit'] = None

        return options

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
        self.prepare_settings()

        working_dir = os.path.join(os.getcwd(), 'species_distribution')

        optionFileName = os.path.join(working_dir, 'sd_options_%s.dill' % os.getpid())

        with open(optionFileName, 'wb', 0) as f:
            dill.dump(self.prepare_options(), f)

        try:
            subprocess.call(["python", os.path.join(working_dir, 'species-distribution'), "-o", optionFileName], cwd= working_dir)
        finally:
            os.remove(optionFileName)


# ===============================================================================================
# ----- MAIN
if __name__ == "__main__":
    app = Application("Species Distribution", DistributionCommandPane)
    app.mainDbPane.db_name.set('sau_int')
    app.mainDbPane.db_username.set('sau_int')
    app.run()