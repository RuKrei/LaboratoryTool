#!/usr/bin/python
# Author: Rudi Kreidenhuber, <Rudi.Kreidenhuber@gmail.com>, 
# License: MIT

import pandas as pd
import os

cwd = os.getcwd()

# configuration
INPUT_FOLDER = os.path.join(cwd, "..", "input")
OUTPUT_FOLDER = os.path.join(cwd, "..", "output")
VALUES_OF_INTEREST = [  "CRP",
                        "Glucose",
                        "HbA1c (IFCC)"
                        "Creatinin",
                        "Cholesterin",
                        "HDL-Cholesterin",
                        "LDL-Cholesterin",
                        "Leukocyten",
                        "Neutrophile",
                        "Lymphocyten",
                        "Erythrocyten",
                        "HKT",
                        "Thrombocyten",
                        "Fibrinogen",
                        "PTZ-INR",
                        "Gesamteiweiß"]


print(f"\n\nFound the following input files:\n{os.listdir(INPUT_FOLDER)}")

class CsvTransformer:
    def __init__(self, INPUT_FOLDER, file = None, df = None, VALUES_OF_INTEREST = None):
        self.INPUT_FOLDER = INPUT_FOLDER
        self.file = file
        self.df = df
        self.VALUES_OF_INTEREST = VALUES_OF_INTEREST
    
    def get_filename(self, file):
        base = os.path.basename(file)
        return os.path.splitext(base)[0]
    
    def read_csv(self, file):
        #str_col = "NUMWERT"
        return pd.read_csv(file, delimiter=";", error_bad_lines = False, 
                            encoding="latin-1", dtype={"NUMWERT": "string"})
    
    def drop_unwanted_cols(self, df):
        garbage = ["GRUPPE", "STATUS", "UHRZEIT", "TEXTWERT", "EINHEIT", "NORMALWERTE"]
        df = df.drop(garbage, axis=1)
        return df
    
    def transform_csv_dates(self, df):
        date = df["DATUM"]
        date = pd.to_datetime(date, dayfirst=True)
        df["DATUM"] = date
        return df
    
    def drop_non_targets(self, df, VALUES_OF_INTEREST):
        df = df.sort_values(by=["PARAMETER", "DATUM"])
        #df = df.drop_duplicates(subset=["PARAMETER"], keep="first")
        #df = df.where(df["PARAMETER"].item() in VALUES_OF_INTEREST)
        df = df[df["PARAMETER"].isin(VALUES_OF_INTEREST)]
        return df
    
    def drop_all_but_oldest(self, df):
        df = df.sort_values(by=["PARAMETER", "DATUM"], na_position="first", ascending=False)
        df = df.drop_duplicates(subset="PARAMETER", keep="last")
        return df

    # not implemented
    def drop_all_but_newest(self, df):
        df = df.sort_values(["PARAMETER", "DATUM"]na_position="last", ascending=True)
        df = df.drop_duplicates(subset="PARAMETER", keep="first")
        return df


if __name__ == "__main__":
    ct = CsvTransformer(INPUT_FOLDER)
    files = os.listdir(INPUT_FOLDER)
    for f in files:
        # add path
        file = os.path.join(INPUT_FOLDER, f)
        # process file
        df = ct.read_csv(file)
        df = ct.drop_unwanted_cols(df)
        df = ct.transform_csv_dates(df)
        df = ct.drop_non_targets(df, VALUES_OF_INTEREST)
        df = ct.drop_all_but_oldest(df)
        df = df.T
        outname = f.split(".")[0] + "_oldest.tsv"
        outname = os.path.join(OUTPUT_FOLDER, outname)
        df.to_csv(outname, sep="\t", index=False)
