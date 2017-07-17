import luigi
import requests

import numpy as np
import pandas as pd

from sklearn.preprocessing import robust_scale

MIN_DATA_URL = "http://www.bom.gov.au/climate/change/acorn/sat/data/acorn.sat.minT.087031.daily.txt"
MAX_DATA_URL = "http://www.bom.gov.au/climate/change/acorn/sat/data/acorn.sat.maxT.087031.daily.txt"

class DownloadMinTemperatures(luigi.Task):
    def requires(self):
        return []
 
    def output(self):
        return luigi.LocalTarget("min_temps.csv")
 
    def run(self):
        min_string_data = requests.get(MIN_DATA_URL).text
        min_list_data = min_string_data.split("\n")

        with self.output().open('w') as out_file:
            for min_temp in min_list_data:
                out_line = min_temp
                out_file.write((out_line + '\n'))

class ImportMinTemperatures(luigi.Task):
    def requires(self):
        return [DownloadMinTemperatures()]
 
    def output(self):
        return luigi.LocalTarget("min_temps.df")
 
    def run(self):
        min_df = pd.read_csv(self.input()[0].open(),
                             delim_whitespace=True,       # Let Pandas know that white space is our delimiter
                             names=["date", "min_temp"],  # Strings for our column headings
                             skiprows=1,                  # Skip the header row
                             parse_dates=[0])             # Parse the first column as a date type

        with self.output().open('w') as min_file:
            min_df.to_csv(min_file,
                          sep='\t',
                          encoding='utf-8',
                          index=None)

class RemoveMinTempNaNs(luigi.Task):
    def requires(self):
        return [ImportMinTemperatures()]
 
    def output(self):
        return luigi.LocalTarget("clean_min_temps.df")
 
    def run(self):
        min_df = pd.read_csv(self.input()[0].open(),
                             delim_whitespace=True,       # Let Pandas know that white space is our delimiter
                             names=["date", "min_temp"],  # Strings for our column headings
                             skiprows=1,                  # Skip the header row
                             parse_dates=[0])             # Parse the first column as a date type

        # Replace the missing values with Numpy NaNs
        min_nan_df = min_df.replace(to_replace=99999.9,
                                    value=np.nan)
        min_interpolated_df = min_nan_df.interpolate()

        with self.output().open('w') as min_file:
            min_interpolated_df.to_csv(min_file,
                                       sep='\t',
                                       encoding='utf-8',
                                       index=None)

class StandardiseMinTemps(luigi.Task):
    def requires(self):
        return [RemoveMinTempNaNs()]
 
    def output(self):
        return luigi.LocalTarget("normalised_min_temps.df")
 
    def run(self):
        min_df = pd.read_csv(self.input()[0].open(),
                             delim_whitespace=True,       # Let Pandas know that white space is our delimiter
                             names=["date", "min_temp"],  # Strings for our column headings
                             skiprows=1,                  # Skip the header row
                             parse_dates=[0])             # Parse the first column as a date type

        min_df["min_temp"] = robust_scale(min_df["min_temp"])

        with self.output().open('w') as min_file:
            min_df.to_csv(min_file,
                          sep='\t',
                          encoding='utf-8',
                          index=None)

class DownloadMaxTemperatures(luigi.Task):
    def requires(self):
        return []
 
    def output(self):
        return luigi.LocalTarget("max_temps.csv")
 
    def run(self):
        max_string_data = requests.get(MAX_DATA_URL).text
        max_list_data = max_string_data.split("\n")

        with self.output().open('w') as out_file:
            for max_temp in max_list_data:
                out_line = max_temp
                out_file.write((out_line + '\n'))

class ImportMaxTemperatures(luigi.Task):
    def requires(self):
        return [DownloadMaxTemperatures()]
 
    def output(self):
        return luigi.LocalTarget("max_temps.df")
 
    def run(self):
        max_df = pd.read_csv(self.input()[0].open(),
                             delim_whitespace=True,       # Let Pandas know that white space is our delimiter
                             names=["date", "max_temp"],  # Strings for our column headings
                             skiprows=1,                  # Skip the header row
                             parse_dates=[0])             # Parse the first column as a date type

        with self.output().open('w') as max_file:
            max_df.to_csv(max_file,
                          sep='\t',
                          encoding='utf-8',
                          index=None)

class RemoveMaxTempNaNs(luigi.Task):
    def requires(self):
        return [ImportMaxTemperatures()]
 
    def output(self):
        return luigi.LocalTarget("clean_max_temps.df")
 
    def run(self):
        max_df = pd.read_csv(self.input()[0].open(),
                             delim_whitespace=True,       # Let Pandas know that white space is our delimiter
                             names=["date", "max_temp"],  # Strings for our column headings
                             skiprows=1,                  # Skip the header row
                             parse_dates=[0])             # Parse the first column as a date type

        # Replace the missing values with Numpy NaNs
        max_nan_df = max_df.replace(to_replace=99999.9,
                                    value=np.nan)
        max_interpolated_df = max_nan_df.interpolate()

        with self.output().open('w') as max_file:
            max_interpolated_df.to_csv(max_file,
                                       sep='\t',
                                       encoding='utf-8',
                                       index=None)

class StandardiseMaxTemps(luigi.Task):
    def requires(self):
        return [RemoveMaxTempNaNs()]
 
    def output(self):
        return luigi.LocalTarget("normalised_max_temps.df")
 
    def run(self):
        max_df = pd.read_csv(self.input()[0].open(),
                             delim_whitespace=True,       # Let Pandas know that white space is our delimiter
                             names=["date", "max_temp"],  # Strings for our column headings
                             skiprows=1,                  # Skip the header row
                             parse_dates=[0])             # Parse the first column as a date type

        max_df["max_temp"] = robust_scale(max_df["max_temp"])

        with self.output().open('w') as max_file:
            max_df.to_csv(max_file,
                          sep='\t',
                          encoding='utf-8',
                          index=None)

class CombineTemps(luigi.Task):
    def requires(self):
        return [StandardiseMinTemps(), StandardiseMaxTemps()]
 
    def output(self):
        return luigi.LocalTarget("comnined_temps.df")
 
    def run(self):
        min_df = pd.read_csv(self.input()[0].open(),
                             delim_whitespace=True,       # Let Pandas know that white space is our delimiter
                             names=["date", "max_temp"],  # Strings for our column headings
                             skiprows=1,                  # Skip the header row
                             parse_dates=[0])             # Parse the first column as a date type
        max_df = pd.read_csv(self.input()[1].open(),
                             delim_whitespace=True,       # Let Pandas know that white space is our delimiter
                             names=["date", "max_temp"],  # Strings for our column headings
                             skiprows=1,                  # Skip the header row
                             parse_dates=[0])             # Parse the first column as a date type

        combined_temp_df = min_df.merge(max_df,
                                        on="date",
                                        how="inner",
                                       )
        combined_temp_df.set_index("date",
                           inplace=True,
                          )

        with self.output().open('w') as combined_file:
            combined_temp_df.to_csv(combined_file,
                                    sep='\t',
                                    encoding='utf-8',
                                    index=None)

if __name__ == '__main__':
    luigi.run()
