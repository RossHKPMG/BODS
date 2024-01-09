### PACKAGES ###

import os, json, locale, requests, shutil, xmltodict, urllib3

import pandas as pd 
from urllib.request import urlopen
from zipfile import ZipFile, is_zipfile
from io import BytesIO
from xml.etree import ElementTree as ET

locale.setlocale(locale.LC_ALL, 'gb_GB')


class FaresCleaner:

    error_list = []

    ### Defining some initial values, also the main informatino that is to be passed to the API request for grabbing the given operators fare data ###

    def __init__(self, xml_folder, json_folder, api_key,  nocs=None, status='published', limit=10_000, offset=0):
        
        self.api_key = api_key
        self.nocs = nocs
        self.status = status
        self.limit = limit
        self.offset = offset
        self.xml_folder = xml_folder
        self.json_folder = json_folder

        self.final_df = pd.DataFrame()


    ### Grabbing XML Files --> JSON Folder Creation --> JSON Conversion --> Single Ticket Extraction ### 

    def grab_fare_data(self):

        for i in range(0, len(self.nocs)):

            request_string = "https://data.bus-data.dft.gov.uk/api/v1/fares/dataset?" + "noc=" + self.nocs[i] + "&status=" + self.status + "&limit=" + str(self.limit) + "&offset=" + str(self.offset) + "&api_key=" + str(self.api_key)
            
            data = requests.get(request_string)
            response_data = data.json()
            num_results = len(response_data['results'])

            if num_results > 1:

                for key in range(0, num_results):
                    self.operator_name = response_data["results"][key]["operatorName"]
                    url = response_data["results"][key]["url"]
                    parent_dir = self.xml_folder
                    path = os.path.join(parent_dir, self.operator_name)
                    if os.path.exists(path):
                        with urlopen(url) as in_stream:
                            if is_zipfile(in_stream):
                                with ZipFile(BytesIO(in_stream.read())) as zfile:
                                    zfile.extractall(path)
                            else:
                                filename = in_stream.headers.get_filename()
                                xml = open(path + "/" + filename, "w")
                                xml.write(in_stream.read().decode('utf-8'))
                                xml.close()
                                

                    else:
                        os.mkdir(path)
                        with urlopen(url) as in_stream:
                            if is_zipfile(in_stream):
                                with ZipFile(BytesIO(in_stream.read())) as zfile:
                                    zfile.extractall(path)
                            else:
                                filename = in_stream.headers.get_filename()
                                xml = open(path + "/" + filename, "w")
                                xml.write(in_stream.read().decode('utf-8'))
                                xml.close()
                                

            elif num_results == 1:

                self.operator_name = response_data["results"][0]["operatorName"]
                url = response_data["results"][0]["url"]
                parent_dir = self.xml_folder
                path = os.path.join(parent_dir, self.operator_name)
                if os.path.exists(path):
                    with urlopen(url) as in_stream:
                        if is_zipfile(in_stream):
                            with ZipFile(BytesIO(in_stream.read())) as zfile:
                                zfile.extractall(path)
                        else:
                            filename = in_stream.headers.get_filename()
                            xml = open(path + "/" + filename, "w")
                            xml.write(in_stream.read().decode('utf-8'))
                            xml.close()
                else:
                    os.mkdir(path)
                    with urlopen(url) as in_stream:
                        if is_zipfile(in_stream):
                            with ZipFile(BytesIO(in_stream.read())) as zfile:
                                zfile.extractall(path)
                        else:
                            filename = in_stream.headers.get_filename()
                            xml = open(path + "/" + filename, "w")
                            xml.write(in_stream.read().decode('utf-8'))
                            xml.close()

        self.single_ticket_extraction()

    def json_folder_creation(self):

        def ig_f(dir, files):
            return [f for f in files if os.path.isfile(os.path.join(dir, f))]

        shutil.copytree(self.xml_folder, self.json_folder, ignore=ig_f, dirs_exist_ok=True)

        self.json_conversion()

    def json_conversion(self):

        for subdir, dirs, files in os.walk(self.xml_folder):
            for file in files:
                filepath = subdir + os.sep + file
                if filepath.endswith(".xml"):
                    with open(filepath,'r') as test_file:
                        obj = xmltodict.parse(test_file.read())
                        jd = json.dumps(obj, ensure_ascii=False, indent=4)
                        new_filepath = filepath.replace(self.xml_folder, self.json_folder)
                        final_filepath = new_filepath.replace('.xml', '.json')
                        with open(final_filepath, "w", encoding='utf-8') as jf:
                            jf.write(jd)
    
    def single_ticket_extraction(self):

        df = pd.read_csv("C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Project/BODS/operator_noc_data_catalogue.csv")

        single_catch = list(str(df.loc[df['noc'] == str(self.nocs[0]), 'single catch'].iloc[0]).split(","))
        subdir = self.xml_folder + self.operator_name
        for file in os.listdir(subdir):
            if self.nocs[0] in ['NIBS', 'YSQU']:
                not_contains = all(item not in file for item in single_catch)
                filepath = subdir + '/' + file  
                if not_contains:
                    os.remove(filepath)
            elif self.nocs[0] == "NCTR":
                if file.startswith(single_catch[0]) == False:
                    filepath = subdir + '/' + file
                    os.remove(filepath)
        
        self.json_folder_creation()