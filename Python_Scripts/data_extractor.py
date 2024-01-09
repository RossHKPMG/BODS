### PACKAGES ###

import os, re, locale, requests

import pandas as pd 
import numpy as np
from urllib.request import urlopen
from zipfile import ZipFile, is_zipfile
from io import BytesIO
import lxml.etree as ET
from unidecode import unidecode


locale.setlocale(locale.LC_ALL, 'gb_GB')

# Probably could be done cleaner, but needed to add in many different clauses/catches due to operators saving their data in different formats/styles,
# tried to create a one-for-all method that could be used for anything no matter the NOCs that you insert for the API request.

class FareDataDownloader:

    error_list = []

    ### Defining some initial values, also the main informatino that is to be passed to the API request for grabbing the given operators fare data ###

    def __init__(self, xml_folder, api_key,  nocs=None, status='published', limit=10_000, offset=0):
        
        self.api_key = api_key
        self.nocs = nocs
        self.status = status
        self.limit = limit
        self.offset = offset
        self.xml_folder = xml_folder

    def grab_fare_data(self):

        # Check for each NOC that is passed through for the API reuest
        # This is still needed even if it is just 1 NOC, as ispassed through within a list and not a single string value

        for i in range(0, len(self.nocs)):


            # Constructing the url string that will be passed through for the API request
            request_string = "https://data.bus-data.dft.gov.uk/api/v1/fares/dataset?" + "noc=" + self.nocs[i] + "&status=" + self.status + "&limit=" + str(self.limit) + "&offset=" + str(self.offset) + "&api_key=" + str(self.api_key)
            
            # request the data from the API URL
            data = requests.get(request_string)

            # Converting request to JSON format
            response_data = data.json()

            # Since there can be multiple responses from the requests, we want to check how many there are
            # for when we need to loop through the data later on
            num_results = len(response_data['results'])


            # Checking to see if the number of responses is greater than 1
            if num_results > 1:

                # Since there are multiple responses, that means there are multiple URLs that link to
                # different download links containing the fares .xml data.
                # Therefore we jump into a loop and perform the below for each response.

                for key in range(0, num_results):


                    # Grabbing the operator name for the request NOC
                    operator_name = response_data["results"][key]["operatorName"]

                    bad_chars = [';', ':', '!', '?', '*', '.', '"']

                    if len(response_data["results"][key]["noc"]) > 1:
                        noc = response_data["results"][key]["description"]
                        for i in bad_chars:
                            noc = noc.replace(i, '')
                        print(noc)
                    else:
                        noc = response_data["results"][key]["noc"][0]

                    # Grabbing the URL that links to the zip/xml files
                    file_url = response_data["results"][key]["url"]
                    
                    # Defining the path that the xml files will be saved into for conversion to JSON
                    path = os.path.join(self.xml_folder, operator_name)
                    subpath = path + "/" + noc

                    # Check to see if the filepath for the operator already exists
                    if os.path.exists(path) or os.path.exists(subpath):

                        # Download request for the zip/xml URL
                        downloaded_file = requests.get(url=file_url)

                        # If the content type of the request is a zip file, then we extract the
                        # contents of the zip file to the specified operators folder
                        if 'zip' in downloaded_file.headers['Content-Type']:
                            with ZipFile(BytesIO(downloaded_file.content)) as zfile:
                                zfile.extractall(subpath)

                        # If the content type of the request is a xml file, then we simply
                        # write the contents of the xml file to the specified operators folder
                        # using the extracted filename
                        elif 'xml' in downloaded_file.headers['Content-Type']:
                            fname = re.findall("filename=(.+)", downloaded_file.headers['content-disposition'])[0]
                            fname = fname.strip('\"')
                            with open((subpath + "/" + fname), "wb") as xml_file:
                                xml_file.write(BytesIO(downloaded_file.content).getbuffer())
                                
                    # If the filepath for the operator does not exist
                    else:

                        # create new folder/path for operator
                        os.mkdir(path)
                        os.mkdir(subpath)

                        downloaded_file = requests.get(url=file_url)

                        if 'zip' in downloaded_file.headers['Content-Type']:
                            with ZipFile(BytesIO(downloaded_file.content)) as zfile:
                                zfile.extractall(subpath)

                        elif 'xml' in downloaded_file.headers['Content-Type']:
                            fname = re.findall("filename=(.+)", downloaded_file.headers['content-disposition'])[0]
                            fname = fname.strip('\"')
                            with open((subpath + "/" + fname), "wb") as xml_file:
                                xml_file.write(BytesIO(downloaded_file.content).getbuffer())
                            

            # Checking to see if the number of responses is equal to 1
            elif num_results == 1:

                bad_chars = [';', ':', '!', '?', '*', '.', '"']

                operator_name = response_data["results"][0]["operatorName"]
                if len(response_data["results"][0]["noc"]) > 1:
                    noc = response_data["results"][0]["description"]
                    for i in bad_chars:
                        noc = noc.replace(i, '')
                else:
                    noc = response_data["results"][0]["noc"][0]
                file_url = response_data["results"][0]["url"]
                path = os.path.join(self.xml_folder, operator_name)
                subpath = path + "/" + noc

                if os.path.exists(path) or os.path.exists(subpath):

                    downloaded_file = requests.get(url=file_url)

                    if 'zip' in downloaded_file.headers['Content-Type']:
                        with ZipFile(BytesIO(downloaded_file.content)) as zfile:
                            zfile.extractall(subpath)

                    elif 'xml' in downloaded_file.headers['Content-Type']:
                            fname = re.findall("filename=(.+)", downloaded_file.headers['content-disposition'])[0]
                            fname = fname.strip('\"')
                            with open((subpath + "/" + fname), "wb") as xml_file:
                                xml_file.write(BytesIO(downloaded_file.content).getbuffer())

                else:

                    os.mkdir(path)
                    os.mkdir(subpath)

                    downloaded_file = requests.get(url=file_url)

                    if 'zip' in downloaded_file.headers['Content-Type']:
                        with ZipFile(BytesIO(downloaded_file.content)) as zfile:
                            zfile.extractall(subpath)

                    elif 'xml' in downloaded_file.headers['Content-Type']:
                            fname = re.findall("filename=(.+)", downloaded_file.headers['content-disposition'])[0]
                            fname = fname.strip('\"')
                            with open((subpath + "/" + fname), "wb") as xml_file:
                                xml_file.write(BytesIO(downloaded_file.content).getbuffer())
            
            print("[All Files Downloaded Successfully]")
        
        # self.single_ticket_extraction(operator_name)
        
    def single_ticket_extraction(self):

        # Again , this was only for initial POC testing purposes to filter out Single/One-Way tickets for Adults/Children

        single_catch = ["Single", "single", "SGL", "sgl", "Sgl", "Sin", "sin", "OneWay", "oneway"]
        passenger_catch = ['Adult', 'AD', 'Child', 'child', 'Chd', 'Ad', 'adult', 'ADULT','CH']
        path = self.xml_folder
        for operator in os.listdir(path):
            path_1 = path + "/" + operator
            for folder in os.listdir(path_1):
                path_2 = path_1 + "/" + folder
                for file in os.listdir(path_2):
                    not_contains_single = all(item not in file for item in single_catch)
                    not_contains_passenger = all(item not in file for item in passenger_catch)
                    if not_contains_single:
                        os.remove(path_2 + "/" + file)
                    elif not_contains_passenger:
                        os.remove(path_2 + "/" + file)

        print("[Single Tickets Extracted]")

class FareDataExtractor:

    def __init__(self, xml_folder):
        
        self.xml_folder = xml_folder
        self.final_df = pd.DataFrame()
        self.fz_df_combined = pd.DataFrame()
        self.t_df_combined = pd.DataFrame()
        self.actual_t_df_combined = pd.DataFrame()
        self.ff_df_combined = pd.DataFrame()


    def get_fare_data(self):

        for operator in os.listdir(self.xml_folder):
            op_path = self.xml_folder + "/" + operator
            for noc in os.listdir(op_path):
                noc_path = op_path + "/" + noc
                for file in os.listdir(noc_path):

                    if "_FF_" in file:

                        # Instance where instead of having multiple different fares attached to routes, there would just be a single Flat Fare attched to all routes (in this case something similar to the £2 government scheme)

                        filepath = noc_path + "/" + file
                        print(filepath)

                        self.root = ET.parse(filepath).getroot()
                        self.namespace = self.root.nsmap

                        # Grabbing necessary infor for visualization and data purposes
                        
                        self.p_type = self.get_p_type()
                        self.d_type = self.get_d_type(file)
                        self.ff_amount = self.get_ff_amount()
                        self.fz_ids = self.get_fz_ids()
                        self.fz_stops = self.get_fz_stops()
                        self.noc_name = noc

                        self.ff_df = self.create_flat_fare_df(self.p_type, self.d_type, self.ff_amount, self.fz_ids, self.fz_stops, self.noc_name)
                        self.ff_df_combined = pd.concat([self.ff_df_combined, self.ff_df], ignore_index=True, axis=0)

                    else:

                        # Instance where there are multiple fares attached for different routes
        
                        filepath = noc_path + "/" + file
                        print(filepath)

                        # Grabbing necessary info for visualization and data purposes, more info required as we need to know what fare zones are being crossed to determine the fare given

                        self.root = ET.parse(filepath).getroot()
                        self.namespace = self.root.nsmap

                        self.line_pid = self.get_line_pid()
                        self.p_type = self.get_p_type()
                        self.d_type = self.get_d_type(file)
                        self.noc_name = noc

                        self.fz_ids = self.get_fz_ids()
                        self.fz_stops = self.get_fz_stops()

                        self.fz_travelled = self.get_fz_travelled()
                        self.fz_start = self.get_fz_start()
                        self.fz_end = self.get_fz_end()
                        self.fz_price = self.get_fz_price()
                        
                        self.fz_df = self.create_fare_zones_df(self.fz_ids, self.fz_stops, self.line_pid, self.d_type, self.noc_name)
                        self.t_df = self.create_tariff_df(self.fz_travelled, self.fz_start, self.fz_end, self.fz_price, self.line_pid, self.p_type, self.d_type, self.noc_name)

                        print("DataFramesCreated")

                        self.fz_df_combined = pd.concat([self.fz_df_combined, self.fz_df], ignore_index=True, axis=0)
                        self.t_df_combined = pd.concat([self.t_df_combined, self.t_df], ignore_index=True, axis=0)
                        
                        print("DataFramesCombined")
            
                
                # Some data cleaning to remove things such as unnesscessary duplicates and defining static values if operators are missing certain data points.
                # This was just a quick/dirty method to allow for visualization purposes, but also helps in highlighting the issue where clients are not populating 
                # their data correctly, which would make it useless when it comes to data quality.
                
                if len(self.ff_df_combined) > 1 and len(self.fz_df_combined) > 1:

                    self.actual_t_df_combined = self.t_df_combined.drop(self.t_df_combined[self.t_df_combined['Route Cost (£)'] == '0.25'].index)

                    self.fz_df_combined = self.fz_df_combined.drop_duplicates(subset=self.fz_df_combined.columns.difference(["Fare Zone Stop References"]))
                    self.t_df_combined = self.t_df_combined.drop_duplicates()
                    self.actual_t_df_combined = self.actual_t_df_combined.drop_duplicates()
                    self.ff_df_combined = self.ff_df_combined.drop_duplicates(subset=self.ff_df_combined.columns.difference(["Fare Zone Stop References"]))

                    # Need to add in FF json conversion and dropping duplicates
                    self.routes_to_json(self.fz_df_combined, self.t_df_combined, self.actual_t_df_combined, operator, noc)
                    self.ff_to_json(self.ff_df_combined, operator, noc)

                    self.fz_df_combined = pd.DataFrame()
                    self.t_df_combined = pd.DataFrame()
                    self.actual_t_df_combined = pd.DataFrame()
                    self.ff_df_combined = pd.DataFrame()

                elif len(self.ff_df_combined) == 0 and len(self.fz_df_combined) > 1:

                    self.actual_t_df_combined = self.t_df_combined.drop(self.t_df_combined[self.t_df_combined['Route Cost (£)'] == '0.25'].index)

                    self.fz_df_combined = self.fz_df_combined.drop_duplicates(subset=self.fz_df_combined.columns.difference(["Fare Zone Stop References"]))
                    self.t_df_combined = self.t_df_combined.drop_duplicates()
                    self.actual_t_df_combined = self.actual_t_df_combined.drop_duplicates()

                    # Need to add in FF json conversion and dropping duplicates
                    self.routes_to_json(self.fz_df_combined, self.t_df_combined, self.actual_t_df_combined, operator, noc)

                    self.fz_df_combined = pd.DataFrame()
                    self.t_df_combined = pd.DataFrame()
                    self.actual_t_df_combined = pd.DataFrame()
                    self.ff_df_combined = pd.DataFrame()
                
                elif len(self.ff_df_combined) > 1 and len(self.fz_df_combined) == 0:

                    self.ff_df_combined = self.ff_df_combined.drop_duplicates(subset=self.ff_df_combined.columns.difference(["Fare Zone Stop References"]))
                    self.ff_to_json(self.ff_df_combined, operator, noc)
                    self.fz_df_combined = pd.DataFrame()
                    self.t_df_combined = pd.DataFrame()
                    self.actual_t_df_combined = pd.DataFrame()
                    self.ff_df_combined = pd.DataFrame()
                    

                   










    ############################################################

    ###        Grabbing Information from XML Functions       ###

    ############################################################

    def get_line_pid(self):
        data = self.root.find("dataObjects/.//ServiceFrame/lines/Line/PublicCode", self.namespace)
        return data.text

    def get_p_type(self):
        adult_check = ["ADULT","Adult","adult","AD","Ad","ad"]
        child_check = ["CHILD","Child","child","CH","Ch","ch","Chd"]
        data = self.root.find("dataObjects/.//fareProducts/PreassignedFareProduct/Name", self.namespace)
        if any(ext in data.text for ext in adult_check):
            x = "Adult"
        elif any(ext in data.text for ext in child_check):
            x = "Child"
        else:
            x = "Error Fix This"
        return x

    def get_d_type(self, file):
        inbound_check = ["Inbound","_I_"]
        outbound_check = ["Outbound","_O_"]
        data = self.root.find("./Description", self.namespace)
        if data == None: 
            if any(ext in file for ext in inbound_check):
                return "Inbound"
            elif any(ext in file for ext in outbound_check):
                return "Outbound"
            else:
                return "ERROR"
        else:
            if "outbound" in data.text.lower():
                return "Outbound"
            elif "inbound" in data.text.lower():
                return "Inbound"
            else:
                return "One Way"
        
    def get_ff_amount(self):
        data = self.root.find("dataObjects/.//TimeIntervalPrice/Amount", self.namespace)
        print(data)
        return data.text
        
    def get_fz_ids(self):
        data = self.root.findall("dataObjects/.//FareZone", self.namespace)
        x = [elt.attrib["id"].replace("fs@", "") for elt in data]
        return x

    def get_fz_stops(self):
        data = self.root.findall("dataObjects/.//FareZone", self.namespace)
        x = [elt[:] for elt in data]
        y = []
        for i in x:
            if len(i) > 1:
                x2 = [elt.attrib['ref'] for elt in i[1]]
                for l in range(len(x2)):
                    x2[l] = x2[l].replace("atco:", "")
                y.append(x2)
            else:
                y.append(["0000000000"])
        return y
    
    def get_fz_travelled(self):
        data = self.root.findall("dataObjects/.//distanceMatrixElements/", self.namespace)
        x = [elt.attrib["id"] for elt in data]
        return x
    
    def get_fz_start(self):
        data = self.root.findall("dataObjects/.//StartTariffZoneRef", self.namespace)
        x = [elt.attrib["ref"].replace("fs@", "") for elt in data]
        return x

    def get_fz_end(self):
        data = self.root.findall("dataObjects/.//EndTariffZoneRef", self.namespace)
        x = [elt.attrib["ref"].replace("fs@", "") for elt in data]
        return x
    
    def get_fz_price(self):
        data = self.root.findall("dataObjects/.//distanceMatrixElements/", self.namespace)
        x = [elt for elt in data]
        y = []
        for i in x:
            if "priceGroups" in i[0].tag:
                price = i[0][0].attrib["ref"]
                y.append(price)
            else:
                y.append("price_band_0.25")
        
        return y
    








    


    ############################################################

    ###            Creating Data Tables Functions            ###

    ############################################################
    
    def create_flat_fare_df(self, p_type, r_type, ff_amount, fz_ids, fz_stops, noc):

        ff_dict = {
            "Passenger Type": p_type,
            "Inbound/Outbound": r_type,
            "Flat Fare Amount": ff_amount,
            "Fare Zone ID": fz_ids,
            "Fare Zone Stop References": fz_stops,
            "NOC": noc
        }
        ff_df = pd.DataFrame(ff_dict, index=[0])

        return ff_df


    def create_fare_zones_df(self, fz_ids, fz_s_points, line_pid, r_type, noc):

        fz_dict = {
            "Fare Zone ID": fz_ids,
            "Fare Zone Stop References": fz_s_points,
            "Route Public ID": line_pid,
            "Direction": r_type,
            "NOC": noc
        }
        for i in fz_dict["Fare Zone Stop References"]:
            for l in i:
                l.replace("'","")
        
        fz_df = pd.DataFrame(fz_dict)

        return fz_df
    
    def create_tariff_df(self, fz_travelled, fz_travelled_start, fz_travelled_end, fz_travelled_price, line_pid, p_type, r_type, noc):

        t_dict = {
            "Tariff ID": fz_travelled,
            "Tariff Start Zone": fz_travelled_start,
            "Tariff End Zone": fz_travelled_end,
            "Tariff Price Band": fz_travelled_price,
            "Route Public ID": line_pid,
            "Passenger Type": p_type,
            "Direction": r_type,
            "NOC": noc
        }
        t_df = pd.DataFrame(t_dict)
        money = []
        for i in t_df['Tariff Price Band']:

            x = np.char.rpartition(i, '_')
            x = x[-1]
            x = "{:.2f}".format(float(x))
            money.append(x)

        t_df['Route Cost (£)'] = money
        t_df = t_df.drop(['Tariff Price Band'], axis=1)
        return t_df
    
    def routes_to_json(self, fz_df_combined, t_df_combined, actual_t_df_combined, operator, noc):

        path = "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Data/Extracted Fare Data/" + operator + "/" + noc + "/"

        if os.path.exists(path):
            fz_df_combined.to_json(path + 'Fare_Zone_Data.json', orient='records')
            t_df_combined.to_json(path + 'Ideal_Tariff_Data.json', orient='records')
            actual_t_df_combined.to_json(path + 'Actual_Tariff_Data.json', orient='records')
        else:
            os.makedirs(path)
            fz_df_combined.to_json(path + 'Fare_Zone_Data.json', orient='records')
            t_df_combined.to_json(path + 'Ideal_Tariff_Data.json', orient='records')
            actual_t_df_combined.to_json(path + 'Actual_Tariff_Data.json', orient='records')

        print(["Data Tables Created"])

    def ff_to_json(self, ff_df_combined, operator, noc):

        path = "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Data/Extracted Fare Data/" + operator + "/" + noc + "/"
        if os.path.exists(path):
            ff_df_combined.to_json(path + 'Flat_Fare_Data.json', orient='records')
        else:
            os.makedirs(path)
            ff_df_combined.to_json(path + 'Flat_Fare_Data.json', orient='records')

        print(["Data Tables Created"])
     
            