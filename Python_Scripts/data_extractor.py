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

        catch = ["Single", "single", "SGL", "sgl", "Sgl", "Sin", "sin", "OneWay", "oneway"]
        #passenger_type = ["Adult", "adult", "Child", "child"]
        path = self.xml_folder
        for operator in os.listdir(path):
            path_1 = path + "/" + operator
            for folder in os.listdir(path_1):
                path_2 = path_1 + "/" + folder
                for file in os.listdir(path_2):
                    not_contains_single = all(item not in file for item in catch)
                    #not_contains_passenger = all(item not in file for item in passenger_type)
                    if not_contains_single:
                        os.remove(path_2 + "/" + file)
                    # elif not_contains_passenger:
                    #     os.remove(path_2 + "/" + file)

        print("[Single Tickets Extracted]")

class FareDataExtractor:

    def __init__(self, xml_folder):
        
        self.xml_folder = xml_folder
        self.final_df = pd.DataFrame()
        self.fz_df_combined = pd.DataFrame()
        self.t_df_combined = pd.DataFrame()
        self.actual_t_df_combined = pd.DataFrame()


    def get_fare_data(self):

        error_list = []

        for operator in os.listdir(self.xml_folder):
            op_path = self.xml_folder + "/" + operator
            for noc in os.listdir(op_path):
                noc_path = op_path + "/" + noc
                for file in os.listdir(noc_path):

                    if "_FF_" in file:

                        filepath = noc_path + "/" + file
                        print(filepath)

                        self.root = ET.parse(filepath).getroot()
                        self.namespace = self.root.nsmap
                        
                        self.p_type = self.get_p_type()
                        self.r_type = self.get_r_type()
                        self.ff = self.get_ff()

                    else:
        
                        filepath = noc_path + "/" + file
                        print(filepath)

                        self.root = ET.parse(filepath).getroot()
                        self.namespace = self.root.nsmap

                        self.line_pid = self.get_line_pid()
                        self.p_type = self.get_p_type()
                        self.r_type = self.get_r_type()

                        self.fz_ids = self.get_fz_ids()
                        self.fz_stops = self.get_fz_stops()

                        self.fz_travelled = self.get_fz_travelled()
                        self.fz_start = self.get_fz_start()
                        self.fz_end = self.get_fz_end()
                        self.fz_price = self.get_fz_price()
                        
                        self.fz_df = self.create_fare_zones_df(self.fz_ids, self.fz_stops, self.line_pid, self.r_type)
                        self.t_df = self.create_tariff_df(self.fz_travelled, self.fz_start, self.fz_end, self.fz_price, self.line_pid, self.p_type, self.r_type)

                        print("DataFramesCreated")

                        self.fz_df_combined = pd.concat([self.fz_df_combined, self.fz_df], ignore_index=True, axis=0)
                        self.t_df_combined = pd.concat([self.t_df_combined, self.t_df], ignore_index=True, axis=0)
                        
                        print("DataFramesCombined")
                
                
                x_list = os.listdir(noc_path)
                if "_FF_" in x_list[0]:
                    path = "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Data/Extracted Fare Data/" + operator + "/" + noc + "/"
                    os.makedirs(path)
                    break
                
                else:
                    self.actual_t_df_combined = self.t_df_combined.drop(self.t_df_combined[self.t_df_combined['Route Cost (£)'] == '0.25'].index)

                    self.fz_df_combined = self.fz_df_combined.drop_duplicates(subset=self.fz_df_combined.columns.difference(["Fare Zone Stop References"]))
                    self.t_df_combined = self.t_df_combined.drop_duplicates()
                    self.actual_t_df_combined = self.actual_t_df_combined.drop_duplicates()

                    self.df_to_json(self.fz_df_combined, self.t_df_combined, self.actual_t_df_combined, operator, noc)

                    self.fz_df_combined = pd.DataFrame()
                    self.t_df_combined = pd.DataFrame()
                    self.actual_t_df_combined = pd.DataFrame()

                break    


    ############################################################

    ###        Grabbing Information from XML Functions       ###

    ############################################################

    def get_line_pid(self):
        data = self.root.find("dataObjects/.//ServiceFrame/lines/Line/PublicCode", self.namespace)
        return data.text

    def get_p_type(self):
        data = self.root.find("dataObjects/.//fareProducts/PreassignedFareProduct/Name", self.namespace)
        return data.text

    def get_r_type(self):
        data = self.root.find("./Description", self.namespace)
        if "outbound" in data.text.lower():
            return "Outbound"
        elif "inbound" in data.text.lower():
            return "Inbound"
        else:
            return data.text.lower()
        
    def get_ff(self):
        data = self.root.find("dataObjects/.//amount", self.namespace)
        print(data.text)
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
    
#     for l in range(len(i)):
#    i[l] = i[l].attrib["ref"].replace("atco:", "")
    
    # def get_fz_stops(self):
    #     data = self.root.findall("dataObjects/.//FareZone", self.namespace)
    #     y=[]
    #     x = [elt[:] for elt in data]
    #     for i in data: 
    #         if "membersGroups" in i[1].tag:
    #             for l in range(len(i)):
    #                 i[l] = i[l].attrib["ref"].replace("atco:", "")
        
    #     # for i in x:
    #     #         for l in range(len(i)):
    #     #             i[l] = i[l].attrib["ref"].replace("atco:", "")


        return data
    








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
    
    def create_fare_zones_df(self, fz_ids, fz_s_points, line_pid, r_type):

        fz_dict = {
            "Fare Zone ID": fz_ids,
            "Fare Zone Stop References": fz_s_points,
            "Route Public ID": line_pid,
            "Inbound/Outbound": r_type,
        }
        for i in fz_dict["Fare Zone Stop References"]:
            for l in i:
                l.replace("'","")
        
        fz_df = pd.DataFrame(fz_dict)

        return fz_df
    
    def create_tariff_df(self, fz_travelled, fz_travelled_start, fz_travelled_end, fz_travelled_price, line_pid, p_type, r_type):

        t_dict = {
            "Tariff ID": fz_travelled,
            "Tariff Start Zone": fz_travelled_start,
            "Tariff End Zone": fz_travelled_end,
            "Tariff Price Band": fz_travelled_price,
            "Route Public ID": line_pid,
            "Passenger Type": p_type,
            "Inbound/Outbound": r_type,
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
    
    def df_to_json(self, fz_df_combined, t_df_combined, actual_t_df_combined, operator, noc):

        path = "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Data/Extracted Fare Data/" + operator + "/" + noc + "/"
        os.makedirs(path)
        fz_df_combined.to_json(path + 'Fare_Zone_Data.json', orient='records')
        t_df_combined.to_json(path + 'Ideal_Tariff_Data.json', orient='records')
        actual_t_df_combined.to_json(path + 'Actual_Tariff_Data.json', orient='records')

        print(["Data Tables Created"])







































# self.stop_ids = self.get_stop_ids()
# print(self.stop_ids)
# print(len(self.stop_ids))
# self.stop_names = self.get_stop_names()
# print(self.stop_names)
# print(len(self.stop_names))




# self.op_name = self.get_op_name()
# print(self.op_name)
# self.op_noc = self.get_op_noc()
# print(self.op_noc)
# self.line_id = self.get_line_id()
# print(self.line_id)
# self.line_name = self.get_line_name()
# print(self.line_name)
# self.line_type = self.get_line_type()
# print(self.line_type)
# self.fz_type = self.get_fz_type()
# print(self.fz_type)




# def create_bus_stops_df(self, stop_ids, stop_names):

#     s_dict = {
#         "Stop ID": stop_ids,
#         "Stop Name": stop_names
#     }   
#     s_df = pd.DataFrame(s_dict)
#     return s_df

# def create_route_df(self, line_id, line_pid, p_type, r_type):

#     r_dict = {
#         "Route ID": line_id,
#         "Route Public ID": line_pid,
#         "Passenger Type": p_type,
#         "Inbound/Outbound": r_type,
#         "Ticket Type": "Single"
#     }
#     r_df = pd.DataFrame(r_dict, index=[0])
#     return r_df



# def get_op_name(self):
#     data = self.root.find("dataObjects/.//ResourceFrame/organisations/Operator/Name", self.namespace)
#     return data.text

# def get_op_noc(self):
#     data = self.root.find("dataObjects/.//ResourceFrame/organisations/Operator/PublicCode", self.namespace)
#     return data.text

# def get_line_id(self):
#     data = self.root.find("dataObjects/.//ServiceFrame/lines/Line", self.namespace)
#     return data.attrib["id"]

# def get_line_name(self):
#     data = self.root.find("dataObjects/.//ServiceFrame/lines/Line/Name", self.namespace)
#     return data.text

# def get_line_type(self):
#     data = self.root.find("dataObjects/.//ServiceFrame/lines/Line/LineType", self.namespace)
#     return data.text

# def get_fz_type(self):
#     data = self.root.find("dataObjects/.//FareFrame//Tariff/TariffBasis", self.namespace)
#     return data.text


# def get_stop_ids(self):
#     data = self.root.findall("dataObjects/.//ScheduledStopPoint", self.namespace)
#     x = [elt.attrib["id"].replace("atco:", "") for elt in data]
#     return x

# def get_stop_names(self):
#     data = self.root.findall("dataObjects/.//ScheduledStopPoint/Name", self.namespace)
#     x = [elt.text for elt in data]
#     return x





































    ### OLD CODE ###

    
    # def create_m_df(self, op_name, op_noc, line_id, line_name, line_type, line_pid, fz_type, p_type, r_type):
    #     dict = {
    #         "Operator Name": op_name,
    #         "Operator NOC": op_noc,
    #         "Line ID": line_id,
    #         "Line Public ID": line_pid,
    #         "Line Name": line_name,
    #         "Line Type": line_type,
    #         "Fare Zone Type": fz_type,
    #         "Passenger Type": p_type,
    #         "Ticket Type": "Single",
    #         "Route Type": r_type
    #     }
    #     df = pd.DataFrame([dict])
    #     return df
    
    # def create_r_df(self, stop_ids, stop_names):

    #     dict = {"Stop ID": stop_ids, "Stop Name": stop_names}   
    #     df = pd.DataFrame(dict)
    #     return df
    
    # def create_fz_df(self, fz_ids, fz_names, fz_s_points):

    #     dict = {
    #         "Fare Zone ID": fz_ids,
    #         "Fare Zone Names": fz_names,
    #         "Fare Zone Stop Reference": fz_s_points,
    #     }
    #     df = pd.DataFrame(dict)
    #     df = df.explode("Fare Zone Stop Reference")

    #     temp_df = pd.DataFrame()
    #     pairings = list(itertools.combinations(df["Fare Zone Stop Reference"], r=2))
    #     p1 = []
    #     p2 = []
    #     for pairs in pairings:
    #         p1.append(pairs[0])
    #         p2.append(pairs[1])
    #     temp_df["Fare Zone Stop Reference (Start)"] = p1
    #     temp_df["Fare Zone Stop Reference (End)"] = p2

    #     fz_start_df = df.merge(
    #         temp_df,
    #         left_on="Fare Zone Stop Reference",
    #         right_on="Fare Zone Stop Reference (Start)",
    #     )
    #     cols = ["Fare Zone ID", "Fare Zone Names", "Fare Zone Stop Reference (Start)"]
    #     fz_start_df = fz_start_df[cols]

    #     fz_end_df = pd.merge(
    #         df,
    #         temp_df,
    #         left_on="Fare Zone Stop Reference",
    #         right_on="Fare Zone Stop Reference (End)",
    #         how="right",
    #     )
    #     cols = ["Fare Zone ID", "Fare Zone Names", "Fare Zone Stop Reference (End)"]
    #     fz_end_df = fz_end_df[cols]

    #     fz_end_df.rename(
    #         {"Fare Zone ID": "Fare Zone ID E", "Fare Zone Names": "Fare Zone E"},
    #         axis=1,
    #         inplace=True,
    #     )

    #     fz_df = pd.concat([fz_start_df, fz_end_df], axis=1, join="inner")
    #     return fz_df
    
    # def create_t_df(self, fz_travelled, fz_travelled_start, fz_travelled_end, fz_travelled_price):

    #     dict = {
    #         "Tariff ID": fz_travelled,
    #         "Tariff Start Zone": fz_travelled_start,
    #         "Tariff End Zone": fz_travelled_end,
    #         "Tariff Price Band": fz_travelled_price,
    #     }

    #     df = pd.DataFrame(dict)

    #     return df
    
    # def df_combination(self, temp_df1, temp_df2, temp_df3):

    #     route_stops_df_start = temp_df2.merge(temp_df1, left_on='Fare Zone Stop Reference (Start)', right_on='Stop ID', how='left')
    #     route_stops_df_start = route_stops_df_start.drop(['Stop ID'], axis=1)
    #     cols = ['Fare Zone ID', 'Fare Zone Names','Fare Zone Stop Reference (Start)', 'Stop Name']
    #     route_stops_df_start = route_stops_df_start[cols]

        

    #     route_stops_df_end = temp_df2.merge(temp_df1, left_on='Fare Zone Stop Reference (End)', right_on='Stop ID', how = 'left')
    #     route_stops_df_end = route_stops_df_end.drop(['Stop ID'], axis=1)
    #     route_stops_df_end.rename({'Stop Name': 'Stop Name E'},axis=1, inplace=True)
    #     cols = ['Fare Zone ID E', 'Fare Zone E', 'Fare Zone Stop Reference (End)', 'Stop Name E']
    #     route_stops_df_end = route_stops_df_end[cols]

        

    #     route_stops_df = pd.concat([route_stops_df_start, route_stops_df_end], axis=1, join='inner')

    #     combined_df = pd.merge(route_stops_df, temp_df3,  how='left', left_on=['Fare Zone ID','Fare Zone ID E'], right_on = ['Tariff Start Zone','Tariff End Zone'])

    #     for i in range(len(combined_df)):
    #         x = re.sub(r'[^\d]+', '', combined_df.loc[i, 'Fare Zone ID'])
    #         y = re.sub(r'[^\d]+', '', combined_df.loc[i, 'Fare Zone ID E'])

    #         if pd.isna(combined_df.loc[i, 'Tariff ID']):
    #             combined_df.loc[i, 'Tariff ID'] = x + '+' + y
    #             combined_df.loc[i, 'Tariff Price Band'] = 'price_band_0.25'

    #     cols = ['Tariff ID', 'Fare Zone ID', 'Fare Zone Names', 'Fare Zone Stop Reference (Start)', 'Stop Name', 'Fare Zone ID E', 'Fare Zone E', 'Fare Zone Stop Reference (End)', 'Stop Name E', 'Tariff Price Band']
        
    #     money = []
    #     for i in combined_df['Tariff Price Band']:

    #         x = re.sub('\D', '', i)
    #         x = float(x)
    #         money.append(x)

    #     combined_df['Route Cost (£)'] = money

    #     combined_df = combined_df.drop(['Tariff Price Band'], axis=1)

    #     combined_df['Fare Zone Stop Reference (Start)'] = combined_df['Fare Zone Stop Reference (Start)'].replace('atco:', '', regex=True)
    #     combined_df['Fare Zone Stop Reference (End)'] = combined_df['Fare Zone Stop Reference (End)'].replace('atco:', '', regex=True)

    #     combined_df.rename({'ID': 'Line ID',
    #                                 'Name': 'Line Name',
    #                                 'Public Code':'Line Public Code',
    #                                 'Tariff ID': 'Tariff Route ID',
    #                                 'Fare Zone ID': 'Fare Zone Start ID',
    #                                 'Fare Zone Names': 'Fare Zone Start Name',
    #                                 'Fare Zone Stop Reference (Start)':'Tariff Route Start ID',
    #                                 'Stop Name':'Tariff Route Start Name',
    #                                 'Fare Zone ID E': 'Fare Zone End ID',
    #                                 'Fare Zone E': 'Fare Zone End Name',
    #                                 'Fare Zone Stop Reference (End)':'Tariff Route End ID',
    #                                 'Stop Name E':'Tariff Route End Name',
    #                                 'Route Cost (£)':'Tariff Route Cost (£)'},axis=1, inplace=True)
        
    #     cols = ['Tariff Route ID', 'Fare Zone Start ID', 'Fare Zone Start Name', 'Tariff Route Start Name', 'Tariff Route Start ID', 'Fare Zone End ID', 'Fare Zone End Name', 'Tariff Route End Name', 'Tariff Route End ID', 'Tariff Route Cost (£)']
    #     combined_df = combined_df[cols]

    #     combined_df['Operator Name'] = self.op_name
    #     combined_df['Operator NOC'] = self.op_noc
    #     combined_df['Line ID'] = self.line_id
    #     combined_df['Line Public ID'] = self.line_pid
    #     combined_df['Line Name'] = self.line_name
    #     combined_df['Line Type'] = self.line_type
    #     combined_df['Fare Zone Type'] = self.fz_type
    #     combined_df['Passenger Type'] = self.p_type
    #     combined_df['Ticket Type'] = "Single"
    #     combined_df['Route'] = self.r_type
        
    #     return combined_df
    






                
            