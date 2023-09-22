### PACKAGES ###

import os, json, re, locale, requests, shutil, xmltodict, itertools, openpyxl, pickle

import pandas as pd 
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from zipfile import ZipFile, is_zipfile
from io import BytesIO

locale.setlocale(locale.LC_ALL, 'gb_GB')


### FARES EXTRACTOR FOR OPERATOR NIBS IN ESSEX ###

class FaresExtractorFIRST:

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

                # Since there are multiple response, that means there are multiple URLs that link to
                # different download links containing the fares .xml data.
                # Therefore we jump into a loop and perform the below for each response.

                for key in range(0, num_results):


                    # Grabbing the operator name for the request NOC
                    self.operator_name = response_data["results"][key]["operatorName"]

                    # Grabbing the URL that links to the zip/xml files
                    file_url = response_data["results"][key]["url"]
                    
                    # Defining the path that the xml files will be saved into for conversion to JSON
                    path = os.path.join(self.xml_folder, self.operator_name)

                    # Check to see if the filepath for the operator already exists
                    if os.path.exists(path):

                        # Download request for the zip/xml URL
                        downloaded_file = requests.get(url=file_url)

                        # If the content type of the request is a zip file, then we extract the
                        # contents of the zip file to the specified operators folder
                        if 'zip' in downloaded_file.headers['Content-Type']:
                            with ZipFile(BytesIO(downloaded_file.content)) as zfile:
                                zfile.extractall(path)

                        # If the content type of the request is a xml file, then we simply
                        # write the contents of the xml file to the specified operators folder
                        # using the extracted filename
                        elif 'xml' in downloaded_file.headers['Content-Type']:
                            fname = re.findall("filename=(.+)", downloaded_file.headers['content-disposition'])[0]
                            fname = fname.strip('\"')
                            with open((path + "/" + fname), "wb") as xml_file:
                                xml_file.write(BytesIO(downloaded_file.content).getbuffer())
                                
                    # If the filepath for the operator does not exist
                    else:

                        # create new folder/path for operator
                        os.mkdir(path)

                        downloaded_file = requests.get(url=file_url)

                        if 'zip' in downloaded_file.headers['Content-Type']:
                            with ZipFile(BytesIO(downloaded_file.content)) as zfile:
                                zfile.extractall(path)

                        elif 'xml' in downloaded_file.headers['Content-Type']:
                            fname = re.findall("filename=(.+)", downloaded_file.headers['content-disposition'])[0]
                            fname = fname.strip('\"')
                            with open((path + "/" + fname), "wb") as xml_file:
                                xml_file.write(BytesIO(downloaded_file.content).getbuffer())
                            

            # Checking to see if the number of responses is equal to 1
            elif num_results == 1:

                self.operator_name = response_data["results"][0]["operatorName"]
                file_url = response_data["results"][0]["url"]
                path = os.path.join(self.xml_folder, self.operator_name)

                if os.path.exists(path):

                    downloaded_file = requests.get(url=file_url)

                    if 'zip' in downloaded_file.headers['Content-Type']:
                        with ZipFile(BytesIO(downloaded_file.content)) as zfile:
                            zfile.extractall(path)

                    elif 'xml' in downloaded_file.headers['Content-Type']:
                            fname = re.findall("filename=(.+)", downloaded_file.headers['content-disposition'])[0]
                            fname = fname.strip('\"')
                            with open((path + "/" + fname), "wb") as xml_file:
                                xml_file.write(BytesIO(downloaded_file.content).getbuffer())

                else:

                    os.mkdir(path)

                    downloaded_file = requests.get(url=file_url)

                    if 'zip' in downloaded_file.headers['Content-Type']:
                        with ZipFile(BytesIO(downloaded_file.content)) as zfile:
                            zfile.extractall(path)

                    elif 'xml' in downloaded_file.headers['Content-Type']:
                            fname = re.findall("filename=(.+)", downloaded_file.headers['content-disposition'])[0]
                            fname = fname.strip('\"')
                            with open((path + "/" + fname), "wb") as xml_file:
                                xml_file.write(BytesIO(downloaded_file.content).getbuffer())
            
            print("[Files extracted successfully]")
        
        self.json_folder_creation()

       
    def json_folder_creation(self):

        def ig_f(dir, files):
            return [f for f in files if os.path.isfile(os.path.join(dir, f))]

        shutil.copytree(self.xml_folder, self.json_folder, ignore=ig_f, dirs_exist_ok=True)

        self.json_conversion()

    def json_conversion(self):

        subdir = self.xml_folder + self.operator_name
        for file in os.listdir(subdir):
            fpath = subdir + "/" + file
            spath = (fpath.replace(self.xml_folder, self.json_folder)).replace('.xml', '.json')
            if fpath.endswith(".xml"):
                xmlf = ET.tostring(ET.parse(fpath).getroot())
                dictf = xmltodict.parse(xmlf, attr_prefix="@", cdata_key="#text", dict_constructor=dict)
                jsonf = json.dumps(dictf, ensure_ascii=False, indent=4)
                with open(spath, "w") as f:
                    f.write(jsonf)
                    
        print("[Fares converted successfully]")

                        
    
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




    ### Open each JSON file --> Extract Nesscessary Fare Info --> Create DF --> Combine DFs --> Clean --> Convert to .xlsx ###
        
    def df_creation(self):

        num = 0
        path = self.json_folder + "/" + self.operator_name
        for subdir, dirs, files in os.walk(path):
            for file in files:
                filepath = subdir + os.sep + file
                test_json = open(filepath)
                self.data = json.load(test_json)
                
                ### Running other functions ###
                self.json_single_info()
                self.json_route_df()
                self.json_fare_zone_df()
                self.json_tariff_df()
                x = self.df_combination()
                y = self.cleaning_df(x, file)
                self.final_df = pd.concat([self.final_df, y], axis=0)
                print(len(y))
                num = num + len(y)

        print(num)
        self.final_df.reset_index(drop=True, inplace=True)
        self.df_to_xlsx(self.final_df)

                
    
    
    def json_single_info(self):
        
        self.line_id = self.data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['ServiceFrame']['lines']['Line']['@id']
        self.line_name = self.data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['ServiceFrame']['lines']['Line']['Name']
        self.line_public_code = self.data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['ServiceFrame']['lines']['Line']['PublicCode']

        
    def json_route_df(self):
        
        scheduled_stop_points = self.data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['ServiceFrame']['scheduledStopPoints']['ScheduledStopPoint']

        route_stop_ids = [i['@id'] for i in scheduled_stop_points]
        route_stop_names = [i['Name'] for i in scheduled_stop_points]

        route_stop_dict = {
                    'Stop ID': route_stop_ids,
                    'Stop Name': route_stop_names
                    }

        self.route_df = pd.DataFrame(route_stop_dict)
        
    def json_fare_zone_df(self):
        
        fare_zone_points = self.data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['FareFrame'][0]['fareZones']['FareZone']

        fare_zone_ids = [i['@id'] for i in fare_zone_points]
        fare_zone_names = [i['Name'] for i in fare_zone_points]
        pre_fare_zone_stop_point_ref= [i['members']['ScheduledStopPointRef']for i in fare_zone_points]

        fare_zone_stop_point_ref = []
        for node1 in pre_fare_zone_stop_point_ref:
            if isinstance(node1, list):
                sub_list = []
                for node2 in node1:
                    sub_list.append(node2['@ref'])
                fare_zone_stop_point_ref.append(sub_list)
            elif isinstance(node1, dict):
                fare_zone_stop_point_ref.append(node1['@ref'])

        fare_zone_dict = {
        'Fare Zone ID': fare_zone_ids,
        'Fare Zone Names': fare_zone_names,
        'Fare Zone Stop Reference': fare_zone_stop_point_ref
        }

        fare_zone_df = pd.DataFrame(fare_zone_dict)
        fare_zone_df = fare_zone_df.explode('Fare Zone Stop Reference')

        pairings = list(itertools.combinations(fare_zone_df['Fare Zone Stop Reference'], r=2))

        temp_df = pd.DataFrame()
        pairings = list(itertools.combinations(fare_zone_df['Fare Zone Stop Reference'], r=2))
        p1 = []
        p2 = []
        for pairs in pairings:
            p1.append(pairs[0])
            p2.append(pairs[1])
        temp_df['Fare Zone Stop Reference (Start)'] = p1
        temp_df['Fare Zone Stop Reference (End)'] = p2





        fare_start_df = fare_zone_df.merge(temp_df, left_on='Fare Zone Stop Reference', right_on='Fare Zone Stop Reference (Start)')
        cols = ['Fare Zone ID', 'Fare Zone Names','Fare Zone Stop Reference (Start)']
        fare_start_df = fare_start_df[cols]


        fare_end_df = pd.merge(fare_zone_df, temp_df, left_on='Fare Zone Stop Reference', right_on='Fare Zone Stop Reference (End)', how = 'right')
        cols = ['Fare Zone ID', 'Fare Zone Names','Fare Zone Stop Reference (End)']
        fare_end_df = fare_end_df[cols]

        fare_end_df.rename({'Fare Zone ID': 'Fare Zone ID E',
                                        'Fare Zone Names': 'Fare Zone E'},axis=1, inplace=True)

        self.fare_combined_df = pd.concat([fare_start_df, fare_end_df], axis=1, join='inner')

        
    def json_tariff_df(self):
        
        distance_matrix_element = self.data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['FareFrame'][1]['tariffs']['Tariff']['fareStructureElements']['FareStructureElement'][0]['distanceMatrixElements']['DistanceMatrixElement']

        if isinstance(distance_matrix_element, list):
            fare_tariff_id = [i['@id'] for i in distance_matrix_element]
            fare_tariff_price_band = [i['priceGroups']['PriceGroupRef']['@ref'] for i in distance_matrix_element]
            fare_tariff_start_zone = [i['StartTariffZoneRef']['@ref'] for i in distance_matrix_element]
            fare_tariff_end_zone = [i['EndTariffZoneRef']['@ref'] for i in distance_matrix_element]
            
        elif isinstance(distance_matrix_element, dict):
            fare_tariff_id = distance_matrix_element['@id']
            fare_tariff_price_band = distance_matrix_element['priceGroups']['PriceGroupRef']['@ref']
            fare_tariff_start_zone = distance_matrix_element['StartTariffZoneRef']['@ref']
            fare_tariff_end_zone = distance_matrix_element['EndTariffZoneRef']['@ref']

        tariff_dict = {
                    'Tariff ID':fare_tariff_id,
                    'Tariff Start Zone': fare_tariff_start_zone,
                    'Tariff End Zone': fare_tariff_end_zone,
                    'Tariff Price Band': fare_tariff_price_band
                }
        
        if isinstance(distance_matrix_element, list):
            self.tariff_df = pd.DataFrame(tariff_dict)
        elif isinstance(distance_matrix_element, dict):
            self.tariff_df = pd.DataFrame(tariff_dict, index=[0])
    
    def df_combination(self):
        
        self.tariff_combined_df = pd.DataFrame()

        route_stops_df_start = self.fare_combined_df.merge(self.route_df, left_on='Fare Zone Stop Reference (Start)', right_on='Stop ID', how='left')
        route_stops_df_start = route_stops_df_start.drop(['Stop ID'], axis=1)
        cols = ['Fare Zone ID', 'Fare Zone Names','Fare Zone Stop Reference (Start)', 'Stop Name']
        route_stops_df_start = route_stops_df_start[cols]

        route_stops_df_end = self.fare_combined_df.merge(self.route_df, left_on='Fare Zone Stop Reference (End)', right_on='Stop ID', how = 'left')
        route_stops_df_end = route_stops_df_end.drop(['Stop ID'], axis=1)
        route_stops_df_end.rename({'Stop Name': 'Stop Name E'},axis=1, inplace=True)
        cols = ['Fare Zone ID E', 'Fare Zone E', 'Fare Zone Stop Reference (End)', 'Stop Name E']
        route_stops_df_end = route_stops_df_end[cols]

        route_stops_df = pd.concat([route_stops_df_start, route_stops_df_end], axis=1, join='inner')

        tariff_combined_df = pd.merge(route_stops_df, self.tariff_df,  how='left', left_on=['Fare Zone ID','Fare Zone ID E'], right_on = ['Tariff Start Zone','Tariff End Zone'])
        tariff_combined_df.head(100000)

        for i in range(len(tariff_combined_df)):
            x = re.sub(r'[^\d]+', '', tariff_combined_df.loc[i, 'Fare Zone ID'])
            y = re.sub(r'[^\d]+', '', tariff_combined_df.loc[i, 'Fare Zone ID E'])

            if pd.isna(tariff_combined_df.loc[i, 'Tariff ID']):
                tariff_combined_df.loc[i, 'Tariff ID'] = x + '+' + y
                tariff_combined_df.loc[i, 'Tariff Price Band'] = 'price_band_0.50'

        cols = ['Tariff ID', 'Fare Zone ID', 'Fare Zone Names', 'Fare Zone Stop Reference (Start)', 'Stop Name', 'Fare Zone ID E', 'Fare Zone E', 'Fare Zone Stop Reference (End)', 'Stop Name E', 'Tariff Price Band']
        self.tariff_combined_df = tariff_combined_df[cols]
        
        return self.tariff_combined_df


            
    def cleaning_df(self, combined_df, file):

        self.combined_df = combined_df
        self.file = file
        
        money = []
        for i in self.combined_df['Tariff Price Band']:
            x = re.sub('\D', '', i)
            x = locale.currency(int(x)/100)
            x = x.replace('+','')
            money.append(x)
        self.combined_df['Route Cost (£)'] = money



        self.combined_df = self.combined_df.drop(['Tariff Price Band'], axis=1)

        self.combined_df.insert(0, column='Public Code', value=self.line_public_code)
        self.combined_df.insert(0, column='Name', value=self.line_name)
        self.combined_df.insert(0, column='ID', value=self.line_id)

        self.combined_df['Fare Zone Stop Reference (Start)'] = self.combined_df['Fare Zone Stop Reference (Start)'].replace('atco:', '', regex=True)
        self.combined_df['Fare Zone Stop Reference (End)'] = self.combined_df['Fare Zone Stop Reference (End)'].replace('atco:', '', regex=True)



        self.combined_df.rename({'ID': 'Line ID',
                                    'Name': 'Line Name',
                                    'Public Code':'Line Public Code',
                                    'Tariff ID': 'Tariff Route ID',
                                    'Fare Zone ID': 'Fare Zone Start ID',
                                    'Fare Zone Names': 'Fare Zone Start Name',
                                    'Fare Zone Stop Reference (Start)':'Tariff Route Start ID',
                                    'Stop Name':'Tariff Route Start Name',
                                    'Fare Zone ID E': 'Fare Zone End ID',
                                    'Fare Zone E': 'Fare Zone End Name',
                                    'Fare Zone Stop Reference (End)':'Tariff Route End ID',
                                    'Stop Name E':'Tariff Route End Name',
                                    'Route Cost (£)':'Tariff Route Cost (£)'},axis=1, inplace=True)

        cols = ['Line ID', 'Line Name', 'Line Public Code', 'Tariff Route ID', 'Fare Zone Start ID', 'Fare Zone Start Name', 'Tariff Route Start Name', 'Tariff Route Start ID', 'Fare Zone End ID', 'Fare Zone End Name', 'Tariff Route End Name', 'Tariff Route End ID', 'Tariff Route Cost (£)']
        self.combined_df = self.combined_df[cols]

        # elements = self.file.split('_')
        # route_ticket = re.findall('[A-Z][^A-Z]*', elements[3])

        # self.combined_df['Passenger Type'] = route_ticket[0]
        # self.combined_df['Route Type'] = elements[2]
        # self.combined_df['Ticket Type'] = route_ticket[1]
        
        return self.combined_df
    
    
    def df_to_xlsx(self, df):
        
        self.df = df
        self.df.to_excel(self.operator_name + '_Fares.xlsx', index=False)
        
    def df_to_csv(self, df):
        
        self.df = df
        string = 'NCTR_' + self.elements[7] + '.csv'
        string = string.replace(' ','')
        self.df.to_csv(string, index=False)






















