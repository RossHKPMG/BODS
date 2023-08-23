import os
import pandas as pd 
import json
import re
import locale
locale.setlocale(locale.LC_ALL, 'gb_GB')

class FareCleanerNew:

    error_list = []

    # def __init__(self, api_key, nocs=None, limit=10_000, offset=0, nocs=None, status='published',
    #              search=None, bods_compliant=True, atco_code=None, service_line_level=False,
    #              stop_level=False, threaded=False):
        
    #     self.api_key = api_key
    #     self.limit = limit
    #     self.offset = offset
    #     self.nocs = nocs
    #     self.status = status
    #     self.search = search
    #     self.bods_compliant = bods_compliant
    #     self.atco_code = atco_code
    #     self.service_line_level = service_line_level
    #     self.stop_level = stop_level
    #     self.threaded = threaded
        
    #     self.final_df = pd.DataFrame()
        
    def df_creation(self, json_folder_pathway):
        
        self.json_folder_pathway = json_folder_pathway
        self.elements = str(self.json_folder_pathway).split('/')
        
        for subdir, dirs, files in os.walk(self.json_folder_pathway):
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
                y = self.cleaning_df(x)
            return y
    
    
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
        fare_zone_stop_point_ref = [i['members']['ScheduledStopPointRef']['@ref'] for i in fare_zone_points]
        
        fare_zone_dict = {
                    'Fare Zone ID': fare_zone_ids,
                    'Fare Zone Names': fare_zone_names,
                    'Fare Zone Stop Reference': fare_zone_stop_point_ref
                }
        
        self.fare_zone_df = pd.DataFrame(fare_zone_dict)
        
    def json_tariff_df(self):
        
        distance_matrix_element = self.data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['FareFrame'][1]['tariffs']['Tariff']['fareStructureElements']['FareStructureElement'][0]['distanceMatrixElements']['DistanceMatrixElement']

        fare_tariff_id = [i['@id'] for i in distance_matrix_element]
        fare_tariff_price_band = [i['priceGroups']['PriceGroupRef']['@ref'] for i in distance_matrix_element]
        fare_tariff_start_zone = [i['StartTariffZoneRef']['@ref'] for i in distance_matrix_element]
        fare_tariff_end_zone = [i['EndTariffZoneRef']['@ref'] for i in distance_matrix_element]
        
        tariff_dict = {
                    'Tariff ID':fare_tariff_id,
                    'Tariff Start Zone': fare_tariff_start_zone,
                    'Tariff End Zone': fare_tariff_end_zone,
                    'Tariff Price Band': fare_tariff_price_band
                }
        
        self.tariff_df = pd.DataFrame(tariff_dict)
    
    def df_combination(self):
        
        self.tariff_combined_df = pd.DataFrame()
        
        route_stops_df = self.fare_zone_df.merge(self.route_df, left_on='Fare Zone Stop Reference', right_on='Stop ID')
        route_stops_df = route_stops_df.drop(['Stop ID'], axis=1)

        tariff_sz_df = self.tariff_df.merge(route_stops_df, left_on='Tariff Start Zone', right_on='Fare Zone ID')
        cols = ['Tariff ID', 'Tariff Start Zone','Fare Zone Stop Reference', 'Stop Name','Tariff End Zone', 'Tariff Price Band']
        tariff_sz_df = tariff_sz_df[cols]

        tariff_ez_df = self.tariff_df.merge(route_stops_df, left_on='Tariff End Zone', right_on='Fare Zone ID', how='left')
        cols = ['Tariff End Zone','Fare Zone Stop Reference', 'Stop Name']
        tariff_ez_df = tariff_ez_df[cols]
        tariff_ez_df.rename({'Stop Name': 'Stop Name EZ',
                            'Fare Zone Stop Reference': 'Fare Zone Stop Reference EZ'},axis=1, inplace=True)

        self.tariff_combined_df = pd.concat([tariff_sz_df, tariff_ez_df], axis=1, join='inner')
        
        return self.tariff_combined_df
    
    def cleaning_df(self, combined_df):
        
        self.combined_df = combined_df
        
        money = []
        for i in self.combined_df['Tariff Price Band']:
            x = re.sub('\D', '', i)
            x = locale.currency(int(x)/100)
            x = x.replace('+','')
            money.append(x)
        self.combined_df['Route Cost (£)'] = money
        
        self.combined_df = self.combined_df.drop(['Tariff Start Zone', 'Tariff End Zone', 'Tariff Price Band'], axis=1)

        self.combined_df.insert(0, column='Public Code', value=self.line_public_code)
        self.combined_df.insert(0, column='Name', value=self.line_name)
        self.combined_df.insert(0, column='ID', value=self.line_id)

        self.combined_df['Fare Zone Stop Reference'] = self.combined_df['Fare Zone Stop Reference'].replace('atco:', '', regex=True)
        self.combined_df['Fare Zone Stop Reference EZ'] = self.combined_df['Fare Zone Stop Reference EZ'].replace('atco:', '', regex=True)

        self.combined_df.rename({'ID': 'Line ID',
                                'Name': 'Line Name',
                                'Public Code':'Line Public Code',
                                'Tariff ID': 'Tariff Route ID',
                                'Stop Name':'Tariff Route Start Name',
                                'Fare Zone Stop Reference':'Tariff Route Start ID',
                                'Stop Name EZ':'Tariff Route End Name',
                                'Fare Zone Stop Reference EZ':'Tariff Route End ID',
                                'Route Cost (£)':'Tariff Route Cost (£)'},axis=1, inplace=True)

        cols = ['Line ID', 'Line Name', 'Line Public Code', 'Tariff Route ID', 'Tariff Route Start Name', 'Tariff Route Start ID', 'Tariff Route End Name', 'Tariff Route End ID', 'Tariff Route Cost (£)']
        self.combined_df = self.combined_df[cols]

        self.final_df = pd.concat([self.final_df, self.combined_df], ignore_index=True)
        
        self.final_df['Passenger Type'] = self.elements[9]
        self.final_df['Route Type'] = self.elements[8]
        self.final_df['Ticket Type'] = self.elements[7]
        
        return self.final_df
    
    
    def df_to_xlsx(self, df):
        
        self.df = df
        string = 'NCTR_' + self.elements[7] + '.xlsx'
        string = string.replace(' ','')
        self.df.to_excel(string, index=False)
        
    def df_to_csv(self, df):
        
        self.df = df
        string = 'NCTR_' + self.elements[7] + '.csv'
        string = string.replace(' ','')
        self.df.to_csv(string, index=False)
    