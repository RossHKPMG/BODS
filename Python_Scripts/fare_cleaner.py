import os
import pandas as pd 
import shutil
import zipfile
import pathlib
import json
import re
import locale
locale.setlocale(locale.LC_ALL, 'gb_GB')

class FareCleaner:

    error_list = []

    def __init__(self):

        self

    def csv_convert(self, fare_json_folder):

        self.fare_json_folder = fare_json_folder

        final_df  = pd.DataFrame()

        for subdir, dirs, files in os.walk(self.fare_json_folder):
            for file in files:

                filepath = subdir + os.sep + file

                test_json = open(filepath)
                data = json.load(test_json)

                line_id = data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['ServiceFrame']['lines']['Line']['@id']
                line_name = data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['ServiceFrame']['lines']['Line']['Name']
                line_public_code = data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['ServiceFrame']['lines']['Line']['PublicCode']

                scheduled_stop_points = data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['ServiceFrame']['scheduledStopPoints']['ScheduledStopPoint']

                route_stop_ids = [i['@id'] for i in scheduled_stop_points]
                route_stop_names = [i['Name'] for i in scheduled_stop_points]

                fare_zone_points = data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['FareFrame'][0]['fareZones']['FareZone']

                fare_zone_ids = [i['@id'] for i in fare_zone_points]
                fare_zone_names = [i['Name'] for i in fare_zone_points]
                fare_zone_stop_point_ref = [i['members']['ScheduledStopPointRef']['@ref'] for i in fare_zone_points]
                fare_zone_stop_point_text = [i['members']['ScheduledStopPointRef']['#text'] for i in fare_zone_points]

                distance_matrix_element = data['PublicationDelivery']['dataObjects']['CompositeFrame'][0]['frames']['FareFrame'][1]['tariffs']['Tariff']['fareStructureElements']['FareStructureElement'][0]['distanceMatrixElements']['DistanceMatrixElement']

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

                fare_zone_dict = {
                    'Fare Zone ID': fare_zone_ids,
                    'Fare Zone Names': fare_zone_names,
                    'Fare Zone Stop Reference': fare_zone_stop_point_ref
                }

                route_stop_dict = {
                    'Stop ID': route_stop_ids,
                    'Stop Name': route_stop_names
                    }

                tariff_df = pd.DataFrame(tariff_dict)
                fare_zone_df = pd.DataFrame(fare_zone_dict)
                route_df = pd.DataFrame(route_stop_dict)

                route_stops_df = fare_zone_df.merge(route_df, left_on='Fare Zone Stop Reference', right_on='Stop ID')
                route_stops_df = route_stops_df.drop(['Stop ID'], axis=1)

                tariff_sz_df = tariff_df.merge(route_stops_df, left_on='Tariff Start Zone', right_on='Fare Zone ID')
                cols = ['Tariff ID', 'Tariff Start Zone','Fare Zone Stop Reference', 'Stop Name','Tariff End Zone', 'Tariff Price Band']
                tariff_sz_df = tariff_sz_df[cols]

                tariff_ez_df = tariff_df.merge(route_stops_df, left_on='Tariff End Zone', right_on='Fare Zone ID', how='left')
                cols = ['Tariff End Zone','Fare Zone Stop Reference', 'Stop Name']
                tariff_ez_df = tariff_ez_df[cols]
                tariff_ez_df.rename({'Stop Name': 'Stop Name EZ',
                                    'Fare Zone Stop Reference': 'Fare Zone Stop Reference EZ'},axis=1, inplace=True)

                tariff_combined_df = pd.concat([tariff_sz_df, tariff_ez_df], axis=1, join='inner')

                money = []
                for i in tariff_combined_df['Tariff Price Band']:
                    x = re.sub('\D', '', i)
                    x = locale.currency(int(x)/100)
                    x = x.replace('+','')
                    money.append(x)
                tariff_combined_df['Route Cost (£)'] = money

                tariff_combined_df = tariff_combined_df.drop(['Tariff Start Zone', 'Tariff End Zone', 'Tariff Price Band'], axis=1)

                tariff_combined_df.insert(0, column='Public Code', value=line_public_code)
                tariff_combined_df.insert(0, column='Name', value=line_name)
                tariff_combined_df.insert(0, column='ID', value=line_id)

                tariff_combined_df['Fare Zone Stop Reference'] = tariff_combined_df['Fare Zone Stop Reference'].replace('atco:', '', regex=True)
                tariff_combined_df['Fare Zone Stop Reference EZ'] = tariff_combined_df['Fare Zone Stop Reference EZ'].replace('atco:', '', regex=True)

                tariff_combined_df.rename({'ID': 'Line ID',
                                        'Name': 'Line Name',
                                        'Public Code':'Line Public Code',
                                        'Tariff ID': 'Tariff Route ID',
                                        'Stop Name':'Tariff Route Start Name',
                                        'Fare Zone Stop Reference':'Tariff Route Start ID',
                                        'Stop Name EZ':'Tariff Route End Name',
                                        'Fare Zone Stop Reference EZ':'Tariff Route End ID',
                                        'Route Cost (£)':'Tariff Route Cost (£)'},axis=1, inplace=True)

                cols = ['Line ID', 'Line Name', 'Line Public Code', 'Tariff Route ID', 'Tariff Route Start Name', 'Tariff Route Start ID', 'Tariff Route End Name', 'Tariff Route End ID', 'Tariff Route Cost (£)']
                tariff_combined_df = tariff_combined_df[cols]

                final_df = pd.concat([final_df, tariff_combined_df], ignore_index=True)

        elements = str(self.fare_json_folder).split('/')
        final_df.insert(0, column='Passenger Type', value=elements[10])
        final_df.insert(0, column='Route Type', value=elements[9])
        final_df.insert(0, column='Ticket Type', value=elements[8])

        return final_df   

    def df_combine(self,x1,y1, z1, x2, y2, z2):

        self.x1 = x1
        self.y1 = y1
        self.z1 = z1
        self.x2 = x2
        self.y2 = y2
        self.z2 = z2

        combined_df = pd.concat([x1,y1,z1,x2,y2,z2], ignore_index=True)

        elements = str(self.fare_json_folder).split('/')

        string = 'NCTR_' + elements[8] + '.csv'
        string = string.replace(' ','')

        combined_df.to_csv(string, index=False)
