try:
    #from Python_Scripts.fare_extractor_nctr import FaresExtractor
    from Python_Scripts.fare_extractor_nibs import FaresExtractor
except:
    #from fare_extractor_nctr import FaresExtractor
    from fare_extractor_nibs import FaresExtractor

import os

api = 'ab70c6d46880d6fb3418a656e5c8e40fb5aa8f2c'

fdo =  FaresExtractor(
                        api_key = api,
                        nocs = ['NIBS'],
                        status = 'published',
                        limit = 10,
                        offset = 0
                    )



### Initial Grabbing Fare Data, Folder Creation, JSON Conversion ###

fdo.grab_fare_data()

xml_folder = "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Project/BODS/Fare XML/"
json_folder = "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Project/BODS/Fare JSON/"
single_catch = ['single', 'Single', 'sgl', 'SGL']

fdo.json_folder_creation(xml_folder, json_folder)
fdo.json_conversion()
fdo.single_ticket_extraction(single_catch)

df = fdo.df_creation()
print(df)





### Source Files from
# SRC = "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS/Fares Data"


### Fares Data XML extraction and File Clean       ###
###                                                ###
### ~ 12 mins to complete full Fares Data Download ###

# fares_data_object = FaresExtractor(SRC)
# fares_data_object.xml_extract()
# fares_data_object.tree_clean()

# fco = FareCleanerNew()
# df_list = [fco.df_creation(p) for p in nctr_pathways]
# fco.df_to_xlsx(df_list[-1])
# #fco.df_to_csv(df_list[-1])

