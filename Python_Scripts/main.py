try:
    from Python_Scripts.data_extractor import FaresExtractor
    from Python_Scripts.fare_cleaner import FareCleaner
    from Python_Scripts.class_fare_cleaner import FareCleanerNew
except:
    from data_extractor import FaresExtractor
    from fare_cleaner import FareCleaner
    from class_fare_cleaner import FareCleanerNew

import os

# api = 

# fare_data_object =  FareCleanerNew(api_key = api,
#                                     nocs  =['BPTR','RBTS'],
#                                     status = 'published'
#                                     limit = 0,
#                                     offset = 0,
#                                     bods_compliant=True
#                                     )





### Source Files from
# SRC = "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS/Fares Data"


# nctr_pathways = [
#     "C:/Users/Ross/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Inbound/Adult",
#     "C:/Users/Ross/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Inbound/Child",
#     "C:/Users/Ross/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Inbound/Student",
#     "C:/Users/Ross/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Outbound/Adult",
#     "C:/Users/Ross/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Outbound/Child",
#     "C:/Users/Ross/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Outbound/Student",
# ]

### Fares Data XML extraction and File Clean       ###
###                                                ###
### ~ 12 mins to complete full Fares Data Download ###

# fares_data_object = FaresExtractor(SRC)
# fares_data_object.xml_extract()
# fares_data_object.tree_clean()

fco = FareCleanerNew()
df_list = [fco.df_creation(p) for p in nctr_pathways]
fco.df_to_xlsx(df_list[-1])
#fco.df_to_csv(df_list[-1])

