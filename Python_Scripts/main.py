try:
    from Python_Scripts.fare_data_extractor import FareDataDownloader, FareDataExtractor
except:
    from fare_data_extractor import FareDataDownloader, FareDataExtractor
    



api = "ab70c6d46880d6fb3418a656e5c8e40fb5aa8f2c"

xml_folder = (
    "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Data/Fare XML/"
)




###########################################################
###                                                     ###
###          Fares Extraction for NIBS (Essex)          ###
###                                                     ###
###########################################################

# fares = FareDataDownloader(
#     xml_folder, api_key=api, nocs=["YSQU"], status="published", limit=40, offset=0
# )
# fares.grab_fare_data()

data = FareDataExtractor(
    xml_folder
)
data.get_fare_data()
