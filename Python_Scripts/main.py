try:
    #from Python_Scripts.fare_extractor_nctr import FaresExtractorNCTR
    #from Python_Scripts.fare_extractor_nibs import FaresExtractorNIBS
    from Python_Scripts.fare_extractor_first import FaresExtractorFIRST
except:
    #from fare_extractor_nctr import FaresExtractorNCTR
    #from fare_extractor_nibs import FaresExtractorNIBS
    from fare_extractor_first import FaresExtractorFIRST



api = "ab70c6d46880d6fb3418a656e5c8e40fb5aa8f2c"

xml_folder = (
    "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Data/Fare XML/"
)
json_folder = (
    "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Data/Fare JSON/"
)



###########################################################
###                                                     ###
###          Fares Extraction for NIBS (Essex)          ###
###                                                     ###
###########################################################

# nibs = FaresCleaner(
#     xml_folder, json_folder, api_key=api, nocs=["NIBS"], status="published", limit=25, offset=0
# )
# nibs.grab_fare_data()

# nctr = FaresCleaner(
#     xml_folder, json_folder, api_key=api, nocs=["NCTR"], status="published", limit=25, offset=0
# )
# nctr.grab_fare_data()

# ysqu = FaresExtractorNIBS(
#     xml_folder, json_folder, api_key=api, nocs=["YSQU"], status="published", limit=25, offset=0
# )
# ysqu.grab_fare_data()
# ysqu.df_creation()

first_wy = FaresExtractorFIRST(
    xml_folder, json_folder, api_key=api, nocs=["FMAN"], status="published", limit=25, offset=0
)
first_wy.grab_fare_data()
