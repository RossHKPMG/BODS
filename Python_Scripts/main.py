try:
    from Python_Scripts.data_extractor import FareDataDownloader, FareDataExtractor
except:
    from data_extractor import FareDataDownloader, FareDataExtractor
    


# Insert API key for your specific account
api = "ab70c6d46880d6fb3418a656e5c8e40fb5aa8f2c"

# For testing and application, an seperate folder was created for any of the BODS data, this was just to keep things tidy and not clutter the code files when uploading onto GitHub.
# If you are wanting to test the code, simply create a new empty file and link it as shown below.
xml_folder = (
    "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Data/Fare XML/"
)



###########################################################
###                                                     ###
###                 Fares Extraction                    ###
###                                                     ###
###########################################################



# Do GONW sperately as it's taking too long to download and coming up with errors, even thoguh we only need two NOCS (GONW)
# Do ANUM sperately as it's taking too long to download and coming up with errors, even thoguh we only need two NOCS (ANUM, WRAY)

# "GAGS","TXCO","SELT","AINT","BNGN",
# "GEMS","SWTL","NUTT","ASMT",
# "DGTR","RWSW","WBHV",
# "ROSS","YSQU","SOLU","AAMG","TEXP",
# "SLVL","KEVS","WACT","NATX","ASHT",
# "KLCO","TRVC","BTNA","MRBU","LTKR"



# Running the Fares Data Downloader class to download the specific fares data for the inputted NOCs

# fares = FareDataDownloader(
#     xml_folder, api_key=api, nocs=["GONW"], status="published", limit=40, offset=0
# )
# # fares.grab_fare_data()

# A seperate function create that goes through the downloaded xml files and only grabs the fares data that relates to Single/One-Way & Child/Adult tickets

# fares.single_ticket_extraction()



###########################################################
###                                                     ###
###                 Fares Extraction                    ###
###                                                     ###
###########################################################

# Once the .XML files have been downloaded and filtered out, the Fare Data Extractor goes through and extracts the necessary data that is required for the visualization purposes 
# and returns them as json objects for use within mapbox.

# data = FareDataExtractor(
#     xml_folder
# )
# data.get_fare_data()
