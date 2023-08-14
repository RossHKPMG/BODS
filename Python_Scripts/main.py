try:
  from Python_Scripts.data_extractor import FaresExtractor
except:
  from data_extractor import FaresExtractor
  
import os

SRC = 'C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS/Fares Data'

### Fares Data XML extraction and File Clean       ###
###                                                ###
### ~ 12 mins to complete full Fares Data Download ###

fares_data_object = FaresExtractor(SRC)
fares_data_object.xml_extract()
fares_data_object.tree_clean()

### XML to JSON conversion ###