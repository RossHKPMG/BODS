try:
  from Python_Scripts.data_extractor import FaresExtractor
  from Python_Scripts.fare_cleaner import FareCleaner
except:
  from data_extractor import FaresExtractor
  from fare_cleaner import FareCleaner
  
import os

SRC = 'C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS/Fares Data'
Single_Inbound_Adult = 'C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Inbound/Adult'
Single_Inbound_Child = 'C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Inbound/Child'
Single_Inbound_Student = 'C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Inbound/Student'
Single_Outbound_Adult = 'C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Outbound/Adult'
Single_Outbound_Child = 'C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Outbound/Child'
Single_Outbound_Student = 'C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Project/BODS/Nottingham City Transport Ltd_25 JSON/Single Ticket/Outbound/Student'

### Fares Data XML extraction and File Clean       ###
###                                                ###
### ~ 12 mins to complete full Fares Data Download ###

# fares_data_object = FaresExtractor(SRC)
# fares_data_object.xml_extract()
# fares_data_object.tree_clean()

### XML to JSON conversion ###


fare_cleaner_object = FareCleaner()
x1=fare_cleaner_object.csv_convert(Single_Inbound_Adult)
y1=fare_cleaner_object.csv_convert(Single_Inbound_Child)
z1=fare_cleaner_object.csv_convert(Single_Inbound_Student)
x2=fare_cleaner_object.csv_convert(Single_Outbound_Adult)
y2=fare_cleaner_object.csv_convert(Single_Inbound_Child)
z2=fare_cleaner_object.csv_convert(Single_Inbound_Student)

fare_cleaner_object.df_combine(x1,y1,z1,x2,y2,z2)