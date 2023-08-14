import json
import pandas
import os
import xmltodict

SRC = 'C:/Users/Ross/Documents/BODS/Ross Travel/'


# for file in os.walk(SRC):
#     filepath = str(SRC) + str(file)
#     if filepath.endswith('.xml'):
#         with open(filepath, 'r') as test_file:
#             obj = xmltodict.parse(test_file.read())
#             jd = json.dumps(obj, ensure_ascii=False, indent=4)
#             new_filepath = filepath.replace('Ross Travel', 'Ross Travel JSON')
#             final_filepath = new_filepath.replace('.xml', '.json')
#             jf = open(final_filepath, 'w', encoding='utf-8')
#             jf.write(jd)
#             jf.close 

with open('C:/Users/Ross/Documents/BODS/Ross Travel/ROSS_144_Inbound_ADsgl_43ad4e3d-65a5-49b7-8a29-3dceda1d9905_638241799544014471.xml', 'r') as test_file:
    obj = xmltodict.parse(test_file.read())
    jd = json.dumps(obj, ensure_ascii=False, indent=4)
    new_filepath = 'C:/Users/Ross/Documents/BODS/Ross Travel JSON/ROSS_144_Inbound_ADsgl_43ad4e3d-65a5-49b7-8a29-3dceda1d9905_638241799544014471.xml'
    final_filepath = new_filepath.replace('.xml', '.json')
    jf = open(final_filepath, 'w', encoding='utf-8')
    jf.write(jd)
    jf.close 