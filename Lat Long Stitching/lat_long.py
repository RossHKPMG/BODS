import xml.etree.ElementTree as ET
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
 
# Parse the XML data
tree = ET.parse('C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Project/BODS/3_FBSM_PK1147727_14_6_2023-04-11.xml')
root = tree.getroot()
 
# Define the XML namespace
namespace = {'ns': 'http://www.transxchange.org.uk/'}
 
# Initialize lists to store data
line_names = []
route_ids = []
route_sections = []
route_links = []
from_stop_refs = []
from_stop_names = []
to_stop_refs = []
to_stop_names = []
latitudes = []
longitudes = []
 
 
# Create a dictionary to store StopPointRef and CommonName mapping
stop_point_names = {}
stop_points = root.findall('.//ns:StopPoints/ns:AnnotatedStopPointRef', namespace)
for stop_point in stop_points:
    stop_point_ref = stop_point.find('./ns:StopPointRef', namespace).text
    common_name = stop_point.find('./ns:CommonName', namespace).text
    stop_point_names[stop_point_ref] = common_name
 
# Iterate through Services and extract data from Line elements
for service in root.findall('.//ns:Services/ns:Service', namespace):
    line_name = service.find('./ns:Lines/ns:Line/ns:LineName', namespace).text
    for route in root.findall('.//ns:Routes/ns:Route', namespace):
        route_id = route.attrib.get('id')
        route_section_refs = route.findall('./ns:RouteSectionRef', namespace)
        route_section_ids = [route_section_ref.text for route_section_ref in route_section_refs]
        for route_section_id in route_section_ids:
            route_section = root.find(f'./ns:RouteSections/ns:RouteSection[@id="{route_section_id}"]', namespace)
            route_link = route_section.find('./ns:RouteLink', namespace).attrib["id"]
            from_stop_point_ref = route_section.find('./ns:RouteLink/ns:From/ns:StopPointRef', namespace).text
            to_stop_point_ref = route_section.find('./ns:RouteLink/ns:To/ns:StopPointRef', namespace).text
            # latitude = route_section.find('./ns:RouteLink/ns:Track/ns:Mapping/ns:Location/ns:Translation/ns:Latitude', namespace).text
            # longitude = route_section.find('./ns:RouteLink/ns:Track/ns:Mapping/ns:Location/ns:Translation/ns:Longitude', namespace).text


            mapping = route_section.find('./ns:RouteLink/ns:Track/ns:Mapping', namespace)
            x = [elt[0][3].text for elt in mapping]
            latitudes.append(x)
            y = [elt[0][2].text for elt in mapping]
            longitudes.append(y)


            from_stop_name = stop_point_names.get(from_stop_point_ref, '')
            to_stop_name = stop_point_names.get(to_stop_point_ref, '')
            line_names.append(line_name)
            route_ids.append(route_id)
            route_sections.append(route_section_id)
            route_links.append(route_link)
            from_stop_refs.append(from_stop_point_ref)
            from_stop_names.append(from_stop_name)
            to_stop_refs.append(to_stop_point_ref)
            to_stop_names.append(to_stop_name)
            # latitudes.append(latitude)
            # longitudes.append(longitude)

 
# Create a DataFrame from the extracted data
 
 
data = {
    'LineName': line_names,
    'Route ID': route_ids,
    'Route Section': route_sections,
    'Route Link': route_links,
    'From StopRef': from_stop_refs,
    'From Stop Name': from_stop_names,
    'To StopRef': to_stop_refs,
    'To Stop Name': to_stop_names,
    'Latitudes': latitudes,
    'Longitudes': longitudes
}
df = pd.DataFrame(data)

df = df.explode(['Latitudes', 'Longitudes']).reset_index(drop=True)
 
df.to_csv("test.csv")