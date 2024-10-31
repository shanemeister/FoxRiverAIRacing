import os
import xml.etree.ElementTree as ET

def replace_dtv_na(directory_path):
    for dirpath, dirnames, filenames in os.walk(directory_path):
        for filename in filenames:
            if filename.endswith(".xml"):
                file_path = os.path.join(dirpath, filename)
                
                # Parse the XML file
                try:
                    tree = ET.parse(file_path)
                    xml_root = tree.getroot()  # Renamed to avoid conflict with `dirpath`

                    # Iterate over all DTV elements
                    for dtv in xml_root.findall(".//DTV"):
                        if dtv.text == "NA":
                            dtv.text = None  # Set as an empty tag

                    # Save the modified XML file
                    tree.write(file_path, encoding="utf-8", xml_declaration=True)
                    print(f"Processed file: {file_path}")

                except ET.ParseError as e:
                    print(f"Error parsing file {file_path}: {e}")
                except Exception as e:
                    print(f"Unexpected error in file {file_path}: {e}")

# Run the replacement function on your directory
replace_dtv_na("/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/Equibase/ResultsCharts/Daily")