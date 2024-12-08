import logging
import os
import glob
from fastkml import kml
from lxml import etree
from pygeoif import geometry as pygeoif_geometry
from shapely.geometry import shape, LineString
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd
from src.data_ingestion.ingestion_utils import log_rejected_record, update_ingestion_status

def process_tpd_routes_data(conn, directory_path, log_file, processed_files):
    """
    Process KML Routes data files and insert them into the routes table.
    """
    has_rejections = False  # Track if any records were rejected

    try:
        logging.info(f"Processing KML files in {directory_path}")

        def get_track_name(conn, course_cd):
            query = "SELECT track_name FROM course WHERE course_cd = %s;"
            with conn.cursor() as cursor:
                cursor.execute(query, (course_cd,))
                result = cursor.fetchone()
            return result[0] if result else "Unknown Track"

        # Loop through all KML files in the directory
        kml_files = glob.glob(os.path.join(directory_path, "*.kml"))
        for kml_file in kml_files:
            try:
                # Extract course code from the file name (last two characters before extension)
                file_name = os.path.basename(kml_file)
                course_code_key = file_name.split('.')[0][-2:].upper()  # Extract last 2 chars before '.kml'
                course_cd = eqb_tpd_codes_to_course_cd.get(course_code_key, 'UNK')

                if course_cd == 'UNK':
                    logging.warning(f"Unknown course code for file {file_name}. Skipping.")
                    continue

                # Get track_name from the course table
                track_name = get_track_name(conn, course_cd)

                # Parse the KML file using lxml.etree.parse
                with open(kml_file, 'rb') as file:
                    parser = etree.XMLParser(ns_clean=True, recover=True)
                    tree = etree.parse(file, parser)
                root = tree.getroot()

                # Remove XML encoding declaration by serializing without xml_declaration
                kml_content = etree.tostring(root, encoding='unicode', xml_declaration=False)

                # Initialize KML object
                k = kml.KML()
                k.from_string(kml_content)

                # Recursively process features
                def process_features(features, course_cd, track_name):
                    for feature in features:
                        print(f"Processing feature: {feature}")
                        if isinstance(feature, (kml.Document, kml.Folder)):
                            process_features(feature.features(), course_cd, track_name)
                        elif isinstance(feature, kml.Placemark):
                            placemark = feature
                            line_name = placemark.name.upper() if placemark.name else ""
                            line_type = (
                                "RUNNING_LINE" if "RUNNING_LINE" in line_name
                                else "WINNING_LINE" if "WINNING_LINE" in line_name
                                else None
                            )
                            if line_type is None:
                                logging.info(f"Skipping unrelated placemark: {placemark.name}")
                                continue

                            # Extract LineStrings from geometry
                            def extract_line_strings(geometry):
                                line_strings = []
                                if isinstance(geometry, pygeoif_geometry.LineString):
                                    line_strings.append(shape(geometry))  # Convert to shapely LineString
                                elif isinstance(geometry, (pygeoif_geometry.MultiLineString, pygeoif_geometry.GeometryCollection)):
                                    for geom in geometry.geoms:
                                        line_strings.extend(extract_line_strings(geom))
                                else:
                                    logging.warning(f"Unsupported geometry type '{type(geometry)}' in {placemark.name}")
                                return line_strings

                            geometry = placemark.geometry
                            line_strings = extract_line_strings(geometry)
                            if line_strings:
                                # Combine all line strings into one
                                all_coords = []
                                for line in line_strings:
                                    all_coords.extend(line.coords)
                                combined_line = LineString(all_coords)
                                # Insert into the database
                                insert_query = """
                                    INSERT INTO routes (course_cd, track_name, line_type, line_name, coordinates)
                                    VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326))
                                    ON CONFLICT (course_cd, line_type, line_name) DO UPDATE 
                                    SET coordinates = EXCLUDED.coordinates, track_name = EXCLUDED.track_name;
                                """
                                with conn.cursor() as cursor:
                                    cursor.execute(
                                        insert_query,
                                        (course_cd, track_name, line_type, placemark.name, combined_line.wkt)
                                    )
                                conn.commit()
                                logging.info(f"Inserted {line_type} for {placemark.name} in course {course_cd}.")
                            else:
                                logging.warning(f"No LineStrings found in {placemark.name}. Skipping.")
                        else:
                            logging.info(f"Skipping unsupported feature type: {type(feature)}")

                # Start processing from the root features
                process_features(k.features(), course_cd, track_name)

            except Exception as file_error:
                has_rejections = True
                logging.error(f"Error processing file {kml_file}: {file_error}", exc_info=True)
                log_rejected_record(conn, 'routes', {"file_name": kml_file}, str(file_error))
                continue

    except Exception as e:
        has_rejections = True
        logging.error(f"Critical error processing KML files: {e}", exc_info=True)
        update_ingestion_status(conn, directory_path, "error", "routes")
        return "error"

    # Update ingestion status
    status = "processed_with_rejections" if has_rejections else "processed"
    update_ingestion_status(conn, directory_path, status, "routes")
    return status