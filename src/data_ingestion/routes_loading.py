import logging
import os
import glob
from fastkml import kml
from lxml import etree
from shapely.geometry import shape, LineString, MultiLineString
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_tpd_routes_data(conn, directory_path, log_file, processed_files):
    """
    Process KML Routes data files and insert them into the routes table.
    Excludes elevation and ensures proper WKT format for database ingestion.
    """
    logging.info(f"Processing KML files in {directory_path}")

    # Helper function to get track name
    def get_track_name(conn, course_cd):
        query = "SELECT track_name FROM course WHERE course_cd = %s;"
        with conn.cursor() as cursor:
            cursor.execute(query, (course_cd,))
            result = cursor.fetchone()
        return result[0] if result else "Unknown Track"

    # Helper function to process placemark geometries
    def extract_coordinates(geometry):
        """
        Extracts 2D coordinates from LineString or MultiLineString geometry, 
        discarding the elevation (z-coordinate).
        """
        if isinstance(geometry, LineString):
            return LineString([(x, y) for x, y, *_ in geometry.coords])
        elif isinstance(geometry, MultiLineString):
            return MultiLineString([
                LineString([(x, y) for x, y, *_ in line.coords])
                for line in geometry.geoms
            ])
        else:
            return None

    # Helper function to parse and process KML
    def process_kml_file(file_path, conn):
        file_name = os.path.basename(file_path)
        course_code_key = file_name.split('.')[0][-2:].upper()
        course_cd = eqb_tpd_codes_to_course_cd.get(course_code_key, 'UNK')

        if course_cd == 'UNK':
            logging.warning(f"Unknown course code for file {file_name}. Skipping.")
            return

        track_name = get_track_name(conn, course_cd)

        with open(file_path, 'rb') as file:
            parser = etree.XMLParser(ns_clean=True, recover=True)
            tree = etree.parse(file, parser)
        root = tree.getroot()

        kml_content = etree.tostring(root, encoding='unicode', xml_declaration=False)
        k = kml.KML()
        k.from_string(kml_content)

        def process_features(features):
            for feature in features:
                print(f"Processing feature: {feature}")
                if isinstance(feature, (kml.Document, kml.Folder)):
                    process_features(feature.features())
                elif isinstance(feature, kml.Placemark):
                    line_name = feature.name.upper() if feature.name else "UNKNOWN"
                    print(f"Line Name: {line_name}")
                    line_type = (
                        "RUNNING_LINE" if "RUNNING_LINE" in line_name
                        else "WINNING_LINE" if "WINNING_LINE" in line_name
                        else None
                    )
                    if not line_type:
                        logging.info(f"Skipping unrelated placemark: {feature.name}")
                        continue
                    
                    geometry = feature.geometry
                    if geometry.geom_type == "MultiLineString":
                        print("Skipping MultiLineString")
                        continue
                    valid_geometry = extract_coordinates(shape(geometry))
                    
                    if valid_geometry:
                        # Prepare the WKT representation
                        wkt_geometry = valid_geometry.wkt
                        insert_query = """
                            INSERT INTO routes (course_cd, track_name, line_type, line_name, coordinates)
                            VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326))
                            ON CONFLICT (course_cd, line_type) DO UPDATE 
                            SET line_name = EXCLUDED.line_name, coordinates = EXCLUDED.coordinates, track_name = EXCLUDED.track_name;
                        """
                        with conn.cursor() as cursor:
                            cursor.execute(
                                insert_query,
                                (course_cd, track_name, line_type, line_name, wkt_geometry)
                            )
                        conn.commit()
                        logging.info(f"Inserted {line_type} for {line_name} in course {course_cd}.")
                    else:
                        logging.warning(f"Unsupported geometry or no coordinates found in {feature.name}. Skipping.")
                else:
                    logging.info(f"Skipping unsupported feature type: {type(feature)}")

        # Process all features in the KML file
        process_features(k.features())

    # Iterate over all KML files in the directory
    kml_files = glob.glob(os.path.join(directory_path, "*.kml"))
    for kml_file in kml_files:
        try:
            logging.info(f"Processing file {kml_file}")
            process_kml_file(kml_file, conn)
        except Exception as e:
            logging.error(f"Error processing file {kml_file}: {e}", exc_info=True)

    logging.info("Finished processing KML files.")