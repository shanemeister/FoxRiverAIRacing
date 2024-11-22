import xml.etree.ElementTree as ET
import logging
from src.data_ingestion.ingestion_utils import (
    validate_xml, get_text, safe_float, parse_date, safe_int,
    log_rejected_record, parse_time, update_ingestion_status
)
from datetime import datetime
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_runners_stats_file(xml_file, xsd_schema_path, conn, cursor):
    """
    Process an XML race data file and insert records into the runners_stats table.
    Validates XML, tracks rejected records, and updates ingestion_files status.
    """
    # Validate the XML file
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, xml_file, "error", "runners_stats")  # Update status in ingestion_files
        return

    has_rejections = False  # Track if any records were rejected
    
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        # Process each race element
        for race in root.findall('racedata'):
            try:
                # Extract course code
                course_code = get_text(race.find('track'), 'Unknown')
                if course_code and course_code != 'Unknown':
                    mapped_course_cd = eqb_tpd_codes_to_course_cd.get(course_code)
                    if mapped_course_cd and len(mapped_course_cd) == 3:
                        course_cd = mapped_course_cd
                    else:
                        logging.info(f"Course code '{course_code}' not found in mapping dictionary or mapped course code is not three characters -- defaulting to 'UNK'.")
                        continue  # Skip
                else:
                    logging.info("Course code not found in XML or is 'Unknown' -- defaulting to 'UNK'.")
                    continue  # Skip

                race_date = parse_date(get_text(race.find('race_date')))
                default_time = datetime(1970, 1, 1).time()

                # Parse the post_time or use the default
                post_time_text = get_text(race.find('post_time'))
                if post_time_text:
                    post_time = parse_time(post_time_text)
                else:
                    post_time = default_time
                race_number = safe_int(get_text(race.find('race')))
                country = get_text(race.find('country'))

                for horse in race.findall('horsedata'):
                    # Directly retrieve program_number as string to allow for alphanumeric values
                    saddle_cloth_number = get_text(horse.find('program'))
                    if saddle_cloth_number is None:
                        logging.warning(f"Missing program_number for horse in race {race_number} from file {xml_file}. Assigning default saddle_cloth_number of '0'.")
                        saddle_cloth_number = '0'  # Assign default value
                    
                    # Extract statistics fields
                    avg_spd_sd = safe_float(get_text(horse.find('avg_spd_sd'), '0.0'))
                    ave_cl_sd = safe_float(get_text(horse.find('ave_cl_sd'), '0.0'))
                    hi_spd_sd = safe_float(get_text(horse.find('hi_spd_sd'), '0.0'))
                    pstyerl = safe_float(get_text(horse.find('pstyerl'), '0.0'))
                    # ... [Extract other fields similarly] ...

                    # Prepare the insert query
                    insert_runners_stats_query = """
                    INSERT INTO public.runners_stats (
                        saddle_cloth_number, avg_spd_sd, ave_cl_sd, hi_spd_sd, pstyerl,
                        pstymid, pstyfin, pstynum, pstyoff, prtestyerl,	
                        prtestymid,	prtestyfin,	prtestynum,	prtestyoff,	pallstyerl,
                        pallstymid,	pallstyfin,	pallstynum,	pallstyoff,	pfigerl,
                        pfigmid, pfigfin, pfignum, pfigoff,	psprfigerl,
                        psprfigmid,	psprfigfin,	psprfignum,	psprfigoff,	prtefigerl,
                        prtefigmid,	prtefigfin,	prtefignum,	prtefigoff,	pallfigerl,
                        pallfigmid,	pallfigfin,	pallfignum,	pallfigoff,	psprstyerl,
                        psprstymid, psprstyfin, psprstynum, psprstyoff, course_cd,
                        race_date, post_time, race_number

                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (course_cd, race_date, post_time, race_number, saddle_cloth_number) DO UPDATE 
                    SET 
                        avg_spd_sd = EXCLUDED.avg_spd_sd,
                        ave_cl_sd = EXCLUDED.ave_cl_sd,
                        hi_spd_sd = EXCLUDED.hi_spd_sd,
                        pstyerl = EXCLUDED.pstyerl,
                        pstymid = EXCLUDED.pstymid,
                        pstyfin = EXCLUDED.pstyfin,
                        pstynum = EXCLUDED.pstynum,
                        pstyoff = EXCLUDED.pstyoff,
                        psprstyerl = EXCLUDED.psprstyerl,
                        psprstymid = EXCLUDED.psprstymid,
                        psprstyfin = EXCLUDED.psprstyfin,
                        psprstynum = EXCLUDED.psprstynum,
                        psprstyoff = EXCLUDED.psprstyoff,
                        prtestyerl = EXCLUDED.prtestyerl,
                        prtestymid = EXCLUDED.prtestymid,
                        prtestyfin = EXCLUDED.prtestyfin,
                        prtestynum = EXCLUDED.prtestynum,
                        prtestyoff = EXCLUDED.prtestyoff,
                        pallstyerl = EXCLUDED.pallstyerl,
                        pallstymid = EXCLUDED.pallstymid,
                        pallstyfin = EXCLUDED.pallstyfin,
                        pallstynum = EXCLUDED.pallstynum,
                        pallstyoff = EXCLUDED.pallstyoff,
                        pfigerl = EXCLUDED.pfigerl,
                        pfigmid = EXCLUDED.pfigmid,
                        pfigfin = EXCLUDED.pfigfin,
                        pfignum = EXCLUDED.pfignum,
                        pfigoff = EXCLUDED.pfigoff,
                        psprfigerl = EXCLUDED.psprfigerl,
                        psprfigmid = EXCLUDED.psprfigmid,
                        psprfigfin = EXCLUDED.psprfigfin,
                        psprfignum = EXCLUDED.psprfignum,
                        psprfigoff = EXCLUDED.psprfigoff,
                        prtefigerl = EXCLUDED.prtefigerl,
                        prtefigmid = EXCLUDED.prtefigmid,
                        prtefigfin = EXCLUDED.prtefigfin,
                        prtefignum = EXCLUDED.prtefignum,
                        prtefigoff = EXCLUDED.prtefigoff,
                        pallfigerl = EXCLUDED.pallfigerl,
                        pallfigmid = EXCLUDED.pallfigmid,
                        pallfigfin = EXCLUDED.pallfigfin,
                        pallfignum = EXCLUDED.pallfignum,
                        pallfigoff = EXCLUDED.pallfigoff
                    """
                    try:
                        # Execute the insert
                        cursor.execute(insert_runners_stats_query, (
                            saddle_cloth_number, avg_spd_sd, ave_cl_sd, hi_spd_sd, pstyerl,
                            # ... [Include all other extracted fields in the correct order] ...
                            psprstymid, psprstyfin, psprstynum, psprstyoff, course_cd,
                            race_date, post_time, race_number
                        ))
                        conn.commit()

                    except Exception as e:
                        has_rejections = True
                        conn.rollback()
                        logging.error(f"Error processing runners_stats for saddle cloth {saddle_cloth_number}: {e}")
                        # Convert date and time objects to strings
                        rejected_record = {
                            "course_cd": course_cd,
                            "race_date": race_date.isoformat() if race_date else None,
                            "post_time": post_time.isoformat() if post_time else None,
                            "race_number": race_number,
                            "saddle_cloth_number": saddle_cloth_number,
                            "avg_spd_sd": avg_spd_sd,
                            # ... [Include other fields] ...
                            "pallfigoff": pallfigoff
                        }
                        log_rejected_record(conn, 'runners_stats', rejected_record, str(e))
                        continue  # Skip to the next record
            except Exception as e:
                has_rejections = True
                conn.rollback()
                logging.error(f"Critical error processing race {race_number} in file {xml_file}: {e}")
                # Prepare minimal rejected record data
                rejected_record = {
                    "course_cd": course_cd,
                    "race_date": race_date.isoformat() if race_date else None,
                    "race_number": race_number
                }
                log_rejected_record(conn, 'runners_stats', rejected_record, str(e))
                continue  # Skip to the next race

        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing runners_stats data file {xml_file}: {e}")
        conn.rollback()
        update_ingestion_status(conn, xml_file, "error", "runners_stats")
        return False