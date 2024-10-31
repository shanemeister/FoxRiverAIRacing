import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text, safe_float, parse_date, safe_int, gen_race_identifier, log_rejected_record
from datetime import datetime

def process_runners_stats_file(xml_file, xsd_schema_path, conn, cursor):
    """
    Process individual XML race data file and insert into the runners table.
    Validates the XML against the provided XSD schema.
    """
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return  # Skip processing this file

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Iterate over each race data
        for race in root.findall('racedata'):
            # 1
            course_cd = race.find('track').text 
            #2
            #logging.info(f"Processing runners for course {course_cd}")
            race_date = parse_date(get_text(race.find('race_date')))
            #logging.info(f"Processing race_date {race_date}")
            # 3
            race_number = safe_int(get_text(race.find('race')))
            # 4
            race_identifier = gen_race_identifier(course_cd,race_date, race_number)
            # Iterate over each horse data
            for horse in race.findall('horsedata'):
                try:
                    # logging.info(f"Program Number: {safe_int(get_text(horse.find('program')))}")    
                                        # Directly retrieve `program_number` to handle alphanumeric cases
                    program_number = get_text(horse.find('program'))
                    if program_number is None:
                        logging.warning(f"Skipping horse with missing program_number in race {race_number}")
                        continue  # Skip to next horse if `program_number` is missing
                    
                    avg_spd_sd = safe_float(get_text(horse.find('avg_spd_sd'), '0.0'))
                    ave_cl_sd = safe_float(get_text(horse.find('ave_cl_sd'), '0.0'))
                    hi_spd_sd = safe_float(get_text(horse.find('hi_spd_sd'), '0.0'))
                    pstyerl = safe_float(get_text(horse.find('pstyerl'), '0.0'))
                    pstymid = safe_float(get_text(horse.find('pstymid'), '0.0'))
                    pstyfin = safe_float(get_text(horse.find('pstyfin'), '0.0'))
                    pstynum = safe_int(get_text(horse.find('pstynum'), '0'))
                    pstyoff = safe_int(get_text(horse.find('pstyoff'), '0'))
                    psprstyerl = safe_float(get_text(horse.find('psprstyerl'), '0.0'))
                    psprstymid = safe_float(get_text(horse.find('psprstymid'), '0.0'))
                    psprstyfin = safe_float(get_text(horse.find('psprstyfin'), '0.0'))
                    psprstynum = safe_int(get_text(horse.find('psprstynum'), '0'))
                    psprstyoff = safe_int(get_text(horse.find('psprstyoff'), '0'))
                    prtestyerl = safe_float(get_text(horse.find('prtestyerl'), '0.0'))
                    prtestymid = safe_float(get_text(horse.find('prtestymid'), '0.0'))
                    prtestyfin = safe_float(get_text(horse.find('prtestyfin'), '0.0'))
                    prtestynum = safe_int(get_text(horse.find('prtestynum'), '0'))
                    prtestyoff = safe_int(get_text(horse.find('prtestyoff'), '0'))
                    pallstyerl = safe_float(get_text(horse.find('pallstyerl'), '0.0'))
                    pallstymid = safe_float(get_text(horse.find('pallstymid'), '0.0'))
                    pallstyfin = safe_float(get_text(horse.find('pallstyfin'), '0.0'))
                    pallstynum = safe_int(get_text(horse.find('pallstynum'), '0'))
                    pallstyoff = safe_int(get_text(horse.find('pallstyoff'), '0'))
                    pfigerl = safe_float(get_text(horse.find('pfigerl'), '0.0'))
                    pfigmid = safe_float(get_text(horse.find('pfigmid'), '0.0'))
                    pfigfin = safe_float(get_text(horse.find('pfigfin'), '0.0'))
                    pfignum = safe_int(get_text(horse.find('pfignum'), '0'))
                    pfigoff = safe_int(get_text(horse.find('pfigoff'), '0'))
                    psprfigerl = safe_float(get_text(horse.find('psprfigerl'), '0.0'))
                    psprfigmid = safe_float(get_text(horse.find('psprfigmid'), '0.0'))
                    psprfigfin = safe_float(get_text(horse.find('psprfigfin'), '0.0'))
                    psprfignum = safe_int(get_text(horse.find('psprfignum'), '0'))
                    psprfigoff = safe_int(get_text(horse.find('psprfigoff'), '0'))
                    prtefigerl = safe_float(get_text(horse.find('prtefigerl'), '0.0'))
                    prtefigmid = safe_float(get_text(horse.find('prtefigmid'), '0.0'))
                    prtefigfin = safe_float(get_text(horse.find('prtefigfin'), '0.0'))
                    prtefignum = safe_int(get_text(horse.find('prtefignum'), '0'))
                    prtefigoff = safe_int(get_text(horse.find('prtefigoff'), '0'))
                    pallfigerl = safe_float(get_text(horse.find('pallfigerl'), '0.0'))
                    pallfigmid = safe_float(get_text(horse.find('pallfigmid'), '0.0'))
                    pallfigfin = safe_float(get_text(horse.find('pallfigfin'), '0.0'))
                    pallfignum = safe_int(get_text(horse.find('pallfignum'), '0'))
                    pallfigoff = safe_int(get_text(horse.find('pallfigoff'), '0'))
                            
                    insert_runners_stats_query = """
                    INSERT INTO public.runners_stats (
                        race_identifier, program_number, avg_spd_sd, ave_cl_sd, hi_spd_sd, 
                        pstyerl, pstymid, pstyfin, pstynum, pstyoff, 
                        psprstyerl, psprstymid, psprstyfin, psprstynum, psprstyoff, 
                        prtestyerl, prtestymid, prtestyfin, prtestynum, prtestyoff, 
                        pallstyerl, pallstymid, pallstyfin, pallstynum, pallstyoff, 
                        pfigerl, pfigmid, pfigfin, pfignum, pfigoff,
                        psprfigerl, psprfigmid, psprfigfin, psprfignum, psprfigoff,
                        prtefigerl, prtefigmid, prtefigfin, prtefignum, prtefigoff,
                        pallfigerl, pallfigmid, pallfigfin, pallfignum, pallfigoff
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s 
                    )
                    ON CONFLICT (race_identifier, program_number) DO UPDATE 
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
                    #logging.info(f"Inserting runners_stats data with program number {program_number}")
                    # Execute the insert query for runners
                    cursor.execute(insert_runners_stats_query, (
                        race_identifier, program_number, avg_spd_sd, ave_cl_sd, hi_spd_sd, 
                        pstyerl, pstymid, pstyfin, pstynum, pstyoff, 
                        psprstyerl, psprstymid, psprstyfin, psprstynum, psprstyoff, 
                        prtestyerl, prtestymid, prtestyfin, prtestynum, prtestyoff, 
                        pallstyerl, pallstymid, pallstyfin, pallstynum, pallstyoff, 
                        pfigerl, pfigmid, pfigfin, pfignum, pfigoff,
                        psprfigerl, psprfigmid, psprfigfin, psprfignum, psprfigoff,
                        prtefigerl, prtefigmid, prtefigfin, prtefignum, prtefigoff,
                        pallfigerl, pallfigmid, pallfigfin, pallfignum, pallfigoff
                   ))
                except Exception as horse_error:
                    logging.error(f"Error processing runners_stats for program_number {program_number}: {horse_error}")
                    rejected_record = {
                        "race_identifier": race_identifier,
                        "program_number": program_number,
                        "avg_spd_sd": avg_spd_sd,
                        "ave_cl_sd": ave_cl_sd,
                        "hi_spd_sd": hi_spd_sd,
                        "pstyerl": pstyerl,
                        "pstymid": pstymid,
                        "pstyfin": pstyfin,
                        "pstynum": pstynum,
                        "pstyoff": pstyoff,
                        "psprstyerl": psprstyerl,
                        "psprstymid": psprstymid,
                        "psprstyfin": psprstyfin,
                        "psprstynum": psprstynum,
                        "psprstyoff": psprstyoff,
                        "prtestyerl": prtestyerl,
                        "prtestymid": prtestymid,
                        "prtestyfin": prtestyfin,
                        "prtestynum": prtestynum,
                        "prtestyoff": prtestyoff,
                        "pallstyerl": pallstyerl,
                        "pallstymid": pallstymid,
                        "pallstyfin": pallstyfin,
                        "pallstynum": pallstynum,
                        "pallstyoff": pallstyoff,
                        "pfigerl": pfigerl,
                        "pfigmid": pfigmid,
                        "pfigfin": pfigfin,
                        "pfignum": pfignum,
                        "pfigoff": pfigoff,
                        "psprfigerl": psprfigerl,
                        "psprfigmid": psprfigmid,
                        "psprfigfin": psprfigfin,
                        "psprfignum": psprfignum,
                        "psprfigoff": psprfigoff,
                        "prtefigerl": prtefigerl,
                        "prtefigmid": prtefigmid,
                        "prtefigfin": prtefigfin,
                        "prtefignum": prtefignum,
                        "prtefigoff": prtefigoff,
                        "pallfigerl": pallfigerl,
                        "pallfigmid": pallfigmid,
                        "pallfigfin": pallfigfin,
                        "pallfignum": pallfignum,
                        "pallfigoff": pallfigoff
                    }
                    log_rejected_record('runners_stats', rejected_record)
                    continue  # Skip to the next race after logging the error
        # Commit the transaction after all runner data has been processed
        conn.commit()

    except Exception as e:
        logging.error(f"Error processing runner data in file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error