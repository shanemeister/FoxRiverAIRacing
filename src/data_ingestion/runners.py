import xml.etree.ElementTree as ET
import logging
from src.data_ingestion.ingestion_utils import (
    validate_xml, get_text, safe_float, parse_date, clean_text, parse_time,
    odds_to_probability, gen_race_identifier, log_rejected_record, safe_int, update_ingestion_status
)
from src.data_ingestion.mappings_dictionaries import eqb_tpd_codes_to_course_cd
from datetime import datetime

def process_runners_file(xml_file, xsd_file_path, conn, cursor):
    """
    Processes an XML race data file, inserts records into the runners table, and logs rejected records.
    """
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, cursor, xml_file, "error")
        return
    has_rejections = False  # Track if any records were rejected
    rejected_record = {}  # Store rejected records for logging

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Process each race in the XML
        for race in root.findall('racedata'):
            try:
                course_cd = eqb_tpd_codes_to_course_cd.get(get_text(race.find('track'), 'EQE'))  # Default 'EQE' for course_cd if missing          
                race_date = parse_date(race.find('race_date').text)
                post_time = parse_time(get_text(race.find('post_time'))) or datetime.strptime("00:00", "%H:%M").time()
                race_number = safe_int(get_text(race.find('race')))
                country = get_text(race.find('country'))

                # Initialize variables for logging in case of an exception
                saddle_cloth_number = axciskey = post_position = todays_cls = None
                owner_name = turf_mud_mark = avg_purse_val_calc = weight = wght_shift = cldate = None
                price = bought_fr = power = med = equip = morn_odds_str = breeder = ae_flag = power_symb = None
                horse_comm = breed_type = lst_salena = lst_salepr = lst_saleda = claimprice = avgspd = avgcls = apprweight = None
                jock_key = train_key = None

                # Process each horse in the race
                for horse in race.findall('horsedata'):
                    # Retrieve and set defaults for program number and axciskey
                    saddle_cloth_number = get_text(horse.find('program')) or 0
                    axciskey = get_text(horse.find('axciskey'))
                    if not axciskey:
                        logging.warning(f"Missing axciskey for horse in race {race_number} from file {xml_file}. Skipping runner data.")
                        continue

                    # Extract fields
                    post_position = safe_int(get_text(horse.find('pp')))
                    todays_cls = safe_int(get_text(horse.find('todays_cls')))
                    owner_name = clean_text(get_text(horse.find('owner_name'), ''))
                    turf_mud_mark = get_text(horse.find('tmmark'), '')
                    avg_purse_val_calc = safe_float(get_text(horse.find('av_pur_val'), '0.0'))
                    weight = safe_float(get_text(horse.find('weight'), '0.0'))
                    wght_shift = safe_float(get_text(horse.find('wght_shift'), '0.0'))
                    cldate = parse_date(get_text(horse.find('cldate')))
                    price = safe_float(get_text(horse.find('price'), '0.0'))
                    bought_fr = get_text(horse.find('bought_fr'), '')
                    power = safe_float(get_text(horse.find('power'), '0.0'))
                    med = get_text(horse.find('med'), '')
                    equip = get_text(horse.find('equip'), 'NA')
                    morn_odds_str = get_text(horse.find('morn_odds'), '')
                    morn_odds = odds_to_probability(morn_odds_str) if morn_odds_str else 0.0
                    breeder = get_text(horse.find('breeder'), '')
                    ae_flag = get_text(horse.find('ae_flag'), '')
                    power_symb = get_text(horse.find('power_symb'), '')
                    horse_comm = get_text(horse.find('horse_comm'), '')
                    breed_type = get_text(horse.find('breed_type'), '')
                    lst_salena = get_text(horse.find('lst_salena'), '')
                    lst_salepr = safe_float(get_text(horse.find('lst_salepr'), '0.0'))
                    lst_saleda = parse_date(get_text(horse.find('lst_saleda')))
                    claimprice = safe_float(get_text(horse.find('claimprice'), '0.0'))
                    avgspd = safe_float(get_text(horse.find('avgspd'), '0.0'))
                    avgcls = safe_float(get_text(horse.find('avgcl'), '0.0'))
                    apprweight = safe_float(get_text(horse.find('apprweight'), '0.0'))
                    jock_key = get_text(horse.find('jockey/jock_key'))
                    train_key = get_text(horse.find('trainer/train_key'))

                    # Insert data into runners table
                    insert_query ="""
                        INSERT INTO public.runners (
                            course_cd, race_date, post_time, race_number, saddle_cloth_number,  
                            country, axciskey, post_position, todays_cls, owner_name,
                            turf_mud_mark, avg_purse_val_calc, weight, wght_shift, cldate,
                            price, bought_fr, power, med, equip,
                            morn_odds, breeder, ae_flag, power_symb, horse_comm,
                            breed_type, lst_salena, lst_salepr, lst_saleda, claimprice,
                            avgspd, avgcls, apprweight, jock_key, train_key     
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                  %s, %s, %s, %s, %s)
                        ON CONFLICT (course_cd, race_date, race_number, saddle_cloth_number) DO UPDATE 
                        SET country = EXCLUDED.country,
                            axciskey = EXCLUDED.axciskey,
                            post_position = EXCLUDED.post_position,
                            todays_cls = EXCLUDED.todays_cls,
                            owner_name = EXCLUDED.owner_name,
                            turf_mud_mark = EXCLUDED.turf_mud_mark,
                            avg_purse_val_calc = EXCLUDED.avg_purse_val_calc,
                            weight = EXCLUDED.weight,
                            wght_shift = EXCLUDED.wght_shift,
                            cldate = EXCLUDED.cldate,
                            price = EXCLUDED.price,
                            bought_fr = EXCLUDED.bought_fr,
                            power = EXCLUDED.power,
                            med = EXCLUDED.med,
                            equip = EXCLUDED.equip,
                            morn_odds = EXCLUDED.morn_odds,
                            breeder = EXCLUDED.breeder,
                            ae_flag = EXCLUDED.ae_flag,
                            power_symb = EXCLUDED.power_symb,
                            horse_comm = EXCLUDED.horse_comm,
                            breed_type = EXCLUDED.breed_type,
                            lst_salena = EXCLUDED.lst_salena,
                            lst_salepr = EXCLUDED.lst_salepr,
                            lst_saleda = EXCLUDED.lst_saleda,
                            claimprice = EXCLUDED.claimprice,
                            avgspd = EXCLUDED.avgspd,
                            avgcls = EXCLUDED.avgcls,
                            apprweight = EXCLUDED.apprweight,
                            jock_key = EXCLUDED.jock_key,
                            train_key = EXCLUDED.train_key
                    """
                    try:
                        # Execute the query
                        cursor.execute(insert_query, (
                            course_cd, race_date, post_time, race_number, saddle_cloth_number,  
                            country, axciskey, post_position, todays_cls, owner_name,
                            turf_mud_mark, avg_purse_val_calc, weight, wght_shift, cldate,
                            price, bought_fr, power, med, equip,
                            morn_odds, breeder, ae_flag, power_symb, horse_comm,
                            breed_type, lst_salena, lst_salepr, lst_saleda, claimprice,
                            avgspd, avgcls, apprweight, jock_key, train_key          
                        ))
                        conn.commit()  # Commit the transaction
                        #logging.info(f"Inserted runner data for {axciskey} in file: {xml_file}")

                    except Exception as race_error:
                        has_rejections = True
                        logging.error(f"Error processing race: {race_number}, error: {race_error}")
                        rejected_record = {
                            "course_cd": course_cd,
                            "race_date": race_date,
                            "post_time": post_time,
                            "race_number": race_number,
                            "saddle_cloth_number": saddle_cloth_number,
                            "country": country,
                            "axciskey": axciskey,
                            "post_position": post_position,
                            "todays_cls": todays_cls,
                            "owner_name": owner_name,
                            "turf_mud_mark": turf_mud_mark,
                            "avg_purse_val_calc": avg_purse_val_calc,
                            "weight": weight,
                            "wght_shift": wght_shift,
                            "cldate": cldate,
                            "price": price,
                            "bought_fr": bought_fr,
                            "power": power,
                            "med": med,
                            "equip": equip,
                            "morn_odds": morn_odds,
                            "breeder": breeder,
                            "ae_flag": ae_flag,
                            "power_symb": power_symb,
                            "horse_comm": horse_comm,
                            "breed_type": breed_type,
                            "lst_salena": lst_salena,
                            "lst_salepr": lst_salepr,
                            "lst_saleda": lst_saleda,
                            "claimprice": claimprice,
                            "avgspd": avgspd,
                            "avgcls": avgcls,
                            "apprweight": apprweight,
                            "jock_key": jock_key,
                            "train_key": train_key
                        }
                        conn.rollback()  # Rollback the transaction before logging the rejected record
                        logging.error(f"Rejected record for runner {axciskey} in file {xml_file}")
                        log_rejected_record(conn, 'runners', rejected_record, str(race_error))
                        continue  # Skip to the next race record
            except Exception as e:
                has_rejections = True
                conn.rollback()  # Rollback the transaction before logging the rejected record
                log_rejected_record(conn, 'runners', rejected_record, str(e))
                continue  # Skip to the next race record
    
        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        conn.rollback()  # Rollback transaction if an error occurred
        return False