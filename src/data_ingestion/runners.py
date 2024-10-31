import xml.etree.ElementTree as ET
import logging
from ingestion_utils import (validate_xml, get_text, safe_float, parse_date, clean_text, 
                            odds_to_probability, gen_race_identifier, log_rejected_record, safe_int)

from datetime import datetime

def process_runners_file(xml_file, xsd_file_path, conn, cursor):
    # Validate the XML file first
    if not validate_xml(xml_file, xsd_file_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        return

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for race in root.findall('racedata'):
            try:
                course_cd = race.find('track').text 
                race_date = parse_date(get_text(race.find('race_date')))
                race_number = safe_int(get_text(race.find('race')))
                race_identifier = gen_race_identifier(course_cd, race_date, race_number)
                country = race.find('country').text

                for horse in race.findall('horsedata'):
                    # Directly retrieve program_number as string to allow for alphanumeric values
                    program_number = get_text(horse.find('program'))
                    if program_number is None:
                        logging.warning(f"Missing program_number for horse in race {race_number} from file {xml_file}. Skipping runner data.")
                        continue

                    axciskey = get_text(horse.find('axciskey'))
                    if not axciskey:
                        logging.warning(f"Missing axciskey for a horse in file {xml_file}. Skipping runner data.")
                        continue

                    # Extract remaining fields
                    post_position = safe_int(get_text(horse.find('pp')))
                    todays_cls = safe_int(get_text(horse.find('todays_cls')))
                    owner_name = clean_text(get_text(horse.find('owner_name'), ''))
                    turf_mud_mark = get_text(horse.find('tmmark'), '')
                    avg_purse_val_calc = safe_float(get_text(horse.find('av_pur_val'), '0.0'))
                    weight = safe_float(get_text(horse.find('weight'), '0.0'))
                    wght_shift = safe_float(get_text(horse.find('wght_shift'), '0.0'))
                    cldate = parse_date(get_text(horse.find('cldate')), 'cldate')
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
                    lst_saleda = parse_date(get_text(horse.find('lst_saleda')), 'lst_saleda')
                    claimprice = safe_float(get_text(horse.find('claimprice'), '0.0'))
                    avgspd = safe_float(get_text(horse.find('avgspd'), '0.0'))
                    avgcls = safe_float(get_text(horse.find('avgcl'), '0.0'))
                    apprweight = safe_float(get_text(horse.find('apprweight'), '0.0'))
                    jock_key = get_text(horse.find('jockey/jock_key'))
                    train_key = get_text(horse.find('trainer/train_key'))

                    # Execute insert query for runners
                    cursor.execute("""
                        INSERT INTO public.runners (
                            race_identifier, program_number, course_cd, race_date, race_number, 
                            country, axciskey, post_position, todays_cls, owner_name,
                            turf_mud_mark, avg_purse_val_calc, weight, wght_shift, cldate,
                            price, bought_fr, power, med, equip,
                            morn_odds, breeder, ae_flag, power_symb, horse_comm,
                            breed_type, lst_salena, lst_salepr, lst_saleda, claimprice,
                            avgspd, avgcls, apprweight, jock_key, train_key     
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (race_identifier, program_number) DO UPDATE 
                        SET course_cd = EXCLUDED.course_cd,
                            race_date = EXCLUDED.race_date,
                            race_number = EXCLUDED.race_number,
                            country = EXCLUDED.country,
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
                    """, (
                        race_identifier, program_number, course_cd, race_date, race_number, 
                        country, axciskey, post_position, todays_cls, owner_name,
                        turf_mud_mark, avg_purse_val_calc, weight, wght_shift, cldate,
                        price, bought_fr, power, med, equip,
                        morn_odds, breeder, ae_flag, power_symb, horse_comm,
                        breed_type, lst_salena, lst_salepr, lst_saleda, claimprice,
                        avgspd, avgcls, apprweight, jock_key, train_key  
                    ))

            except Exception as race_error:
                logging.error(f"Error processing race: {race_number}, error: {race_error}")
                rejected_record = {
                    "race_identifier": race_identifier,
                    "program_number": program_number,
                    "course_cd": course_cd,
                    "race_date": race_date,
                    "race_number": race_number,
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
                log_rejected_record(conn, 'runners', rejected_record, str(race_error))
                continue

        conn.commit()

    except Exception as e:
        logging.error(f"Error processing runner data in file {xml_file}: {e}")
        conn.rollback()