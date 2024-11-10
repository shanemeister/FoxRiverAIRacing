import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text, safe_float, parse_date, safe_int, gen_race_identifier, log_rejected_record, update_ingestion_status, parse_time
from datetime import datetime
from mappings_dictionaries import eqb_tpd_codes_to_course_cd

def process_ppData_file(xml_file, xsd_schema_path, conn, cursor):
    """
    Process individual XML race data file and insert into the ppdata table.
    Validates the XML against the provided XSD schema and updates ingestion status.
    """

    # Validate the XML file first
    if not validate_xml(xml_file, xsd_schema_path):
        logging.error(f"XML validation failed for file {xml_file}. Skipping processing.")
        update_ingestion_status(conn, cursor, xml_file, "error")  # Record error status
        return "error"

    has_rejections = False  # Track if any records were rejected
    rejected_record = {}  # Store rejected records for logging
    
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Iterate over each race data
        for race in root.findall('racedata'):
            course_cd = eqb_tpd_codes_to_course_cd.get(get_text(race.find('track'), 'EQE'))  # Default 'Unknown' for course_cd if missing
            race_date = parse_date(get_text(race.find('race_date'), '1970-01-01'))  # Default to a placeholder date
            race_number = safe_int(get_text(race.find('race'), '0'))  # Default to '0' if race_number is missing
            post_time = parse_time(get_text(race.find('post_time'))) or datetime(1970, 1, 1).time()
            for horse in race.findall('horsedata'):
                try:
                    saddle_cloth_number = get_text(horse.find('program'), '0')  # Default to '0' if program_number is missing

                    # If program_number is essential and missing, skip this horse
                    if saddle_cloth_number == '0':
                        logging.warning(f"Skipping horse with missing program_number in race {race_number}")
                        continue

                    # Initialize all ppdata fields with defaults
                    ppdata_data = horse.find('ppdata')
                    if ppdata_data is None:
                        logging.warning(f"No 'ppdata' found for horse in race {race_number}. Skipping.")
                        continue  # Skip processing for this horse
                    pp_course_cd = eqb_tpd_codes_to_course_cd.get(get_text(ppdata_data.find('trackcode'), 'EQE')) 
                    pp_saddle_cloth_number = safe_int(get_text(ppdata_data.find('postpositi'), '0'))
                    pp_race_date = parse_date(get_text(ppdata_data.find('racedate'), '1970-01-01'))
                    pp_race_number = safe_int(get_text(ppdata_data.find('racenumber'), '0'))
                    racetype = get_text(ppdata_data.find('racetype'), 'NA')
                    raceclass = get_text(ppdata_data.find('raceclass'), 'NA')
                    claimprice = safe_int(get_text(ppdata_data.find('claimprice'), '0'))
                    purse = safe_int(get_text(ppdata_data.find('purse'), '0'))
                    classratin = get_text(ppdata_data.find('classratin'), 'NA')
                    trackcondi = get_text(ppdata_data.find('trackcondi'), 'NA')
                    distance = safe_int(get_text(ppdata_data.find('distance'), '0'))
                    disttype = get_text(ppdata_data.find('disttype'), 'NA')
                    about_dist_indicator = get_text(ppdata_data.find('aboudist'), 'N')
                    courseid = get_text(ppdata_data.find('courseid'), 'NA')
                    surface = get_text(ppdata_data.find('surface'), 'NA')
                    pulledofft = safe_int(get_text(ppdata_data.find('pulledofft'), '0'))
                    winddirect = get_text(ppdata_data.find('winddirect'), 'N')
                    windspeed = safe_int(get_text(ppdata_data.find('windspeed'), '0'))
                    trackvaria = safe_int(get_text(ppdata_data.find('trackvaria'), '0'))
                    sealedtrac = get_text(ppdata_data.find('sealedtrac'), 'N')
                    racegrade = safe_int(get_text(ppdata_data.find('racegrade'), '0'))
                    age_restr_cd = get_text(ppdata_data.find('agerestric'), 'NA')
                    sexrestric = get_text(ppdata_data.find('sexrestric'), 'N')
                    statebredr = get_text(ppdata_data.find('statebredr'), 'N')
                    abbrev_conditions = get_text(ppdata_data.find('abbrevcond'), 'N')
                    postpositi = safe_int(get_text(ppdata_data.find('postpositi'), '0'))
                    favorite = safe_int(get_text(ppdata_data.find('favorite'), '0'))
                    weightcarr = safe_int(get_text(ppdata_data.find('weightcarr'), '0'))
                    jockfirst = get_text(ppdata_data.find('jockfirst'), 'NA')
                    jockmiddle = get_text(ppdata_data.find('jockmiddle'), 'NA')
                    jocklast = get_text(ppdata_data.find('jocklast'), 'NA')
                    jocksuffix = get_text(ppdata_data.find('jocksuffix'), 'NA')
                    jockdisp = get_text(ppdata_data.find('jockdisp'), 'NA')
                    equipment = get_text(ppdata_data.find('equipment'), 'N')
                    medication = get_text(ppdata_data.find('medication'), 'N')
                    fieldsize = safe_int(get_text(ppdata_data.find('fieldsize'), '0'))
                    posttimeod = safe_float(get_text(ppdata_data.find('posttimeod'), '0.0'))
                    shortcomme = get_text(ppdata_data.find('shortcomme'), 'NA')
                    longcommen = get_text(ppdata_data.find('longcommen'), 'NA')
                    gatebreak = safe_int(get_text(ppdata_data.find('gatebreak'), '0'))
                    position1 = safe_int(get_text(ppdata_data.find('position1'), '0'))
                    lenback1 = safe_int(get_text(ppdata_data.find('lenback1'), '0'))
                    horsetime1 = safe_float(get_text(ppdata_data.find('horsetime1'), '0.0'))
                    leadertime = safe_float(get_text(ppdata_data.find('leadertime'), '0.0'))
                    pacefigure = safe_int(get_text(ppdata_data.find('pacefigure'), '0'))
                    position2 = safe_int(get_text(ppdata_data.find('position2'), '0'))
                    lenback2 = safe_float(get_text(ppdata_data.find('lenback2'), '0.0'))
                    horsetime2 = safe_float(get_text(ppdata_data.find('horsetime2'), '0.0'))
                    leadertim2 = safe_float(get_text(ppdata_data.find('leadertim2'), '0.0'))
                    pacefigur2 = safe_int(get_text(ppdata_data.find('pacefigur2'), '0'))
                    positionst = safe_int(get_text(ppdata_data.find('positionst'), '0'))
                    lenbackstr = safe_float(get_text(ppdata_data.find('lenbackstr'), '0.0'))
                    horsetimes = safe_float(get_text(ppdata_data.find('horsetimes'), '0.0'))
                    leadertim3 = safe_float(get_text(ppdata_data.find('leadertim3'), '0.0'))
                    dqindicato = get_text(ppdata_data.find('dqindicato'), 'N')
                    positionfi = safe_int(get_text(ppdata_data.find('positionfi'), '0'))
                    lenbackfin = safe_float(get_text(ppdata_data.find('lenbackfin'), '0.0'))
                    horsetimef = safe_float(get_text(ppdata_data.find('horsetimef'), '0.0'))
                    leadertim4 = safe_float(get_text(ppdata_data.find('leadertim4'), '0.0'))
                    speedfigur = safe_int(get_text(ppdata_data.find('speedfigur'), '0'))
                    turffigure = safe_float(get_text(ppdata_data.find('turffigure'), '0.0'))
                    winnersspe = safe_int(get_text(ppdata_data.find('winnersspe'), '0'))
                    foreignspe = safe_int(get_text(ppdata_data.find('foreignspe'), '0'))
                    horseclaim = safe_int(get_text(ppdata_data.find('horseclaim'), '0'))
                    biasstyle = get_text(ppdata_data.find('biasstyle'), 'N')
                    track_bias_indicator = get_text(ppdata_data.find('biaspath'), 'N')
                    complineho = get_text(ppdata_data.find('complineho'), 'NA')
                    complinele = safe_float(get_text(ppdata_data.find('complinele'), '0.0'))
                    complinewe = safe_int(get_text(ppdata_data.find('complinewe'), '0'))
                    complinedq = get_text(ppdata_data.find('complinedq'), 'N')
                    complineh2 = get_text(ppdata_data.find('complineh2'), 'NA')
                    complinel2 = safe_float(get_text(ppdata_data.find('complinel2'), '0.0'))
                    complinew2 = safe_int(get_text(ppdata_data.find('complinew2'), '0'))
                    complined2 = get_text(ppdata_data.find('complined2'), 'N')
                    complineh3 = get_text(ppdata_data.find('complineh3'), 'NA')
                    complinel3 = safe_float(get_text(ppdata_data.find('complinel3'), '0.0'))
                    complinew3 = safe_int(get_text(ppdata_data.find('complinew3'), '0'))
                    complined3 = get_text(ppdata_data.find('complined3'), 'N')
                    linebefore = get_text(ppdata_data.find('linebefore'), 'NA')
                    lineafter = get_text(ppdata_data.find('lineafter'), 'NA')
                    domesticpp = safe_int(get_text(ppdata_data.find('domesticpp'), '0'))
                    oflfinish = safe_int(get_text(ppdata_data.find('oflfinish'), '0'))
                    runup_dist = safe_int(get_text(ppdata_data.find('runup_dist'), '0'))
                    rail_dist = safe_int(get_text(ppdata_data.find('rail_dist'), '0'))
                    apprentice_wght = safe_int(get_text(ppdata_data.find('apprweight'), '0'))
                    vd_claim = get_text(ppdata_data.find('vd_claim'), 'N')
                    vd_reason = get_text(ppdata_data.find('vd_reason'), 'NA')                        
                
                    # SQL insert query for the racedata table
                    insert_query = """
                        INSERT INTO public.ppdata (
                            course_cd, race_date, post_time, race_number, saddle_cloth_number,  
                            pp_saddle_cloth_number, pp_race_date, pp_race_number, racetype, raceclass, 
                            claimprice, purse, classratin, trackcondi, distance, 
                            disttype, about_dist_indicator, courseid, surface, pulledofft, 
                            winddirect, windspeed, trackvaria, sealedtrac, racegrade,
                            age_restr_cd, sexrestric, statebredr, abbrev_conditions, postpositi, 
                            favorite, weightcarr, jockfirst, jockmiddle, jocklast, 
                            jocksuffix, jockdisp, equipment, medication, fieldsize, 
                            posttimeod, shortcomme, longcommen, gatebreak, position1, 
                            lenback1, horsetime1, leadertime, pacefigure, position2, 
                            lenback2, horsetime2, leadertim2, pacefigur2, positionst, 
                            lenbackstr, horsetimes, leadertim3, dqindicato, positionfi, 
                            lenbackfin, horsetimef, leadertim4, speedfigur, turffigure, 
                            winnersspe, foreignspe, horseclaim, biasstyle, track_bias_indicator, 
                            complineho, complinele, complinewe, complinedq, complineh2, 
                            complinel2, complinew2, complined2, complineh3, complinel3, 
                            complinew3, complined3, linebefore, lineafter, domesticpp, 
                            oflfinish, runup_dist, rail_dist, apprentice_wght, vd_claim, 
                            vd_reason, pp_course_cd
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s
                        )
                        ON CONFLICT (course_cd, race_date, post_time, race_number, saddle_cloth_number) DO UPDATE 
                        SET pp_course_cd = EXCLUDED.pp_course_cd,
                            pp_saddle_cloth_number = EXCLUDED.pp_saddle_cloth_number,
                            pp_race_date = EXCLUDED.pp_race_date,
                            pp_race_number = EXCLUDED.pp_race_number,
                            racetype = EXCLUDED.racetype,
                            
                            raceclass = EXCLUDED.raceclass,
                            claimprice = EXCLUDED.claimprice,
                            purse = EXCLUDED.purse,
                            classratin = EXCLUDED.classratin,
                            trackcondi = EXCLUDED.trackcondi,
                            
                            distance = EXCLUDED.distance,
                            disttype = EXCLUDED.disttype,
                            about_dist_indicator = EXCLUDED.about_dist_indicator,
                            courseid = EXCLUDED.courseid,
                            surface = EXCLUDED.surface,
                            
                            pulledofft = EXCLUDED.pulledofft,
                            winddirect = EXCLUDED.winddirect,
                            windspeed = EXCLUDED.windspeed,
                            trackvaria = EXCLUDED.trackvaria,
                            sealedtrac = EXCLUDED.sealedtrac,
                            
                            racegrade = EXCLUDED.racegrade,
                            age_restr_cd = EXCLUDED.age_restr_cd,
                            sexrestric = EXCLUDED.sexrestric,
                            statebredr = EXCLUDED.statebredr,
                            abbrev_conditions = EXCLUDED.abbrev_conditions,
                            
                            postpositi = EXCLUDED.postpositi,
                            favorite = EXCLUDED.favorite,
                            weightcarr = EXCLUDED.weightcarr,
                            jockfirst = EXCLUDED.jockfirst,
                            jockmiddle = EXCLUDED.jockmiddle,
                            
                            jocklast = EXCLUDED.jocklast,
                            jocksuffix = EXCLUDED.jocksuffix,
                            jockdisp = EXCLUDED.jockdisp,
                            equipment = EXCLUDED.equipment,
                            medication = EXCLUDED.medication,
                            
                            fieldsize = EXCLUDED.fieldsize,
                            posttimeod = EXCLUDED.posttimeod,
                            shortcomme = EXCLUDED.shortcomme,
                            longcommen = EXCLUDED.longcommen,
                            gatebreak = EXCLUDED.gatebreak,
                            
                            position1 = EXCLUDED.position1,
                            lenback1 = EXCLUDED.lenback1,
                            horsetime1 = EXCLUDED.horsetime1,
                            leadertime = EXCLUDED.leadertime,
                            pacefigure = EXCLUDED.pacefigure,
                            
                            position2 = EXCLUDED.position2,
                            lenback2 = EXCLUDED.lenback2,
                            horsetime2 = EXCLUDED.horsetime2,
                            leadertim2 = EXCLUDED.leadertim2,
                            pacefigur2 = EXCLUDED.pacefigur2,
                            
                            positionst = EXCLUDED.positionst,
                            lenbackstr = EXCLUDED.lenbackstr,
                            horsetimes = EXCLUDED.horsetimes,
                            leadertim3 = EXCLUDED.leadertim3,
                            dqindicato = EXCLUDED.dqindicato,
                            
                            positionfi = EXCLUDED.positionfi,
                            lenbackfin = EXCLUDED.lenbackfin,
                            horsetimef = EXCLUDED.horsetimef,
                            leadertim4 = EXCLUDED.leadertim4,
                            speedfigur = EXCLUDED.speedfigur,
                            
                            turffigure = EXCLUDED.turffigure,
                            winnersspe = EXCLUDED.winnersspe,
                            foreignspe = EXCLUDED.foreignspe,
                            horseclaim = EXCLUDED.horseclaim,
                            biasstyle = EXCLUDED.biasstyle,
                            
                            track_bias_indicator = EXCLUDED.track_bias_indicator,
                            complineho = EXCLUDED.complineho,
                            complinele = EXCLUDED.complinele,
                            complinewe = EXCLUDED.complinewe,
                            complinedq = EXCLUDED.complinedq,
                            
                            complineh2 = EXCLUDED.complineh2,
                            complinel2 = EXCLUDED.complinel2,
                            complinew2 = EXCLUDED.complinew2,
                            complined2 = EXCLUDED.complined2,
                            complineh3 = EXCLUDED.complineh3,
                            
                            complinel3 = EXCLUDED.complinel3,
                            complinew3 = EXCLUDED.complinew3,
                            complined3 = EXCLUDED.complined3,
                            linebefore = EXCLUDED.linebefore,
                            lineafter = EXCLUDED.lineafter,
                            
                            domesticpp = EXCLUDED.domesticpp,
                            oflfinish = EXCLUDED.oflfinish,
                            runup_dist = EXCLUDED.runup_dist,
                            rail_dist = EXCLUDED.rail_dist,
                            apprentice_wght = EXCLUDED.apprentice_wght,
                            
                            vd_claim = EXCLUDED.vd_claim,
                            vd_reason = EXCLUDED.vd_reason;
                            """
                    try:
                            
                        cursor.execute(insert_query, (
                                course_cd, race_date, post_time, race_number, saddle_cloth_number,  
                                pp_saddle_cloth_number, pp_race_date, pp_race_number, racetype, raceclass, 
                                claimprice, purse, classratin, trackcondi, distance, 
                                disttype, about_dist_indicator, courseid, surface, pulledofft, 
                                winddirect, windspeed, trackvaria, sealedtrac, racegrade,
                                age_restr_cd, sexrestric, statebredr, abbrev_conditions, postpositi, 
                                favorite, weightcarr, jockfirst, jockmiddle, jocklast, 
                                jocksuffix, jockdisp, equipment, medication, fieldsize, 
                                posttimeod, shortcomme, longcommen, gatebreak, position1, 
                                lenback1, horsetime1, leadertime, pacefigure, position2, 
                                lenback2, horsetime2, leadertim2, pacefigur2, positionst, 
                                lenbackstr, horsetimes, leadertim3, dqindicato, positionfi, 
                                lenbackfin, horsetimef, leadertim4, speedfigur, turffigure, 
                                winnersspe, foreignspe, horseclaim, biasstyle, track_bias_indicator, 
                                complineho, complinele, complinewe, complinedq, complineh2, 
                                complinel2, complinew2, complined2, complineh3, complinel3, 
                                complinew3, complined3, linebefore, lineafter, domesticpp, 
                                oflfinish, runup_dist, rail_dist, apprentice_wght, vd_claim, 
                                vd_reason, pp_course_cd,
                        ))
    
                    except Exception as ppData_error:
                        has_rejections = True
                        logging.error(f"Error processing race: {ppdata_data}, error: {ppData_error}")
                        rejected_record = {
                            "course_cd": course_cd,
                            "race_date": race_date,
                            "post_time": post_time,
                            "race_number": race_number,
                            "saddle_cloth_number": saddle_cloth_number,
                        #5 
                            "pp_course_cd": pp_course_cd,
                            "pp_saddle_cloth_number": pp_saddle_cloth_number,
                            "pp_race_date": pp_race_date,
                            "pp_race_number": pp_race_number,
                            "racetype": racetype,
                            #10
                            "raceclass": raceclass,
                            "claimprice": claimprice,
                            "purse": purse,
                            "classratin": classratin,
                            "trackcondi": trackcondi,
                            #20
                            "distance": distance,
                            "disttype": disttype,
                            "about_dist_indicator": about_dist_indicator,
                            "courseid": courseid,
                            "surface": surface,
                            #25
                            "pulledofft": pulledofft,
                            "winddirect": winddirect,
                            "windspeed": windspeed,
                            "trackvaria": trackvaria,
                            "sealedtrac": sealedtrac,
                            #30
                            "racegrade": racegrade,
                            "age_restr_cd": age_restr_cd,
                            "sexrestric": sexrestric,
                            "statebredr": statebredr,
                            "abbrev_conditions": abbrev_conditions,
                            #35
                            "postpositi": postpositi,
                            "favorite": favorite,
                            "weightcarr": weightcarr,
                            "jockfirst": jockfirst,
                            "jockmiddle": jockmiddle,
                            #40
                            "jocklast": jocklast,
                            "jocksuffix": jocksuffix,
                            "jockdisp": jockdisp,
                            "equipment": equipment,
                            "medication": medication,
                            #45
                            "fieldsize": fieldsize,
                            "posttimeod": posttimeod,
                            "apprentice_wght": apprentice_wght,
                            "shortcomme": shortcomme,
                            "longcommen": longcommen,
                            #50
                            "gatebreak": gatebreak,
                            "position1": position1,
                            "lenback1": lenback1,
                            "horsetime1": horsetime1,
                            "leadertime": leadertime,
                            #55
                            "pacefigure": pacefigure,
                            "position2": position2,
                            "lenback2": lenback2,
                            "horsetime2": horsetime2,
                            "leadertim2": leadertim2,
                            #60
                            "pacefigur2": pacefigur2,
                            "positionst": positionst,
                            "lenbackstr": lenbackstr,
                            "horsetimes": horsetimes,
                            "leadertim3": leadertim3,
                            #65
                            "dqindicato": dqindicato,
                            "positionfi": positionfi,
                            "lenbackfin": lenbackfin,
                            "horsetimef": horsetimef,
                            "leadertim4": leadertim4,
                            #70
                            "speedfigur": speedfigur,
                            "turffigure": turffigure,
                            "winnersspe": winnersspe,
                            "foreignspe": foreignspe,
                            "horseclaim": horseclaim,
                            #75
                            "biasstyle": biasstyle,
                            "track_bias_indicator": track_bias_indicator,
                            "complineho": complineho,
                            "complinele": complinele,
                            "complinewe": complinewe,
                            #80
                            "complinedq": complinedq,
                            "complineh2": complineh2,
                            "complinel2": complinel2,
                            "complinew2": complinew2,
                            "complined2": complined2,
                            #85
                            "complineh3": complineh3,
                            "complinel3": complinel3,
                            "complinew3": complinew3,
                            "complined3": complined3,
                            "linebefore": linebefore,
                            #90
                            "lineafter": lineafter,
                            "domesticpp": domesticpp,
                            "oflfinish": oflfinish,
                            "runup_dist": runup_dist,
                            "rail_dist": rail_dist,
                                
                            "vd_claim": vd_claim,
                            "vd_reason": vd_reason
                        }
                        conn.rollback()  # Rollback the transaction before logging the rejected record
                        log_rejected_record(conn, 'ppData', rejected_record, str(ppData_error))
                        continue  # Skip to the next race record
                    
                except Exception as e:
                    has_rejections = True
                    conn.rollback()  # Rollback the transaction before logging the rejected record
                    log_rejected_record(conn, 'ppData', rejected_record, str(e))
                    continue  # Skip to the next race record
    
        return not has_rejections  # Returns True if no rejections, otherwise False

    except Exception as e:
        logging.error(f"Critical error processing horse data file {xml_file}: {e}")
        conn.rollback()  # Rollback transaction if an error occurred
        return False