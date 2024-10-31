import xml.etree.ElementTree as ET
import logging
from ingestion_utils import validate_xml, get_text, safe_float, parse_date, safe_int, gen_race_identifier , log_rejected_record
from datetime import datetime

def process_ppData_file(xml_file, xsd_schema_path, conn, cursor):
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
            course_cd = race.find('track').text 
            race_date = parse_date(get_text(race.find('race_date')))
            race_number = safe_int(get_text(race.find('race')))
            for horse in race.findall('horsedata'):
                try:
                    # Extract horse-related data
                    # Directly retrieve `program_number` to handle alphanumeric cases
                    program_number = get_text(horse.find('program'))
                    if program_number is None:
                        logging.warning(f"Skipping horse with missing program_number in race {race_number}")
                        continue  # Skip to next horse if `program_number` is missing
                    race_identifier = gen_race_identifier(course_cd,race_date, race_number)
                    # Find the ppdata section
                    ppdata_data = horse.find('ppdata')
                    if ppdata_data is not None:
                        # Extract ppdata information
                        pp_race_date = parse_date(get_text(ppdata_data.find('racedate')))
                        pp_trackcode = get_text(ppdata_data.find('trackcode'))
                        pp_racenumber = safe_int(get_text(ppdata_data.find('racenumber')))
                        pp_race_identifier = gen_race_identifier(pp_trackcode, pp_race_date, pp_racenumber)
                        racetype = get_text(ppdata_data.find('racetype'))
                        raceclass = get_text(ppdata_data.find('raceclass'))
                        claimprice = safe_int(get_text(ppdata_data.find('claimprice')))
                        purse = safe_int(get_text(ppdata_data.find('purse')))
                        classratin = get_text(ppdata_data.find('classratin'))
                        trackcondi = get_text(ppdata_data.find('trackcondi'))
                        distance = safe_int(get_text(ppdata_data.find('distance')))
                        disttype = get_text(ppdata_data.find('disttype'))
                        about_dist_indicator = get_text(ppdata_data.find('aboudist'))
                        courseid = get_text(ppdata_data.find('courseid'))
                        surface = get_text(ppdata_data.find('surface'))
                        pulledofft = safe_int(get_text(ppdata_data.find('pulledofft')))
                        winddirect = get_text(ppdata_data.find('winddirect'))
                        windspeed = safe_int(get_text(ppdata_data.find('windspeed')))
                        trackvaria = safe_int(get_text(ppdata_data.find('trackvaria')))
                        sealedtrac = get_text(ppdata_data.find('sealedtrac'))
                        racegrade = safe_int(get_text(ppdata_data.find('racegrade')))
                        age_restr_cd = get_text(ppdata_data.find('agerestric'))
                        sexrestric = get_text(ppdata_data.find('sexrestric'))
                        statebredr = get_text(ppdata_data.find('statebredr'))
                        abbrev_conditions = get_text(ppdata_data.find('abbrevcond'))
                        postpositi = safe_int(get_text(ppdata_data.find('postpositi')))
                        favorite = safe_int(get_text(ppdata_data.find('favorite')))
                        weightcarr = safe_int(get_text(ppdata_data.find('weightcarr')))
                        jockfirst = get_text(ppdata_data.find('jockfirst'))
                        jockmiddle = get_text(ppdata_data.find('jockmiddle'))
                        jocklast = get_text(ppdata_data.find('jocklast'))
                        jocksuffix = get_text(ppdata_data.find('jocksuffix'))
                        jockdisp = get_text(ppdata_data.find('jockdisp'))
                        equipment = get_text(ppdata_data.find('equipment'))
                        medication = get_text(ppdata_data.find('medication'))
                        fieldsize = safe_int(get_text(ppdata_data.find('fieldsize')))
                        posttimeod = safe_float(get_text(ppdata_data.find('posttimeod')))
                        shortcomme = get_text(ppdata_data.find('shortcomme'))
                        longcommen = get_text(ppdata_data.find('longcommen'))
                        gatebreak = safe_int(get_text(ppdata_data.find('gatebreak')))
                        position1 = safe_int(get_text(ppdata_data.find('position1')))
                        lenback1 = safe_int(get_text(ppdata_data.find('lenback1')))
                        horsetime1 = safe_float(get_text(ppdata_data.find('horsetime1')))
                        leadertime = safe_float(get_text(ppdata_data.find('leadertime')))
                        pacefigure = safe_int(get_text(ppdata_data.find('pacefigure')))
                        position2 = safe_int(get_text(ppdata_data.find('position2')))
                        lenback2 = safe_int(get_text(ppdata_data.find('lenback2')))
                        horsetime2 = safe_float(get_text(ppdata_data.find('horsetime2')))
                        leadertim2 = safe_float(get_text(ppdata_data.find('leadertim2')))
                        pacefigur2 = safe_int(get_text(ppdata_data.find('pacefigur2')))
                        positionst = safe_int(get_text(ppdata_data.find('positionst')))
                        lenbackstr = safe_int(get_text(ppdata_data.find('lenbackstr')))
                        horsetimes = safe_float(get_text(ppdata_data.find('horsetimes')))
                        leadertim3 = safe_float(get_text(ppdata_data.find('leadertim3')))
                        dqindicato = get_text(ppdata_data.find('dqindicato'))
                        positionfi = safe_int(get_text(ppdata_data.find('positionfi')))
                        lenbackfin = safe_int(get_text(ppdata_data.find('lenbackfin')))
                        horsetimef = safe_float(get_text(ppdata_data.find('horsetimef')))
                        leadertim4 = safe_float(get_text(ppdata_data.find('leadertim4')))
                        speedfigur = safe_int(get_text(ppdata_data.find('speedfigur')))
                        turffigure = safe_float(get_text(ppdata_data.find('turffigure')))
                        winnersspe = safe_int(get_text(ppdata_data.find('winnersspe')))
                        foreignspe = safe_int(get_text(ppdata_data.find('foreignspe')))
                        horseclaim = safe_int(get_text(ppdata_data.find('horseclaim')))
                        biasstyle = get_text(ppdata_data.find('biasstyle'))
                        track_bias_indicator = get_text(ppdata_data.find('biaspath'))
                        complineho = get_text(ppdata_data.find('complineho'))
                        complinele = safe_int(get_text(ppdata_data.find('complinele')))
                        complinewe = safe_int(get_text(ppdata_data.find('complinewe')))
                        complinedq = get_text(ppdata_data.find('complinedq'))
                        complineh2 = get_text(ppdata_data.find('complineh2'))
                        complinel2 = safe_int(get_text(ppdata_data.find('complinel2')))
                        complinew2 = safe_int(get_text(ppdata_data.find('complinew2')))
                        complined2 = get_text(ppdata_data.find('complined2'))
                        complineh3 = get_text(ppdata_data.find('complineh3'))
                        complinel3 = safe_int(get_text(ppdata_data.find('complinel3')))
                        complinew3 = safe_int(get_text(ppdata_data.find('complinew3')))
                        complined3 = get_text(ppdata_data.find('complined3'))
                        linebefore = get_text(ppdata_data.find('linebefore'))
                        lineafter = get_text(ppdata_data.find('lineafter'))
                        domesticpp = safe_int(get_text(ppdata_data.find('domesticpp')))
                        oflfinish = safe_int(get_text(ppdata_data.find('oflfinish')))
                        runup_dist = safe_int(get_text(ppdata_data.find('runup_dist')))
                        rail_dist = safe_int(get_text(ppdata_data.find('rail_dist')))
                        apprentice_wght = safe_int(get_text(ppdata_data.find('apprweight')))
                        vd_claim = get_text(ppdata_data.find('vd_claim'))
                        vd_reason = get_text(ppdata_data.find('vd_reason'))
                        
                        insert_ppdata_query = """
                        INSERT INTO public.ppdata (
                            race_identifier, program_number, pp_race_identifier, racetype, raceclass, 
                            claimprice,purse, classratin, trackcondi, distance, 
                            disttype, about_dist_indicator, courseid,surface, pulledofft, 
                            winddirect, windspeed, trackvaria, sealedtrac, racegrade, 
                            age_restr_cd, sexrestric, statebredr, abbrev_conditions, postpositi, 
                            favorite, weightcarr, jockfirst, jockmiddle, jocklast, 
                            jocksuffix, jockdisp, equipment, medication, fieldsize, 
                            posttimeod, shortcomme, longcommen, gatebreak, position1, 
                            lenback1, horsetime1, leadertime, pacefigure, position2, 
                            lenback2, horsetime2, leadertim2, pacefigur2, positionst, 
                            lenbackstr, horsetimes, leadertim3, dqindicato, positionfi, 
                            lenbackfin, horsetimef, leadertim4, speedfigur, turffigure, 
                            winnersspe,foreignspe, horseclaim, biasstyle, track_bias_indicator, 
                            complineho, complinele,complinewe, complinedq, complineh2, 
                            complinel2, complinew2, complined2, complineh3, complinel3, 
                            complinew3, complined3, linebefore, lineafter, domesticpp, 
                            oflfinish, runup_dist, rail_dist, apprentice_wght, vd_claim, 
                            vd_reason
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (race_identifier, program_number, pp_race_identifier) DO UPDATE 
                        SET 
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
                            vd_reason = EXCLUDED.vd_reason
                        """

                        # Execute the query for ppData
                        cursor.execute(insert_ppdata_query, (
                            race_identifier, program_number, pp_race_identifier, racetype, raceclass, 
                            claimprice,purse, classratin, trackcondi, distance, 
                            disttype, about_dist_indicator, courseid,surface, pulledofft, 
                            winddirect, windspeed, trackvaria, sealedtrac, racegrade, 
                            age_restr_cd, sexrestric, statebredr, abbrev_conditions, postpositi, 
                            favorite, weightcarr, jockfirst, jockmiddle, jocklast, 
                            jocksuffix, jockdisp, equipment, medication, fieldsize, 
                            posttimeod, shortcomme, longcommen, gatebreak, position1, 
                            lenback1, horsetime1, leadertime, pacefigure, position2, 
                            lenback2, horsetime2, leadertim2, pacefigur2, positionst, 
                            lenbackstr, horsetimes, leadertim3, dqindicato, positionfi, 
                            lenbackfin, horsetimef, leadertim4, speedfigur, turffigure, 
                            winnersspe,foreignspe, horseclaim, biasstyle, track_bias_indicator, 
                            complineho, complinele,complinewe, complinedq, complineh2, 
                            complinel2, complinew2, complined2, complineh3, complinel3, 
                            complinew3, complined3, linebefore, lineafter, domesticpp, 
                            oflfinish, runup_dist, rail_dist, apprentice_wght, vd_claim, 
                            vd_reason
                        ))                
                except Exception as horse_error:
                    logging.error(f"Error processing race: {race_number}, error: {horse_error}")
                    rejected_record = {
                        "race_identifier": race_identifier,
                        "program_number": program_number,
                        "pp_race_identifier": pp_race_identifier,
                        "racetype": racetype,
                        "raceclass": raceclass,
                        "claimprice": claimprice,
                        "purse": purse,
                        "classratin": classratin,
                        "trackcondi": trackcondi,
                        "distance": distance,
                        "disttype": disttype,
                        "about_dist_indicator": about_dist_indicator,
                        "courseid": courseid,
                        "surface": surface,
                        "pulledofft": pulledofft,
                        "winddirect": winddirect,
                        "windspeed": windspeed,
                        "trackvaria": trackvaria,
                        "sealedtrac": sealedtrac,
                        "racegrade": racegrade,
                        "age_restr_cd": age_restr_cd,
                        "sexrestric": sexrestric,
                        "statebredr": statebredr,
                        "abbrev_conditions": abbrev_conditions,
                        "postpositi": postpositi,
                        "favorite": favorite,
                        "weightcarr": weightcarr,
                        "jockfirst": jockfirst,
                        "jockmiddle": jockmiddle,
                        "jocklast": jocklast,
                        "jocksuffix": jocksuffix,
                        "jockdisp": jockdisp,
                        "equipment": equipment,
                        "medication": medication,
                        "fieldsize": fieldsize,
                        "posttimeod": posttimeod,
                        "shortcomme": shortcomme,
                        "longcommen": longcommen,
                        "gatebreak": gatebreak,
                        "position1": position1,
                        "lenback1": lenback1,
                        "horsetime1": horsetime1,
                        "leadertime": leadertime,
                        "pacefigure": pacefigure,
                        "position2": position2,
                        "lenback2": lenback2,
                        "horsetime2": horsetime2,
                        "leadertim2": leadertim2,
                        "pacefigur2": pacefigur2,
                        "positionst": positionst,
                        "lenbackstr": lenbackstr,
                        "horsetimes": horsetimes,
                        "leadertim3": leadertim3,
                        "dqindicato": dqindicato,
                        "positionfi": positionfi,
                        "lenbackfin": lenbackfin,
                        "horsetimef": horsetimef,
                        "leadertim4": leadertim4,
                        "speedfigur": speedfigur,
                        "turffigure": turffigure,
                        "winnersspe": winnersspe,
                        "foreignspe": foreignspe,
                        "horseclaim": horseclaim,
                        "biasstyle": biasstyle,
                        "track_bias_indicator": track_bias_indicator,
                        "complineho": complineho,
                        "complinele": complinele,
                        "complinewe": complinewe,
                        "complinedq": complinedq,
                        "complineh2": complineh2,
                        "complinel2": complinel2,
                        "complinew2": complinew2,
                        "complined2": complined2,
                        "complineh3": complineh3,
                        "complinel3": complinel3,
                        "complinew3": complinew3,
                        "complined3": complined3,
                        "linebefore": linebefore,
                        "lineafter": lineafter,
                        "domesticpp": domesticpp,
                        "oflfinish": oflfinish,
                        "runup_dist": runup_dist,
                        "rail_dist": rail_dist,
                        "apprentice_wght": apprentice_wght,
                        "vd_claim": vd_claim,
                        "vd_reason": vd_reason
                    }
                    log_rejected_record('ppdata', rejected_record)
                    continue  # Skip to the next race after logging the error

        # Commit the transaction after all runner data has been processed
        conn.commit()

    except Exception as e:
        logging.error(f"Error processing runner data in file {xml_file}: {e}")
        conn.rollback()  # Rollback the transaction in case of an error