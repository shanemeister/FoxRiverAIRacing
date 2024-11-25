# FoxRiverAIRacing
Repo of models

Creating Partitions 

DO $$
DECLARE
    course TEXT;
    batch_size INT := 50; -- Adjust based on system capacity
    counter INT := 0;
    course_list TEXT[] := ARRAY[
        'CNL', 'SAR', 'PIM', 'TSA', 'BEL', 'MVR', 'TWO', 'CLS', 'KEE', 'TAM', 'TTP', 'TKD', 
        'ELP', 'PEN', 'HOU', 'DMR', 'TLS', 'AQU', 'MTH', 'TGP', 'TGG', 'CBY', 'LRL', 
        'TED', 'IND', 'CTD', 'ASD', 'TCD', 'LAD', 'MED', 'TOP', 'HOO',
        'EQE','EQC','CCP','CMR','EQG','EQD','EQJ','EQF','EQB',
        'BCC','EQA','EQI','EQH','KAR','RAS','BCD','BCT','DUB','BFF','EQK',
        'EQZ','BCA','EQN','EQX','BCB','EQO','EQY','EQR','EQV','EQQ','BCG',
        'EQS','EQM','EQP','BCE','EQU','EQT','EQW','SWA','SWD','SWC','LSP',
        'LTD','LTH','TFG','ABT','GPR','LBG','CTM','MIL','TNP','STP','AZD',
        'TDG','DUN','RIL','SAF','SON','TUP','YAV','DEP','TEP','HST','KAM',
        'KIN','SAN','SND','BHP','BMF','BSR','FER','FNO','FPX','HOL','TLA',
        'LRC','OSA','OTH','OTP','PLN','SAC','SLR','SOL','TSR','STK','ARP',
        'DEL','WNT','CRC','GPW','HIA','LEV','OTC','PMB','ATH','GRA','PMT',
        'PRM','BKF','BOI','CAS','EMT','JRM','ONE','POD','RUP','SDY','TAP',
        'DUQ','FAN','HAW','SPT','ANF','EUR','WDS','LEX','RDM','SRM','SRR',
        'DED','EVD','TBM','GBF','SUF','RPD','BOW','FAI','GLN','TGN','MAR',
        'MON','SHW','TIM','DET','GLD','THP','MPM','NVD','PNL','TGF','KSP',
        'MAF','TMC','WMF','TYD','BRO','CHL','CLM','TMS','TFP','SOP','STN',
        'TRY','CPW','FAR','ATO','FON','FPL','HPO','LEG','LNN','RKM','ATL',
        'TFH','TRB','ALB','LAM','RUI','SFE','SJD','SRP','SUN','ZIA','ELK',
        'ELY','HCF','WPR','BAQ','TFL','TGV','TGD','BEU','BTP','TRD','TDN',
        'BRD','FMT','WRD','AJX','PIC','BRN','GRP','PRV','TIL','TUN','CHE',
        'MAL','PHA','PID','PRX','UNI','WIL','AIK','CAM','CHA','BCF','FTP',
        'TMD','MDA','MMD','TPW','GIL','MAN','RET','DXD','LBT','WBR','FAX',
        'TFX','GRM','ING','MID','MOR','MTP','ODH','OKR','TSH','DAY','EMD',
        'HAP','SUD','WTS','TCT','MNR','SHD','CWF','EDR','SWF','WYO','TFE',
        'TRP','TPM','TWW','TYM'
    ];
    sql_stmt TEXT;
BEGIN
    -- Create Default Partitions
    RAISE NOTICE 'Creating default partitions...';
    
    EXECUTE '
        CREATE TABLE IF NOT EXISTS gpspoint_default PARTITION OF gpspoint
        DEFAULT;
    ';
    
    EXECUTE '
        CREATE TABLE IF NOT EXISTS sectionals_default PARTITION OF sectionals
        DEFAULT;
    ';
    
    FOREACH course IN ARRAY course_list LOOP
        -- Create gpspoint partition
        sql_stmt := format('
            CREATE TABLE IF NOT EXISTS gpspoint_%s PARTITION OF gpspoint
            FOR VALUES IN (%L);
        ', course, course);
        
        -- Output the SQL statement for debugging
        RAISE NOTICE 'Executing SQL for gpspoint: %', sql_stmt;
        
        -- Execute the SQL statement
        EXECUTE sql_stmt;
        
        -- Create sectionals partition
        sql_stmt := format('
            CREATE TABLE IF NOT EXISTS sectionals_%s PARTITION OF sectionals
            FOR VALUES IN (%L);
        ', course, course);
        
        -- Output the SQL statement for debugging
        RAISE NOTICE 'Executing SQL for sectionals: %', sql_stmt;
        
        -- Execute the SQL statement
        EXECUTE sql_stmt;
        
        counter := counter + 1;
        
        -- Optional: Provide progress feedback
        IF counter % batch_size = 0 THEN
            RAISE NOTICE 'Created % partitions so far', counter;
        END IF;
    END LOOP;
END $$;