from dataclasses import dataclass

def transfer_landing_raw_queries():
    
    transfer_soil_data = '''
        INSERT INTO raw.soil_data (field_id, soil_grid_data)
        SELECT l.field_id, l.soil_grid_data
        FROM landing.soil_data l
        LEFT JOIN raw.soil_data r
        ON l.field_id = r.field_id
        WHERE r.field_id IS NULL;
    '''

    transfer_meteo_data = '''
        INSERT INTO raw.meteo_data (meteo_landing_id, meteo_location_id, meteo_data)
        SELECT l.meteo_landing_id, l.meteo_location_id, l.meteo_data
        FROM landing.meteo_data l 
        LEFT JOIN raw.meteo_data r 
        ON l.meteo_landing_id = r.meteo_landing_id
        WHERE r.meteo_landing_id IS NULL;
    '''

    transfer_agro_meteo_data = '''
        INSERT INTO raw.agro_meteo_data (meteo_landing_id, meteo_location_id, agro_meteo_data)
        SELECT l.meteo_landing_id, l.meteo_location_id, l.agro_meteo_data
        FROM landing.agro_meteo_data l 
        LEFT JOIN raw.agro_meteo_data r 
        ON l.meteo_landing_id = r.meteo_landing_id
        WHERE r.meteo_landing_id IS NULL;
    '''
    
    @dataclass
    class LandingRawfTransfer:
        soil_data: str = transfer_soil_data
        meteo_data: str = transfer_meteo_data
        agro_meteo_data: str = transfer_agro_meteo_data
        
    return LandingRawfTransfer()
