from dataclasses import dataclass

def transfer_raw_exploration_queries(user: str, password: str):

    raw_landing_soil_query = f'''
        SELECT
            x.field_id,
            x.soil_depth,
            MAX(CASE WHEN x.property_name = 'bdod' THEN (x.mean_value / 100) END)::DECIMAL(5,2)       AS bdod,
            MAX(CASE WHEN x.property_name = 'clay' THEN (x.mean_value / 10) END)::DECIMAL(5,2)        AS clay,
            MAX(CASE WHEN x.property_name = 'silt' THEN (x.mean_value / 10) END)::DECIMAL(5,2)        AS silt,
            MAX(CASE WHEN x.property_name = 'sand' THEN (x.mean_value / 10) END) ::DECIMAL(5,2)       AS sand,
            MAX(CASE WHEN x.property_name = 'nitrogen' THEN (x.mean_value / 100) END)::DECIMAL(5,2)   AS nitrogen,
            MAX(CASE WHEN x.property_name = 'soc' THEN (x.mean_value / 10) END)::DECIMAL(5,2)         AS soc,
            MAX(CASE WHEN x.property_name = 'phh2o' THEN (x.mean_value / 10) END)::DECIMAL(5,2)       AS phh2o,
            MAX(CASE WHEN x.property_name = 'cec' THEN (x.mean_value / 10) END)::DECIMAL(5,2)         AS cec,
            NOW() AT TIME ZONE 'Europe/Zagreb' AS created_on
        FROM (
            SELECT
                source_data.field_id,
                layer ->> 'name' AS property_name,
                depth_item ->> 'label' AS soil_depth,
                (depth_item -> 'values' ->> 'mean')::NUMERIC AS mean_value
            FROM dblink(
                'dbname=raw_zone user={user} password={password} host=localhost',
                'SELECT field_id, soil_grid_data FROM raw.soil_data'
            ) AS source_data(field_id INTEGER, soil_grid_data JSON)
            CROSS JOIN LATERAL json_array_elements(source_data.soil_grid_data->'properties'->'layers') AS layer
            CROSS JOIN LATERAL json_array_elements(layer->'depths') AS depth_item
            WHERE layer->>'name' IN (
                'bdod', 'cec', 'clay', 'nitrogen', 'phh2o', 'sand', 'silt', 'soc'
        )
        AND NOT EXISTS (
            SELECT 1 
            FROM external_data.soil_data dest
            WHERE dest.field_id = source_data.field_id
        )
        ) AS x
        GROUP BY
        x.field_id,
        x.soil_depth;
    '''
    
    raw_landing_meteo_query = f'''
    
    '''
    
    raw_landing_agro_meteo_query = f'''
    
    '''
    
    raw_landing_meteo_query = f'''
    
    '''
    
    @dataclass
    class RawExplorationTransfer:
        raw_landing: str = raw_landing_soil_query
    
    return RawExplorationTransfer()