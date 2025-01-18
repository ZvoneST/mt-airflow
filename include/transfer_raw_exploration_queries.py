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
        WITH max_date_cte AS (
            SELECT MAX(meteo_time)::DATE AS max_date
            FROM external_data.hourly_meteo_data
        )
        INSERT INTO external_data.hourly_meteo_data (
            meteo_location_id,
            air_temperature,
            humidity,
            precipitation,
            pressure,
            meteo_time,
            created_on
        )
        SELECT 
            source_data.meteo_location_id,
            (hour_data->'airTemperature'->>'sg')::NUMERIC AS air_temperature,
            (hour_data->'humidity'->>'sg')::NUMERIC AS humidity,
            (hour_data->'precipitation'->>'sg')::NUMERIC AS precipitation,
            (hour_data->'pressure'->>'sg')::NUMERIC AS pressure,
            (hour_data->>'time')::TIMESTAMP AS meteo_time,
            source_data.created_on
        FROM dblink(
            'dbname=raw_zone user={user} password={password} host=localhost',
            'SELECT meteo_location_id, meteo_data, created_on FROM raw.meteo_data'
        ) AS source_data(meteo_location_id INTEGER, meteo_data JSON, created_on TIMESTAMP),
            json_array_elements(source_data.meteo_data->'hours') AS hour_data,
            max_date_cte
        WHERE
            CAST(source_data.created_on AS DATE) > (
                SELECT COALESCE(MAX(created_on)::DATE, '2000-01-01'::DATE)
                FROM external_data.hourly_meteo_data
            );
    '''
    
    raw_landing_agro_meteo_query = f'''
        WITH max_date_cte AS (
            SELECT MAX(meteo_time)::DATE AS max_date
            FROM external_data.hourly_agro_meteo_data
        )
        INSERT INTO external_data.hourly_agro_meteo_data (
            meteo_location_id,
            soil_moisture,
            soil_moisture_10cm,
            soil_temperature,
            soil_temperature_10cm,
            meteo_time,
            created_on
        )
        SELECT
            source_data.meteo_location_id,
            (hour_data->'soilMoisture'->>'sg')::NUMERIC AS soil_moisture,
            (hour_data->'soilMoisture10cm'->>'sg')::NUMERIC AS soil_moisture_10cm,
            (hour_data->'soilTemperature'->>'sg')::NUMERIC AS soil_temperature,
            (hour_data->'soilTemperature10cm'->>'sg')::NUMERIC AS soil_temperature_10cm,
            (hour_data->>'time')::TIMESTAMP AS meteo_time,
            source_data.created_on
        FROM dblink(
            'dbname=raw_zone user={user} password={password} host=localhost',
            'SELECT meteo_location_id, agro_meteo_data, created_on FROM raw.agro_meteo_data'
        ) AS source_data(meteo_location_id INTEGER, agro_meteo_data JSON, created_on TIMESTAMP),
            json_array_elements(source_data.agro_meteo_data->'hours') AS hour_data
        WHERE
            CAST(source_data.created_on AS DATE) > (
                SELECT COALESCE(MAX(created_on)::DATE, '2000-01-01'::DATE)
                FROM external_data.hourly_agro_meteo_data);
    '''
    
    # raw_landing_satellite_query = f'''
    
    # '''
    
    @dataclass
    class RawExplorationTransfer:
        raw_landing_soil: str = raw_landing_soil_query
        raw_landing_meteo: str = raw_landing_meteo_query
        raw_landing_agro_meteo: str = raw_landing_agro_meteo_query
    
    return RawExplorationTransfer()