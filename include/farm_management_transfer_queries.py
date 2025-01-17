from dataclasses import dataclass

def farm_management_transfers(user: str, password: str):
    
    fm_agent_types = f'''
    INSERT INTO farm_management.agent_types (agent_type_id, agent_type_name)
    SELECT agent_type_id, agent_type_name
    FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT agent_type_id, agent_type_name FROM management.agent_types'
        ) AS source_data(agent_type_id smallint, agent_type_name character varying(50))
        WHERE NOT EXISTS (
            SELECT 1 
            FROM farm_management.agent_types dest
            WHERE dest.agent_type_id = source_data.agent_type_id
        );
    '''

    fm_agents = f'''
        INSERT INTO farm_management.agents (agent_id, agent_type_id, agent_name)
        SELECT agent_id, agent_type_id, agent_name
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT agent_id, agent_type_id, agent_name FROM management.agents'
        ) AS source_data(agent_id INTEGER, agent_type_id SMALLINT, agent_name CHARACTER VARYING(100))
        WHERE NOT EXISTS (
            SELECT 1
            FROM farm_management.agents dest
            WHERE dest.agent_id = source_data.agent_id
        );
    '''

    fm_agro_organizations = f'''
        INSERT INTO farm_management.agro_organizations (agro_organization_id, agro_organization_name)
        SELECT agro_organization_id, agro_organization_name
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT agro_organization_id, agro_organization_name FROM management.agro_organizations'
        ) AS source_data(agro_organization_id SMALLINT, agro_organization_name CHARACTER VARYING(100))
        WHERE NOT EXISTS (
            SELECT 1
            FROM farm_management.agro_organizations dest
            WHERE dest.agro_organization_id = source_data.agro_organization_id
        );
    '''

    fm_agrotehnical_operation_groups = f'''
        INSERT INTO farm_management.agrotehnical_operation_groups (agrotehnical_operation_group_id, agrotehnical_operation_group_name)
        SELECT agrotehnical_operation_group_id, agrotehnical_operation_group_name
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT agrotehnical_operation_group_id, agrotehnical_operation_group_name FROM management.agrotehnical_operation_groups'
        ) AS source_data(agrotehnical_operation_group_id SMALLINT, agrotehnical_operation_group_name VARCHAR(25))
        WHERE NOT EXISTS (
            SELECT 1 
            FROM farm_management.agrotehnical_operation_groups dest
            WHERE dest.agrotehnical_operation_group_id = source_data.agrotehnical_operation_group_id
        );
    '''

    fm_agrotehnical_operations = f'''
        INSERT INTO farm_management.agrotehnical_operations (agrotehnical_operation_id, agrotehnical_operation_name, agrotehnical_operation_group_id)
        SELECT agrotehnical_operation_id, agrotehnical_operation_name, agrotehnical_operation_group_id
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT agrotehnical_operation_id, agrotehnical_operation_name, agrotehnical_operation_group_id FROM management.agrotehnical_operations'
        ) AS source_data(agrotehnical_operation_id INTEGER, agrotehnical_operation_name VARCHAR(50), agrotehnical_operation_group_id SMALLINT)
        WHERE NOT EXISTS (
            SELECT 1 
            FROM farm_management.agrotehnical_operations dest
            WHERE dest.agrotehnical_operation_id = source_data.agrotehnical_operation_id
        );
    '''

    fm_crops = f'''
        INSERT INTO farm_management.crops (crop_id, crop_name)
        SELECT crop_id, crop_name
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT crop_id, crop_name FROM management.crops'
        ) AS source_data(crop_id SMALLINT, crop_name VARCHAR(100))
        WHERE NOT EXISTS (
            SELECT 1 
            FROM farm_management.crops dest
            WHERE dest.crop_id = source_data.crop_id
        );
    '''

    fm_varieties = f'''
        INSERT INTO farm_management.varieties (variety_id, crop_id, variety_name)
        SELECT variety_id, crop_id, variety_name
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT variety_id, crop_id, variety_name FROM management.varieties'
        ) AS source_data(variety_id SMALLINT, crop_id SMALLINT, variety_name VARCHAR(100))
        WHERE NOT EXISTS (
            SELECT 1 
            FROM farm_management.varieties dest
            WHERE dest.variety_id = source_data.variety_id
        );
    '''

    fm_measurement_units = f'''
        INSERT INTO farm_management.measurement_units (measurement_unit_id, measurement_unit_label)
        SELECT measurement_unit_id, measurement_unit_label
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT measurement_unit_id, measurement_unit_label FROM management.measurement_units'
        ) AS source_data(measurement_unit_id SMALLINT, measurement_unit_label VARCHAR(10))
        WHERE NOT EXISTS (
            SELECT 1
            FROM farm_management.measurement_units dest
            WHERE dest.measurement_unit_id = source_data.measurement_unit_id
        );
    '''

    fm_meteo_locations = f'''
        INSERT INTO farm_management.meteo_locations (meteo_location_id, meteo_location_name, meteo_coordinates_wkt, meteo_coordinates)
        SELECT meteo_location_id, meteo_location_name, meteo_coordinates_wkt, meteo_coordinates
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT meteo_location_id, meteo_location_name, meteo_coordinates_wkt, meteo_coordinates::TEXT FROM management.meteo_locations'
        ) AS source_data(meteo_location_id SMALLINT, meteo_location_name VARCHAR(50), meteo_coordinates_wkt TEXT, meteo_coordinates TEXT)
        WHERE NOT EXISTS (
            SELECT 1
            FROM farm_management.meteo_locations dest
            WHERE dest.meteo_location_id = source_data.meteo_location_id
        );
    '''

    fm_production_types = f'''
        INSERT INTO farm_management.production_types (production_type_id, production_type_name)
        SELECT production_type_id, production_type_name
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT production_type_id, production_type_name FROM management.production_types'
        ) AS source_data(production_type_id SMALLINT, production_type_name VARCHAR(50))
        WHERE NOT EXISTS (
            SELECT 1
            FROM farm_management.production_types dest
            WHERE dest.production_type_id = source_data.production_type_id
        );
    '''

    fm_seasons = f'''
        INSERT INTO farm_management.seasons (season_id, season, season_label)
        SELECT season_id, season, season_label
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT season_id, season, season_label FROM management.seasons'
        ) AS source_data(season_id SMALLINT, season SMALLINT, season_label VARCHAR(25))
        WHERE NOT EXISTS (
            SELECT 1
            FROM farm_management.seasons dest
            WHERE dest.season_id = source_data.season_id
        );
    '''
    
    fm_fields = f'''
        INSERT INTO farm_management.fields (
            field_id,
            agro_organization_id,
            field_name,
            area_ha,
            field_polygon_wkt,
            field_polygon,
            centroid_coordinates_wkt,
            centroid_coordinates,
            meteo_location_id
        )
        SELECT 
            field_id,
            agro_organization_id,
            field_name,
            area_ha,
            field_polygon_wkt,
            field_polygon::TEXT AS field_polygon,
            centroid_coordinates_wkt,
            centroid_coordinates::TEXT AS centroid_coordinates,
            meteo_location_id
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT 
                field_id,
                agro_organization_id,
                field_name,
                area_ha,
                field_polygon_wkt,
                field_polygon::TEXT AS field_polygon,
                centroid_coordinates_wkt,
                centroid_coordinates::TEXT AS centroid_coordinates,
                meteo_location_id
            FROM management.fields'
        ) AS source_data(
            field_id INTEGER,
            agro_organization_id SMALLINT,
            field_name VARCHAR(100),
            area_ha NUMERIC(11, 2),
            field_polygon_wkt TEXT,
            field_polygon TEXT,
            centroid_coordinates_wkt TEXT,
            centroid_coordinates TEXT,
            meteo_location_id SMALLINT
        )
        WHERE NOT EXISTS (
            SELECT 1
            FROM farm_management.fields dest
            WHERE dest.field_id = source_data.field_id
        );
    '''
    
    fm_field_tasks = f'''
        INSERT INTO farm_management.field_tasks (
            field_task_id, 
            agrotehnical_operation_id, 
            agro_organization_id, 
            field_id, 
            crop_id, 
            variety_id, 
            production_type_id, 
            agent_id, 
            season_id, 
            due_date, 
            measurement_unit_id, 
            quantity_ha, 
            total_quantity, 
            created_on
        )
        SELECT 
            field_task_id, 
            agrotehnical_operation_id, 
            agro_organization_id, 
            field_id, 
            crop_id, 
            variety_id, 
            production_type_id, 
            agent_id, 
            season_id, 
            due_date, 
            measurement_unit_id, 
            quantity_ha, 
            total_quantity, 
            created_on
        FROM dblink(
            'dbname=farm_management user={user} password={password} host=localhost',
            'SELECT 
                field_task_id, 
                agrotehnical_operation_id, 
                agro_organization_id, 
                field_id, 
                crop_id, 
                variety_id, 
                production_type_id, 
                agent_id, 
                season_id, 
                due_date, 
                measurement_unit_id, 
                quantity_ha, 
                total_quantity, 
                created_on 
            FROM management.field_tasks'
        ) AS source_data(
            field_task_id INTEGER, 
            agrotehnical_operation_id SMALLINT, 
            agro_organization_id SMALLINT, 
            field_id SMALLINT, 
            crop_id SMALLINT, 
            variety_id SMALLINT, 
            production_type_id SMALLINT, 
            agent_id SMALLINT, 
            season_id SMALLINT, 
            due_date DATE, 
            measurement_unit_id SMALLINT, 
            quantity_ha NUMERIC(12, 3), 
            total_quantity NUMERIC(12, 3), 
            created_on TIMESTAMP
        )
        WHERE NOT EXISTS (
            SELECT 1 
            FROM farm_management.field_tasks dest 
            WHERE dest.field_task_id = source_data.field_task_id
        );
    '''
    
    @dataclass
    class FarmManagementTransfer:
        agent_types: str = fm_agent_types
        agents: str = fm_agents
        agro_organizations: str = fm_agro_organizations
        agrotehnical_operation_groups: str = fm_agrotehnical_operation_groups
        agrotehnical_operations: str = fm_agrotehnical_operations
        crops: str = fm_crops
        varieties: str = fm_varieties
        measurement_units:str = fm_measurement_units
        meteo_locations: str = fm_meteo_locations
        production_types: str = fm_production_types
        seasons: str = fm_seasons
        fields: str = fm_fields
        field_tasks: str = fm_field_tasks
        
    return FarmManagementTransfer()
