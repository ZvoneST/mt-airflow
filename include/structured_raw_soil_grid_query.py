structured_raw_soil_grid_query = '''
    INSERT INTO raw.structured_soil_data (
        field_id,
        soil_depth,
        bdod,
        clay,
        silt,
        sand,
        nitrogen,
        soc,
        phh2o,
        cec,
        created_on
    )

    SELECT
    x.field_id,
    x.soil_depth,
    MAX(CASE WHEN x.property_name = 'bdod' THEN x.mean_value END)      AS bdod,
    MAX(CASE WHEN x.property_name = 'clay' THEN x.mean_value END)      AS clay,
    MAX(CASE WHEN x.property_name = 'silt' THEN x.mean_value END)      AS silt,
    MAX(CASE WHEN x.property_name = 'sand' THEN x.mean_value END)      AS sand,
    MAX(CASE WHEN x.property_name = 'nitrogen' THEN x.mean_value END)  AS nitrogen,
    MAX(CASE WHEN x.property_name = 'soc' THEN x.mean_value END)       AS soc,
    MAX(CASE WHEN x.property_name = 'phh2o' THEN x.mean_value END)     AS phh2o,
    MAX(CASE WHEN x.property_name = 'cec' THEN x.mean_value END)       AS cec,
    NOW() AT TIME ZONE 'Europe/Zagreb' AS created_on
    FROM (
    SELECT
        sd.field_id,
        layer ->> 'name' AS property_name,
        depth_item ->> 'label' AS soil_depth,
        (depth_item -> 'values' ->> 'mean')::numeric AS mean_value
    FROM landing.soil_data sd
    CROSS JOIN LATERAL json_array_elements(sd.soil_grid_data->'properties'->'layers') AS layer
    CROSS JOIN LATERAL json_array_elements(layer->'depths') AS depth_item
    WHERE layer->>'name' IN (
        'bdod', 'cec', 'clay', 'nitrogen', 'phh2o', 'sand', 'silt', 'soc'
    )
    ) AS x
    GROUP BY
    x.field_id,
    x.soil_depth;
'''