SELECT
    route_id,
    route_short_name,
    route_long_name,
    split_part(route_long_name, ' - ', 1) as terminal_station_1,
    split_part(route_long_name, ' - ', 2) as terminal_station_2
FROM {{ source("silver", "routes")}}