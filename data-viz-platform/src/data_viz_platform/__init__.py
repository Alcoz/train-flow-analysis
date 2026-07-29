import streamlit as st
import duckdb


def run_data_viz_plateform(warehouse_file: str):
    train_dw = duckdb.connect(warehouse_file)
    delay_table = train_dw.sql(
        query="""
            SELECT dim_routes.route_long_name, avg(fact_train_trips.trip_delay) as mean_trip_delay
            FROM sncf_data_analysis.main.fact_train_trips as fact_train_trips
            INNER JOIN sncf_data_analysis.main.dim_routes as dim_routes ON dim_routes.route_id=fact_train_trips.route_id
            GROUP BY dim_routes.route_long_name
            ORDER BY mean_trip_delay DESC
            """
    )
    st.title("Data platform des informations relatives horaires de trains de la SNCF")

    st.dataframe(delay_table)
