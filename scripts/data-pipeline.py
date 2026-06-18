from data_eng.sncf_getter import (
    get_sncf_theoretical_train_data,
    get_sncf_trip_update_train_data,
)

from data_eng.sncf_transformer import (
    transform_sncf_theoretical_train_data,
    transform_sncf_trip_updates_data,
)

THEORY_DATA_FOLDER = "data/theory/"
CONTINUE_DATA_FOLDER = "data/continue/"

###############################
#### Bronze layer
###############################

### Theoretical data
sncf_theoretical_data = get_sncf_theoretical_train_data()

for filename, file in sncf_theoretical_data["files"].items():
    with open(THEORY_DATA_FOLDER + "bronze/" + filename, "wb") as fw:
        fw.write(file)

### Continuous data
sncf_trip_update_data = get_sncf_trip_update_train_data()

with open(CONTINUE_DATA_FOLDER + "bronze/" + "sncf_trip_update.pb", "wb") as fw:
    fw.write(sncf_trip_update_data)


###############################
#### Silver Layer
###############################

### Theoretical data
transform_sncf_theoretical_train_data(
    input_folder_path=THEORY_DATA_FOLDER + "bronze/",
    output_folder_path=THEORY_DATA_FOLDER + "silver/",
)

transform_sncf_trip_updates_data(
    trip_updates_input_path=CONTINUE_DATA_FOLDER + "bronze/" + "sncf_trip_update.pb",
    trip_updates_output_path=CONTINUE_DATA_FOLDER
    + "silver/"
    + "sncf_trip_update.parquet",
)
