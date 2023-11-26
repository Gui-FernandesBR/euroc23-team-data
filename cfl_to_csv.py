"""This file helps on the creation of a .csv file from the .cfl files."""

import os

import pandas as pd

import binary_parser
from embedded_constants import FLIGHT_MAP


def main(folder):
    """This makes the magic. The 'folder' argument is the name of the folder
    where the .cfl file is located. The code also works for multiple .cfl files
    within the same folder.
    """
    # search for all the files in folder that ends with ".cfl"
    files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, f)) and f.endswith(".cfl")
    ]

    for file in files:
        dictionary, plot_dir, base_name = binary_parser.extract_data(
            input_log_path=file,
            output_log_path=None,
            state_map=FLIGHT_MAP,
            from_notebook=False,
        )

        # Merge all the dataframes into one single one.
        df_all = pd.DataFrame()

        # Iterate over all dataframes
        for name, df in dictionary.items():
            if name in ["flight_states_df", "error_info_df", "event_info_df"]:
                continue
            if "ts" in df.columns:
                if df_all.empty:
                    df_all = df
                else:
                    try:
                        df_all = df_all.merge(df, on="ts", how="outer")
                    except KeyError as e:
                        print(f"Unable to merge with {name}: {e}")
                        continue
            else:
                print(f"No 'ts' column in {name}")

        # remove unnecessary columns
        to_be_removed = ["id_x", "id_y", "acceleration", "height", "voltage"]
        df_all.drop(columns=to_be_removed, inplace=True, errors="ignore")

        # sort the dataframe ascending order column "ts"
        df_all.sort_values(by="ts", inplace=True)

        # round the values to appropriate decimal places (this reduces file size)
        # round the values to appropriate decimal places (this reduces file size)
        df_all["ts"] = df_all["ts"].round(3)  # Timestamps
        df_all["Ax"] = df_all["Ax"].round(4)  # Accelerometer data
        df_all["Ay"] = df_all["Ay"].round(4)  # Accelerometer data
        df_all["Az"] = df_all["Az"].round(4)  # Accelerometer data
        df_all["Gx"] = df_all["Gx"].round(4)  # Gyroscope data
        df_all["Gy"] = df_all["Gy"].round(4)  # Gyroscope data
        df_all["Gz"] = df_all["Gz"].round(4)  # Gyroscope data
        df_all["T"] = df_all["T"].round(1)  # Temperature
        df_all["P"] = df_all["P"].round()  # Pressure
        df_all["velocity"] = df_all["velocity"].round(3)
        df_all["q0_estimated"] = df_all["q0_estimated"].round(5)
        df_all["q1_estimated"] = df_all["q1_estimated"].round(5)
        df_all["q2_estimated"] = df_all["q2_estimated"].round(5)
        df_all["q3_estimated"] = df_all["q3_estimated"].round(5)
        df_all["filtered_altitude_AGL"] = df_all["filtered_altitude_AGL"].round(2)
        df_all["filtered_acceleration"] = df_all["filtered_acceleration"].round(3)

        # remove any duplicated value of "ts" column
        df_all.drop_duplicates(subset="ts", inplace=True)

        # drop rows that are completely null
        df_all.dropna(how="all", inplace=True)

        # save to csv file
        df_all.to_csv(f"{folder}/{base_name}_all.csv", index=False)

        # save files individually
        # for name, df in dictionary.items():
        #     # save the dictionary to a csv file
        #     # df = pd.DataFrame.from_dict(dictionary, orient="index")
        #     df.to_csv(f"{folder}/{base_name}_{name}.csv", index=False)


if __name__ == "__main__":
    folders = [
        "01_astg",
        "03_air_esiea",
        "05_asat",
        "07_aesir",
        "10_epfl",
        "11_faraday",
        "12_bristol",
        "16_ns",
        "17_polito",
        "18_ntnu",
        "19_put",
        "20_red",
        "23_star",
    ]
    for folder in folders:
        main(folder)
        print(f"Successfully parsed .cfl files from {folder}")
