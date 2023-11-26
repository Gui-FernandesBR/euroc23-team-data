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
        to_be_removed = ["id_x", "id_y", "voltage"]
        df_all.drop(columns=to_be_removed, inplace=True, errors="ignore")

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
