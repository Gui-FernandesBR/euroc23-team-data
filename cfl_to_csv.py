"""This file helps on the creation of .csv files from the .cfl files."""

import os

import catslogs.binary_parser as binary_parser
from catslogs.embedded_constants import FLIGHT_MAP


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
        print("\033[91m" + f"Processing {file}" + "\033[0m")

        dictionary, plot_dir, base_name = binary_parser.extract_data(
            input_log_path=file,
            output_log_path=None,
            state_map=FLIGHT_MAP,
            from_notebook=False,
        )

        for name, df in dictionary.items():
            if name in ["flight_states_df", "error_info_df", "event_info_df"]:
                continue
            if "q0_estimated" in df.columns:
                df["q0_estimated"] = df["q0_estimated"] / 10
                df["q1_estimated"] = df["q1_estimated"] / 10
                df["q2_estimated"] = df["q2_estimated"] / 10
                df["q3_estimated"] = df["q3_estimated"] / 10
            if "ts" in df.columns:
                df.drop_duplicates(subset="ts", inplace=True)
                df.dropna(how="all", inplace=True)
                df.to_csv(f"{folder}/refined/{base_name}_{name}.csv", index=False)
            else:
                print("\033[91m" + f"No 'ts' column in {name}" + "\033[0m")
                print("available columns: " + ", ".join(df.columns))


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
    for f in folders:
        main(f)
        print("\033[91m" + f"Successfully parsed .cfl files from {f}" + "\033[0m")
