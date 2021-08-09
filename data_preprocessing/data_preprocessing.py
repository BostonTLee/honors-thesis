import json

import pandas as pd


def main():
    CONFIG_PATH = "./../data/config"
    RAW_DATA_PATH = "./../data/raw"

    # Load naming config
    SAMSHA_NAME_CONFIG_FILEPATH = "{}/samhsa_column_names.json".format(CONFIG_PATH)
    with open(SAMSHA_NAME_CONFIG_FILEPATH, "r") as config:
        samhsa_name_dict = json.load(config)

    SUBSTATE_COUNTY_DEF_PATH = "{}/substate_county141516.csv".format(RAW_DATA_PATH)
    substate_county_df = pd.read_csv(SUBSTATE_COUNTY_DEF_PATH)
    substate_county_df = substate_county_df.rename(samhsa_name_dict, axis=1)

    SUBSTATE_TRACT_DEF_PATH = "{}/raw/substate_tract141516.csv".format(RAW_DATA_PATH)
    substate_tract_df = pd.read_csv(SUBSTATE_TRACT_DEF_PATH)
    substate_tract_df = substate_tract_df.rename(samhsa_name_dict, axis=1)


if __name__ == "__main__":
    main()
