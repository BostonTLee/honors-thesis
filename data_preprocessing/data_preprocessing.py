import json

import pandas as pd


def preprocess_samhsa_mapping(df):
    column_name_map = {
        "aggflg": "aggregate_area_flag",
        "county": "county_FIPS",
        "sbst16": "substate_region_id",
        "sbst16n": "substate_region_name",
        "sbsta16n": "aggregate_substate_area_name",
        "sbstag16": "aggregate_substate_area_id",
        "sbstfg16": "change_indicator",
        "state": "state_FIPS",
        "tract": "census_tract_code"
    }
    return df.rename(column_name_map, axis=1)

def preprocess_acs_demographics(df):
    pass

def preprocess_acs_income(df):
    pass

def main():
    RAW_DATA_PATH = "./../data/raw"

    SUBSTATE_COUNTY_DEF_PATH = "{}/substate_county141516.csv".format(RAW_DATA_PATH)
    substate_county_df = pd.read_csv(SUBSTATE_COUNTY_DEF_PATH)
    # substate_county_df = substate_county_df.rename(samhsa_name_dict, axis=1)
    substate_county_df = preprocess_samhsa_mapping(substate_county_df)

    SUBSTATE_TRACT_DEF_PATH = "{}/substate_tract141516.csv".format(RAW_DATA_PATH)
    substate_tract_df = pd.read_csv(SUBSTATE_TRACT_DEF_PATH)
    #substate_tract_df = substate_tract_df.rename(samhsa_name_dict, axis=1)
    substate_tract_df = preprocess_samhsa_mapping(substate_tract_df)

    substate_tract_full_df = substate_tract_df.merge(substate_county_df, how="left")
    print(substate_tract_full_df)

    ACS_DEMOGRAPHICS_PATH = "{}/ACSDP5Y2018.DP05_data_with_overlays_2021-08-12T180023.csv".format(RAW_DATA_PATH)
    # acs_demographics_df = pd.read_csv(ACS_DEMOGRAPHICS_PATH)

    ACS_INCOME_PATH = "{}/ACSDP5Y2018.DP05_data_with_overlays_2021-08-12T180023.csv".format(RAW_DATA_PATH)
    # acs_income_df = pd.read_csv(ACS_INCOME_PATH)


if __name__ == "__main__":
    main()
