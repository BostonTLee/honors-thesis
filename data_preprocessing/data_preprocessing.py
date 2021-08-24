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
        "tract": "census_tract_code",
    }
    return df.rename(column_name_map, axis=1)


def read_acs_table(filepath):
    return pd.read_csv(filepath, skiprows=[1], na_values="*****")


def preprocess_acs_demographics(df):
    column_name_map = {
        "GEO_ID": "geo_id",
        "DP05_0001E": "total_pop",
        # Race
        "DP05_0037PE": "percent_white",
        "DP05_0037PM": "percent_white_moe",
        "DP05_0038PE": "percent_black",
        "DP05_0038PM": "percent_black_moe",
        "DP05_0039PE": "percent_native",
        "DP05_0039PM": "percent_native_moe",
        "DP05_0044PE": "percent_asian",
        "DP05_0044PM": "percent_asian_moe",
        "DP05_0052PE": "percent_pacific_islander",
        "DP05_0052PM": "percent_pacific_islander_moe",
        "DP05_0071PE": "percent_latino",
        "DP05_0071PM": "percent_latino_moe",
        "DP05_0057PE": "percent_other_race",
        "DP05_0057PM": "percent_other_race_moe",
        "DP05_0058PE": "percent_two_or_more_races",
        "DP05_0058PM": "percent_two_or_more_races_moe",
        # Housing
        "DP05_0086E": "total_housing_units",
        "DP05_0086M": "total_housing_units_moe",
        # Sex
        "DP05_0002PE": "percent_male",
        "DP05_0002PM": "percent_male_moe",
        "DP05_0003PE": "percent_female",
        "DP05_0003PM": "percent_female_moe",
        "DP05_0004E": "sex_ratio_mf",
        "DP05_0004M": "sex_ratio_mf_moe",
        "DP05_0028E": "sex_ratio_mf_18_over_years",
        "DP05_0028M": "sex_ratio_mf_18_over_years_moe",
        # Age
        "DP05_0008PE": "percent_15_to_19_years",
        "DP05_0008PM": "percent_15_to_19_years_moe",
        "DP05_0009PE": "percent_20_to_24_years",
        "DP05_0009PM": "percent_20_to_24_years_moe",
        "DP05_0010PE": "percent_25_to_34_years",
        "DP05_0010PM": "percent_25_to_34_years_moe",
        "DP05_0011PE": "percent_35_to_44_years",
        "DP05_0011PM": "percent_35_to_44_years_moe",
        "DP05_0012PE": "percent_45_to_54_years",
        "DP05_0012PM": "percent_45_to_54_years_moe",
        "DP05_0013PE": "percent_55_to_59_years",
        "DP05_0013PM": "percent_55_to_59_years_moe",
        "DP05_0014PE": "percent_60_to_64_years",
        "DP05_0014PM": "percent_60_to_64_years_moe",
        "DP05_0015PE": "percent_65_to_74_years",
        "DP05_0015PM": "percent_65_to_74_years_moe",
        "DP05_0016PE": "percent_75_to_84_years",
        "DP05_0016PM": "percent_75_to_84_years_moe",
        "DP05_0017PE": "percent_85_over_years",
        "DP05_0017PM": "percent_85_over_years_moe",
        "DP05_0021PE": "percent_18_over_years",
        "DP05_0021PM": "percent_18_over_years_moe",
        "DP05_0022PE": "percent_21_over_years",
        "DP05_0022PM": "percent_21_over_years_moe",
        "DP05_0023PE": "percent_62_over_years",
        "DP05_0023PM": "percent_62_over_years_moe",
        "DP05_0024PE": "percent_65_over_years",
        "DP05_0024PM": "percent_65_over_years_moe",
        "DP05_0018E": "median_age",
        "DP05_0018M": "median_age_moe",
    }
    df = df.loc[:, column_name_map.keys()]
    df = df.rename(column_name_map, axis=1)
    df["state_code"] = df["geo_id"].str.slice(9, 11).astype(int)
    df["county_code"] = df["geo_id"].str.slice(11).astype(int)
    return df


def preprocess_acs_income(df):
    column_name_map = {}
    df = df.loc[:, column_name_map.keys()]
    return df.rename(column_name_map, axis=1)
    pass


def main():
    RAW_DATA_PATH = "./../data/raw"

    SUBSTATE_COUNTY_DEF_PATH = "{}/substate_county141516.csv".format(RAW_DATA_PATH)
    substate_county_df = pd.read_csv(SUBSTATE_COUNTY_DEF_PATH)
    # substate_county_df = substate_county_df.rename(samhsa_name_dict, axis=1)
    substate_county_df = preprocess_samhsa_mapping(substate_county_df)

    SUBSTATE_TRACT_DEF_PATH = "{}/substate_tract141516.csv".format(RAW_DATA_PATH)
    substate_tract_df = pd.read_csv(SUBSTATE_TRACT_DEF_PATH)
    # substate_tract_df = substate_tract_df.rename(samhsa_name_dict, axis=1)
    substate_tract_df = preprocess_samhsa_mapping(substate_tract_df)

    substate_tract_full_df = substate_tract_df.merge(substate_county_df, how="left")

    ACS_DEMOGRAPHICS_PATH = "{}/ACSDP5Y2018.DP05_data_with_overlays_2021-08-12T180023.csv".format(
        RAW_DATA_PATH
    )
    acs_demographics_df = read_acs_table(ACS_DEMOGRAPHICS_PATH)
    print("Before processing")
    print(acs_demographics_df)
    acs_demographics_df = preprocess_acs_demographics(acs_demographics_df)
    print("After processing")
    print(acs_demographics_df)

    ACS_INCOME_PATH = "{}/ACSST5Y2018.S1901_data_with_overlays_2021-08-12T175539.csv".format(
        RAW_DATA_PATH
    )
    acs_income_df = read_acs_table(ACS_INCOME_PATH)


if __name__ == "__main__":
    main()
