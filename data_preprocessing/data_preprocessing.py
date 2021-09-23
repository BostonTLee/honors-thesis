import pandas as pd
import us


def drop_and_rename_cols_by_dict(df, column_name_map):
    # Subset to slice only relevant columns
    df = df.reindex(columns=column_name_map.keys())
    # Rename using the map
    df = df.rename(column_name_map, axis=1)
    return df.reset_index(drop=True)


def make_all_percent_cols_proportions(df):
    percent_cols = [col for col in df if col.startswith("percent")]
    # percent_cols = df.columns.str.startswith("percent")
    df[percent_cols] = df[percent_cols] / 100
    df.columns = df.columns.str.replace("^percent", "prop")
    return df


def merge_list_of_dfs(list_of_dfs, on):
    if len(list_of_dfs) <= 1:
        raise ValueError(
            "List of DataFrames has too few values to be joined: {}".format(
                list_of_dfs
            )
        )
    full_df = list_of_dfs[0]
    for temp_df in list_of_dfs[1:]:
        full_df = full_df.merge(temp_df, how="inner", on=on)
    return full_df


def preprocess_samhsa_mapping(df):
    column_name_map = {
        "county": "county_fips",
        "sbst16": "substate_region_id",
        "sbst16n": "substate_region_name",
        "state": "state_fips",
        "tract": "census_tract_code",
    }
    df = drop_and_rename_cols_by_dict(df, column_name_map)
    df["state_fips"] = (
        df["state_fips"].astype(str).str.pad(width=2, fillchar="0")
    )
    df["county_fips"] = (
        df["county_fips"].astype(str).str.pad(width=3, fillchar="0")
    )
    return df


def read_samhsa_table(filepath):
    return pd.read_csv(filepath, skiprows=7)


def preprocess_samhsa_table(df, variable_name):
    # Map Group 4 is the substate regions included on the SAMHSA maps
    df = df.loc[df["Map Group"] == 4]
    column_name_map = {
        "State": "state",
        "Substate Region": "substate_region_name",
        "Small Area Estimate": variable_name,
    }
    df = drop_and_rename_cols_by_dict(df, column_name_map)
    df[variable_name] = df[variable_name].str.rstrip("%").astype("float")
    # Map the state names to FIPS codes to match the region definitions
    state_name_to_code_map = us.states.mapping("name", "fips")
    df["state_fips"] = df["state"].map(state_name_to_code_map)
    return df.reset_index(drop=True)


def read_acs_table(filepath, colnames):
    df = pd.read_csv(
        filepath,
        skiprows=[1],
        usecols=colnames,
        na_values=["*****", "*", "-", "(X)"],
    )
    return df


def slice_acs_fips_col(df, geo_id_column_name):
    # Extract state and county code as integers, then drop full geo id
    df["state_fips"] = df[geo_id_column_name].str.slice(9, 11)
    df["county_fips"] = df[geo_id_column_name].str.slice(11)
    df = df.drop(geo_id_column_name, axis=1)
    return df


def read_and_preprocess_acs_demographics(filepath):
    column_name_map = {
        "GEO_ID": "geo_id",
        # Race
        "DP05_0037PE": "percent_white",
        "DP05_0038PE": "percent_black",
        "DP05_0039PE": "percent_native",
        "DP05_0044PE": "percent_asian",
        "DP05_0052PE": "percent_pacific_islander",
        "DP05_0071PE": "percent_latino",
        "DP05_0057PE": "percent_other_race",
        "DP05_0058PE": "percent_two_or_more_races",
        # Age
        "DP05_0008PE": "percent_15_to_19_years",
        "DP05_0009PE": "percent_20_to_24_years",
        "DP05_0010PE": "percent_25_to_34_years",
        "DP05_0011PE": "percent_35_to_44_years",
        "DP05_0012PE": "percent_45_to_54_years",
        "DP05_0013PE": "percent_55_to_59_years",
        "DP05_0014PE": "percent_60_to_64_years",
        "DP05_0015PE": "percent_65_to_74_years",
        "DP05_0016PE": "percent_75_to_84_years",
        "DP05_0017PE": "percent_85_over_years",
        "DP05_0021PE": "percent_18_over_years",
        "DP05_0022PE": "percent_21_over_years",
        "DP05_0023PE": "percent_62_over_years",
        "DP05_0024PE": "percent_65_over_years",
        "DP05_0018E": "median_age",
    }
    df = read_acs_table(filepath, column_name_map.keys())
    df = drop_and_rename_cols_by_dict(df, column_name_map)
    df = slice_acs_fips_col(df, "geo_id")
    return df


def read_and_preprocess_acs_income(filepath):
    column_name_map = {
        "GEO_ID": "geo_id",
        "S1901_C01_012E": "median_household_income",
        "S1901_C01_002E": "percent_households_less_than_10000",
        "S1901_C01_003E": "percent_households_10000_to_14999",
    }
    df = read_acs_table(filepath, column_name_map.keys())
    df = drop_and_rename_cols_by_dict(df, column_name_map)
    df = slice_acs_fips_col(df, "geo_id")
    return df


def read_and_preprocess_acs_education(filepath):
    column_name_map = {
        "GEO_ID": "geo_id",
        "S1501_C02_007E": "percent_25_years_over_less_than_9th_grade",
        "S1501_C02_008E": "percent_25_years_over_9th_to_12th_no_diploma",
        "S1501_C02_009E": "percent_25_years_over_high_school",
        "S1501_C02_010E": "percent_25_years_over_some_college",
        "S1501_C02_011E": "percent_25_years_over_associates",
        "S1501_C02_012E": "percent_25_years_over_bachelors",
    }
    df = read_acs_table(filepath, column_name_map.keys())
    df = drop_and_rename_cols_by_dict(df, column_name_map)
    df = slice_acs_fips_col(df, "geo_id")
    return df


def read_and_preprocess_acs_marital_status(filepath):
    column_name_map = {
        "GEO_ID": "geo_id",
        "S1201_C02_001E": "percent_married_15_years_and_older",
        "S1201_C03_001E": "percent_widowed_15_years_and_older",
        "S1201_C04_001E": "percent_divorced_15_years_and_older",
        "S1201_C05_001E": "percent_separated_15_years_and_older",
        "S1201_C06_001E": "percent_never_married_15_years_and_older",
    }
    df = read_acs_table(filepath, column_name_map.keys())
    df = drop_and_rename_cols_by_dict(df, column_name_map)
    df = slice_acs_fips_col(df, "geo_id")
    return df


def read_and_preprocess_acs_poverty(filepath):
    column_name_map = {
        "GEO_ID": "geo_id",
        "S1701_C03_001E": "percent_below_poverty_level",
    }
    df = read_acs_table(filepath, column_name_map.keys())
    df = drop_and_rename_cols_by_dict(df, column_name_map)
    df = slice_acs_fips_col(df, "geo_id")
    return df


def main():
    RAW_DATA_PATH = "./../data/raw"
    PREPROCESSED_DATA_PATH = "./../data/preprocessed"

    # Reading in SAMHSA mappings
    SUBSTATE_COUNTY_DEF_PATH = "{}/substate_county141516.csv".format(
        RAW_DATA_PATH
    )
    substate_county_df = pd.read_csv(SUBSTATE_COUNTY_DEF_PATH)
    substate_county_df = preprocess_samhsa_mapping(substate_county_df)

    SUBSTATE_TRACT_DEF_PATH = "{}/substate_tract141516.csv".format(
        RAW_DATA_PATH
    )
    substate_tract_df = pd.read_csv(SUBSTATE_TRACT_DEF_PATH)
    substate_tract_df = preprocess_samhsa_mapping(substate_tract_df)

    # Merging SAMHSA mappings into complete substate regions definition
    substate_tract_full_df = pd.concat([substate_county_df, substate_tract_df])

    # Reading SAMHSA data
    samhsa_df_list = [substate_tract_full_df]

    SAMHSA_MDE_CSV_PATH = "{}/NSDUHsubstateExcelTab32-2018.csv".format(
        RAW_DATA_PATH
    )
    samhsa_MDE_df = read_samhsa_table(SAMHSA_MDE_CSV_PATH)
    samhsa_MDE_df = preprocess_samhsa_table(
        samhsa_MDE_df,
        "percent_MDE",
    )
    samhsa_df_list.append(samhsa_MDE_df)

    SAMHSA_SUICIDAL_THOUGHTS_CSV_PATH = (
        "{}/NSDUHsubstateExcelTab31-2018.csv".format(RAW_DATA_PATH)
    )
    samhsa_suicidal_thoughts_df = read_samhsa_table(
        SAMHSA_SUICIDAL_THOUGHTS_CSV_PATH
    )
    samhsa_suicidal_thoughts_df = preprocess_samhsa_table(
        samhsa_suicidal_thoughts_df,
        "percent_suicidal_thoughts",
    )
    samhsa_df_list.append(samhsa_suicidal_thoughts_df)

    SAMHSA_ALCOHOL_USE_DISORDER_CSV_PATH = (
        "{}/NSDUHsubstateExcelTab23-2018.csv".format(RAW_DATA_PATH)
    )
    samhsa_alcohol_use_disorder_df = read_samhsa_table(
        SAMHSA_ALCOHOL_USE_DISORDER_CSV_PATH
    )
    samhsa_alcohol_use_disorder_df = preprocess_samhsa_table(
        samhsa_alcohol_use_disorder_df,
        "percent_alcohol_use_disorder",
    )
    samhsa_df_list.append(samhsa_alcohol_use_disorder_df)

    full_samhsa_df = merge_list_of_dfs(
        samhsa_df_list, on=["state_fips", "substate_region_name"]
    )

    # Reading ACS data
    acs_df_list = []

    ACS_DEMOGRAPHICS_CSV_PATH = (
        "{}/ACSDP5Y2018.DP05_data_with_overlays_2021-08-12T180023.csv".format(
            RAW_DATA_PATH
        )
    )
    acs_demographics_df = read_and_preprocess_acs_demographics(
        ACS_DEMOGRAPHICS_CSV_PATH
    )
    acs_df_list.append(acs_demographics_df)

    ACS_INCOME_CSV_PATH = (
        "{}/ACSST5Y2018.S1901_data_with_overlays_2021-08-12T175539.csv".format(
            RAW_DATA_PATH
        )
    )
    acs_income_df = read_and_preprocess_acs_income(ACS_INCOME_CSV_PATH)
    acs_df_list.append(acs_income_df)

    ACS_EDUCATION_CSV_PATH = "{}/acs_education_data_with_overlays.csv".format(
        RAW_DATA_PATH
    )
    acs_education_df = read_and_preprocess_acs_education(
        ACS_EDUCATION_CSV_PATH
    )
    acs_df_list.append(acs_education_df)

    ACS_MARITAL_STATUS_CSV_PATH = (
        "{}/acs_marital_status_data_with_overlays.csv".format(RAW_DATA_PATH)
    )
    acs_marital_status_df = read_and_preprocess_acs_marital_status(
        ACS_MARITAL_STATUS_CSV_PATH
    )
    acs_df_list.append(acs_marital_status_df)

    ACS_POVERTY_CSV_PATH = "{}/acs_poverty_data_with_overlays.csv".format(
        RAW_DATA_PATH
    )
    acs_poverty_df = read_and_preprocess_acs_poverty(ACS_POVERTY_CSV_PATH)
    acs_df_list.append(acs_poverty_df)

    full_acs_df = merge_list_of_dfs(
        acs_df_list, on=["state_fips", "county_fips"]
    )

    final_df = full_samhsa_df.merge(
        full_acs_df, on=["state_fips", "county_fips"]
    )
    final_df = make_all_percent_cols_proportions(final_df)

    FINAL_CSV_PATH = "{}/mental_health_2018.csv".format(PREPROCESSED_DATA_PATH)
    final_df.to_csv(FINAL_CSV_PATH, index=False)


if __name__ == "__main__":
    main()
