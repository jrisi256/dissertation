library(here)
library(readr)
library(dplyr)
library(purrr)
library(tidyr)
library(stringr)

impute_missing_min_max <- function(df, vector_ind_codes) {
    years <- 2017:2023
    
    # Extract all county FIPS codes each year (can change from year to year).
    counties <-
        map(
            years,
            function(df_arg, year_arg) {
                df_arg |> filter(year == year_arg) |> pull(full_fips) |> unique()
            },
            df_arg = df
        )
    names(counties) <- years
    
    # Create every combination of each county and each industry code.
    counties_ind_code <-
        pmap(
            list(counties, names(counties)),
            function(counties_arg, year_arg) {
                expand_grid(
                    ind_code = vector_ind_codes,
                    full_fips = counties_arg
                ) |>
                mutate(
                    year = as.numeric(year_arg),
                    min_nr_b = 0,
                    max_nr_b = 2,
                    state = str_sub(full_fips, 1, 2),
                    county = str_sub(full_fips, 3, 5)
                )
            }
        ) |>
        bind_rows()

    # Join with the original data and use min and max to impute missing values.
    final_df <-
        full_join(
            df,
            counties_ind_code,
            by = c("year", "full_fips", "state", "county", "ind_code")
        )
}

read_dir <- here("1_get_data", "output")
save_dir <- here("2_clean_data", "output")

################################################################################
# Read in CBP county data.
cbp_county <- read_csv(file.path(read_dir, "cbp_county.csv.gz"))

################################################################################
# Establish the NAICS and SIC codes we want to capture.
social_capital_codes <-
    c(
        "713910", "713940", "713950", "713990", "813110", "813410", "813910",
        "813920", "813930", "813940", "7930", "7992", "7997", "8600"
    )

social_support_codes <-
    c(
        "623210", "623220", "623312", "623990", "624110", "624190", "6242",
        "624310", "624410",  "8132", "8133", "6732", "8300"
    )

beauty_codes <- c("81211", "7230", "7240")

################################################################################
# Calculate the total number of businesses.
cbp_county_all_business <-
    cbp_county |>
    filter(emplysize == "All establishments", ind_code == "00") |>
    select(-emplysize, -matches("ind_"))

################################################################################
# Calculate the total number of social capital enhancing associations.
cbp_county_social_capital_pre2016 <-
    cbp_county |>
    filter(
        year <= 2016,
        emplysize == "All establishments",
        ind_code %in% social_capital_codes
    ) |>
    count(
        year, full_fips, state, county,
        wt = nr_businesses,
        name = "nr_social_associations_min"
    ) |>
    mutate(nr_social_associations_max = nr_social_associations_min)

cbp_county_social_capital_post2017 <-
    cbp_county |>
    filter(
        year >= 2017,
        emplysize == "All establishments",
        ind_code %in% social_capital_codes
    ) |>
    impute_missing_min_max(
        vector_ind_codes =
            c(
                "713910", "713940", "713950", "713990", "813110", "813410",
                "813910", "813920", "813930", "813940"
            )
    ) |>
    mutate(
        nr_businesses_max = if_else(is.na(nr_businesses), max_nr_b, nr_businesses),
        nr_businesses_min = if_else(is.na(nr_businesses), min_nr_b, nr_businesses),
    ) |>
    group_by(year, full_fips, state, county) |>
    summarise(
        nr_social_associations_max = sum(nr_businesses_max),
        nr_social_associations_min = sum(nr_businesses_min)
    ) |>
    ungroup()

cbp_county_social_capital <-
    bind_rows(
        cbp_county_social_capital_pre2016, cbp_county_social_capital_post2017
    )

################################################################################
# Calculate the total number of social support organizations.
cbp_county_social_support_pre2016 <-
    cbp_county |>
    filter(
        year <= 2016,
        emplysize == "All establishments",
        ind_code %in% social_support_codes
    ) |>
    count(
        year, full_fips, state, county,
        wt = nr_businesses,
        name = "nr_social_support_services_min"
    ) |>
    mutate(nr_social_support_services_max = nr_social_support_services_min)

cbp_county_social_support_post2017 <-
    cbp_county |>
    filter(
        year >= 2017,
        emplysize == "All establishments",
        ind_code %in% social_support_codes
    ) |>
    impute_missing_min_max(
        vector_ind_codes =
            c(
                "623210", "623220", "623312", "623990", "624110", "624190",
                "6242", "624310", "624410",  "8132", "8133"
            )
    ) |>
    mutate(
        nr_businesses_max = if_else(is.na(nr_businesses), max_nr_b, nr_businesses),
        nr_businesses_min = if_else(is.na(nr_businesses), min_nr_b, nr_businesses),
    ) |>
    group_by(year, full_fips, state, county) |>
    summarise(
        nr_social_support_services_max = sum(nr_businesses_max),
        nr_social_support_services_min = sum(nr_businesses_min)
    ) |>
    ungroup()

cbp_county_social_support <-
    bind_rows(
        cbp_county_social_support_pre2016, cbp_county_social_support_post2017
    )

################################################################################
# Calculate the total number of social support + social capital organizations.
cbp_county_all_social_pre2016 <-
    cbp_county |>
    filter(
        year <= 2016,
        emplysize == "All establishments",
        ind_code %in% c(social_capital_codes, social_support_codes)
    ) |>
    count(
        year, full_fips, state, county,
        wt = nr_businesses,
        name = "nr_social_associations_and_support_min"
    ) |>
    mutate(
        nr_social_associations_and_support_max = nr_social_associations_and_support_min
    )

cbp_county_all_social_post2017 <-
    cbp_county |>
    filter(
        year >= 2017,
        emplysize == "All establishments",
        ind_code %in% c(social_capital_codes, social_support_codes)
    ) |>
    impute_missing_min_max(
        vector_ind_codes =
            c(
                "713910", "713940", "713950", "713990", "813110", "813410",
                "813910", "813920", "813930", "813940", "623210", "623220",
                "623312", "623990", "624110", "624190", "6242", "624310",
                "624410",  "8132", "8133"
            )
    ) |>
    mutate(
        nr_businesses_max = if_else(is.na(nr_businesses), max_nr_b, nr_businesses),
        nr_businesses_min = if_else(is.na(nr_businesses), min_nr_b, nr_businesses),
    ) |>
    group_by(year, full_fips, state, county) |>
    summarise(
        nr_social_associations_and_support_max = sum(nr_businesses_max),
        nr_social_associations_and_support_min = sum(nr_businesses_min)
    ) |>
    ungroup()

cbp_county_all_social <-
    bind_rows(cbp_county_all_social_pre2016, cbp_county_all_social_post2017)

################################################################################
# Calculate the total number of all types of social organizations.
cbp_county_social_beauty_pre2016 <-
    cbp_county |>
    filter(
        year <= 2016,
        emplysize == "All establishments",
        ind_code %in% c(social_capital_codes, social_support_codes, beauty_codes)
    ) |>
    count(
        year, full_fips, state, county,
        wt = nr_businesses,
        name = "nr_all_social_and_beauty_businesses_min"
    ) |>
    mutate(nr_all_social_and_beauty_businesses_max = nr_all_social_and_beauty_businesses_min)

cbp_county_social_beauty_post2017 <-
    cbp_county |>
    filter(
        year >= 2017,
        emplysize == "All establishments",
        ind_code %in% c(social_capital_codes, social_support_codes, beauty_codes)
    ) |>
    impute_missing_min_max(
        vector_ind_codes =
            c(
                "713910", "713940", "713950", "713990", "813110", "813410",
                "813910", "813920", "813930", "813940", "623210", "623220",
                "623312", "623990", "624110", "624190", "6242", "624310",
                "624410",  "8132", "8133", "81211"
            )
    ) |>
    mutate(
        nr_businesses_max = if_else(is.na(nr_businesses), max_nr_b, nr_businesses),
        nr_businesses_min = if_else(is.na(nr_businesses), min_nr_b, nr_businesses),
    ) |>
    group_by(year, full_fips, state, county) |>
    summarise(
        nr_all_social_and_beauty_businesses_max = sum(nr_businesses_max),
        nr_all_social_and_beauty_businesses_min = sum(nr_businesses_min)
    ) |>
    ungroup()

cbp_county_social_beauty <-
    bind_rows(
        cbp_county_social_beauty_pre2016, cbp_county_social_beauty_post2017
    )

################################################################################
# Merge all data together and save the results.
cbp_county_clean <-
    list(
        cbp_county_all_business, cbp_county_social_capital, cbp_county_social_support,
        cbp_county_all_social, cbp_county_social_beauty
    ) |>
    reduce(
        function(x, y) {
            full_join(x, y, by = c("year", "full_fips", "state", "county"))
        }
    ) |>
    relocate(c("year", "full_fips"), .before = "state")

write_csv(cbp_county_clean, file.path(save_dir, "cbp_county_clean.csv"))
