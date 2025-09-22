library(here)
library(readr)
library(dplyr)
library(purrr)

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
        "624310", "624410", "6732", "8132", "8133", "8300"
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
cbp_county_social_capital <-
    cbp_county |>
    filter(
        emplysize == "All establishments", ind_code %in% social_capital_codes
    ) |>
    count(
        year, full_fips, state, county,
        wt = nr_businesses,
        name = "nr_social_associations"
    )

################################################################################
# Calculate the total number of social support organizations.
cbp_county_social_support <-
    cbp_county |>
    filter(
        emplysize == "All establishments", ind_code %in% social_support_codes
    ) |>
    count(
        year, full_fips, state, county,
        wt = nr_businesses,
        name = "nr_social_support_services"
    )

################################################################################
# Calculate the total number of social support + social capital organizations.
cbp_county_all_social <-
    cbp_county |>
    filter(
        emplysize == "All establishments",
        ind_code %in% c(social_capital_codes, social_support_codes)
    ) |>
    count(
        year, full_fips, state, county,
        wt = nr_businesses,
        name = "nr_social_associations_and_support"
    )

################################################################################
# Calculate the total number of all types of social organizations.
cbp_county_social_beauty <-
    cbp_county |>
    filter(
        emplysize == "All establishments",
        ind_code %in% c(social_capital_codes, social_support_codes, beauty_codes)
    ) |>
    count(
        year, full_fips, state, county,
        wt = nr_businesses,
        name = "nr_all_social_and_beauty_businesses"
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
