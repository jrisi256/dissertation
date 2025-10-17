library(sf)
library(here)
library(readr)
library(dplyr)
library(tidyr)
library(ipumsr)
library(stringr)
library(ggplot2)

################################################################################
# Read in data.
data <- read_csv(here('3_merge_data', 'output', 'merged_data.csv.gz'))
data_no_missing <- data

county_shapefiles_2021 <-
    read_ipums_sf(
        here("1_get_data", "output", "ipums_shapefileCountyState2021.zip"),
        file_select = "nhgis0102_shape/nhgis0102_shapefile_tl2021_us_county_2021.zip"
    )

state_shapefiles_2021 <-
    read_ipums_sf(
        here("1_get_data", "output", "ipums_shapefileCountyState2021.zip"),
        file_select = "nhgis0102_shape/nhgis0102_shapefile_tl2021_us_state_2021.zip"
    ) |>
    filter(!(STATEFP %in% c("02", 15, 72)))

################################################################################
# Make data long so it is easier to work with for missing analysis.
data_long <-
    data_no_missing |>
    pivot_longer(
        cols = -matches("^year$|full_fips|^state$|^county$"),
        names_to = "variable",
        values_to = "value"
    ) |>
    arrange(year, variable, full_fips)

################################################################################
# Drop any columns which are completely missing in a given year.
data_variable_year_missing <-
    data_long |>
    group_by(variable, year) |>
    summarise(all_values_missing_this_year = all(is.na(value) | value == 0)) |>
    ungroup() |> 
    arrange(-all_values_missing_this_year, variable, year)

data_long_drop_missing_vars <-
    data_long |>
    filter(!str_detect(variable, "[iI]nsurance|doNotLiveWithParent"))

################################################################################
# Drop columns which are missing a lot of values in a given year OR
# Drop counties which are consistently missing values across variables.
data_variable_missing <-
    data_long_drop_missing_vars |>
    mutate(is_missing = if_else(is.na(value), 1, 0)) |>
    group_by(variable, year) |>
    summarise(number_missing_per_year = sum(is_missing)) |>
    ungroup() |>
    filter(number_missing_per_year > 0) |>
    arrange(variable, year)

# Create clean data with no missing values (backwards imputation process).
data_no_missing <-
    data |>
    filter(
        !is.na(total_prison_adm_rate),
        !is.na(total_prison_pop_rate),
        !is.na(total_prison_adm_rate_self),
        !is.na(total_prison_pop_rate_self),
        !is.na(nr_socialAssociations_per10k),
        #!is.na(nrBirthsPer10000Unmarried_rate_est_ages15to50_w_f),
        !is.na(yearMoved_median_est),
        !is.na(hhNoPhone_prcnt_est),
        !is.na(hhShareOfIncome_1stQuintile_est),
        !is.na(homeValue_25thPtile_est),
        !is.na(grossMortgagePrcntIncome_median_est)
    ) |>
    # For now, drop columns with a high amount of missing values.
    select(
        -matches("hhIncome_median.*(b|w|h)|medianHhIncome(Black|Hisp)|nr_socialSupportServices|[iI]nsurance|doNotLiveWithParent")
    )

write_csv(
    data_no_missing,
    here("4_clean_missing_data", "output", "data_no_missing.csv")
)

################################################################################
# Map county coverage.
counties_in_sample <-
    county_shapefiles_2021 |>
    filter(!(STATEFP %in% c("02", 15, 72))) |>
    mutate(in_sample = GEOID %in% data_no_missing$full_fips)

ggplot(counties_in_sample) +
    geom_sf(aes(fill = in_sample)) +
    geom_sf(data = state_shapefiles_2021, fill = NA, linewidth = 1) +
    labs(fill = "County in sample") +
    theme_void()
