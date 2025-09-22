library(here)
library(readr)
library(dplyr)
library(stringr)

vera_data <- read_csv(here("1_get_data", "output", "vera_county.csv.gz"))

vera_data_clean <-
    vera_data |>
    select(
        year, quarter, fips, total_pop_15to64, black_pop_15to64,
        latinx_pop_15to64, white_pop_15to64, total_prison_pop,
        total_prison_pop_rate, total_prison_adm, total_prison_adm_rate,
        black_prison_pop, latinx_prison_pop, white_prison_pop, black_prison_adm,
        latinx_prison_adm, white_prison_adm, black_prison_pop_rate,
        latinx_prison_pop_rate, white_prison_pop_rate, black_prison_adm_rate,
        latinx_prison_adm_rate, white_prison_adm_rate
    ) |>
    filter(!is.na(total_prison_pop) | !is.na(total_prison_adm)) |>
    distinct(pick(-matches("quarter"))) |>
    group_by(fips, year) |>
    # In cases where there are multiple observations per year, find the mean.
    summarise(
        across(matches("pop|adm"), function(col) {mean(col, na.rm = T)})
    ) |>
    # Hand-calculated values do not always match officially reported values.
    ungroup() |>
    mutate(
        total_prison_pop_rate_self = total_prison_pop / total_pop_15to64,
        white_prison_pop_rate_self = white_prison_pop / white_pop_15to64,
        black_prison_pop_rate_self = black_prison_pop / black_pop_15to64,
        latinx_prison_pop_rate_self = latinx_prison_pop / latinx_pop_15to64,
        total_prison_adm_rate_self = total_prison_adm / total_pop_15to64,
        white_prison_adm_rate_self = white_prison_adm / white_pop_15to64,
        black_prison_adm_rate_self = black_prison_adm / black_pop_15to64,
        latinx_prison_adm_rate_self = latinx_prison_adm / latinx_pop_15to64,
        state = str_sub(fips, 1, 2),
        county = str_sub(fips, 3, 5)
    ) |>
    rename(full_fips = fips) |>
    # Variables really do not start being reported until 1983 and stop in 2019.
    filter(year >= 1983, year <= 2019)

write_csv(
    vera_data_clean,
    here("2_clean_data", "output", "vera_county_clean.csv")
)
