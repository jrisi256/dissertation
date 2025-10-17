library(here)
library(dplyr)
library(purrr)
library(readr)
library(R.utils)

################################################################################
# Directories.
read_dir <- here("2_clean_data", "output")

################################################################################
# Read in data.
acs <- read_csv(file.path(read_dir, "acs_county_clean.csv.gz"))
cbp <- read_csv(file.path(read_dir, "cbp_county_clean.csv"))
ipumsTs <- read_csv(file.path(read_dir, "ipumsTS_county_clean.csv"))
pep <- read_csv(file.path(read_dir, "pep_county_clean.csv"))
sahie <- read_csv(file.path(read_dir, "sahie_county_clean.csv"))
saipe <- read_csv(file.path(read_dir, "saipe_county_clean.csv"))
vera <-
    read_csv(file.path(read_dir, "vera_county_clean.csv")) |>
    select(matches("year|full_fips|state|county|total_prison.*rate"))
cdc_homicides <- read_csv(file.path(read_dir, "cdc_county_homicides_clean.csv"))

################################################################################
# Drop any columns which need to be merged across different data sources.
ipumsTs_no_merge <-
    ipumsTs |>
    select(
        -matches("pop_prcnt|(white|black|hispanic)_prcnt|index|belowPoverty_prcnt|median|renter|single|pop_nr_est")
    )

acs_no_merge <-
    acs |>
    select(-matches("renters_(prcnt|ratio)|^single.*est$"))

################################################################################
# Merge PEP and IPUMS Time Series data.
ipumsTs_race_age <-
    ipumsTs |>
    select(matches("year|fips|state|county|(white|black|hispanic|pop)_prcnt|index|pop_nr")) |>
    filter(year %in% c(1980, 1990)) |>
    rename_with(
        .fn = function(col) {paste0("ipumsTs_", col)},
        .cols = matches("(white|black|hispanic|pop)_prcnt|index|pop_nr")
    )
    
merge_ipumsTs_pep <-
    full_join(ipumsTs_race_age, pep, by = c("year", "full_fips", "state", "county")) |>
    mutate(
        shannon_index_scaled =
            if_else(
                year %in% c(1980, 1990),
                ipumsTs_shannon_index_scaled,
                shannon_index_scaled
            ),
        gini_simpson_index =
            if_else(
                year %in% c(1980, 1990),
                ipumsTs_gini_simpson_index,
                gini_simpson_index
            ),
        pop_prcnt_est_allAges_w =
            if_else(
                year %in% c(1980, 1990),
                ipumsTs_white_prcnt_est_allAges,
                pop_prcnt_est_allAges_w
            ),
        pop_prcnt_est_allAges_b =
            if_else(
                year %in% c(1980, 1990),
                ipumsTs_black_prcnt_est_allAges,
                pop_prcnt_est_allAges_b
            ),
        pop_prcnt_est_allAges_h =
            if_else(
                year %in% c(1980, 1990),
                ipumsTs_hispanic_prcnt_est_allAges,
                pop_prcnt_est_allAges_h
            ),
        pop_prcnt_est_15to24_allRaces_m =
            if_else(
                year == 1980,
                ipumsTs_pop_prcnt_est_15to24_allRaces_m,
                pop_prcnt_est_15to24_allRaces_m
            ),
        pop_nr_est = if_else(year == 1980, ipumsTs_pop_nr_est, pop_nr_est)
    ) |>
    select(-matches("shannon_index$|ipumsTs"))

################################################################################
# Merge SAIPE and IPUMS time series data.
ipumsTs_poverty <-
    ipumsTs |>
    select(matches("year|fips|state|county|belowPoverty_prcnt|median")) |>
    filter(year == 1980) |>
    rename_with(
        .fn = function(col) {paste0("ipumsTs_", col)}, .cols = matches("median")
    )

merge_ipumsTs_saipe <-
    full_join(ipumsTs_poverty, saipe, by = c("year", "full_fips", "state", "county")) |>
    mutate(
        poverty_prcnt_est_allAges =
            if_else(
                year == 1980,
                belowPoverty_prcnt_est_povertyUniverse,
                poverty_prcnt_est_allAges
            ),
        poverty_prcnt_est_0to17 =
            if_else(
                year == 1980,
                belowPoverty_prcnt_est_under18,
                poverty_prcnt_est_0to17
            ),
        hhIncome_median_est =
            if_else(
                year == 1980,
                ipumsTs_HhIncome_median_est,
                hhIncome_median_est
            )
    ) |>
    select(-matches("belowPoverty|ipumsTs"))

################################################################################
# Merge ACS and IPUMS time series data.
ipumsTs_renters_race <-
    ipumsTs |>
    select(matches('year|full_fips|state|county|renter')) |>
    filter(year %in% c(1980, 1990, 2010, 2020))

acs_renters_race <-
    acs |>
    select(matches("^year$|full_fips|^state$|^county$|renters_(prcnt|ratio)")) |>
    filter(year %in% c(2009, 2011:2019, 2021:2023))

merge_ipumsTs_acs_renters_race <- bind_rows(ipumsTs_renters_race, acs_renters_race)

ipumsTs_paoc <-
    ipumsTs |>
    select(matches("year|full_fips|state|county|single")) |>
    filter(year %in% c(1980, 1990, 2000, 2010, 2020))

acs_paoc <-
    acs |>
    select(matches("^year$|full_fips|^state$|^county$|^single.*est$")) |>
    filter(year %in% c(2009, 2011:2019, 2021:2023))

merge_ipumsTs_acs_paoc <- bind_rows(ipumsTs_paoc, acs_paoc)

################################################################################
# Merge CBP and IPUMS time series data.
merge_ipumsTs_cbp <-
    inner_join(
        cbp,
        select(merge_ipumsTs_pep, year, full_fips, state, county, pop_nr_est),
        by = c("year", "full_fips", "state", "county")
    ) |>
    mutate(
        nr_businesses_per10k = nr_businesses * 10000 / pop_nr_est,
        nr_socialAssociations_per10k = nr_social_associations * 10000 / pop_nr_est,
        nr_socialSupportServices_per10k = nr_social_support_services * 10000 / pop_nr_est,
        nr_socialAssociationsAndSupport_per10k = nr_social_associations_and_support * 10000 / pop_nr_est,
        nr_allSocialBeautyBusinesses_per10k = nr_all_social_and_beauty_businesses * 10000 / pop_nr_est
    ) |>
    select(-matches("businesses$|associations$|services$|support$|pop_nr_est"))

################################################################################
# Join together all data now with unique columns.
df_final <-
    list(
        acs_no_merge, ipumsTs_no_merge, merge_ipumsTs_acs_paoc,
        merge_ipumsTs_acs_renters_race, merge_ipumsTs_cbp, merge_ipumsTs_pep,
        merge_ipumsTs_saipe, sahie, vera
    ) |>
    reduce(
        function(x, y) {
            full_join(x, y, by = c("year", "full_fips", "state", "county"))
        }
    ) |>
    filter(year >= 2010 & year <= 2019) |>
    arrange(full_fips, year) |>
    # Drop columns which are collinear.
    select(
        -lessThanHs_prcnt_est_25older_b, -lessThanHs_prcnt_est_25older_h,
        -lessThanHs_prcnt_est_25older_w, -hhCostsLessThan10PrcntIncome_prcnt_est,
        -hhLessThan5PeronPerRoom_prcnt_est, -hhWith0Problem_prcnt_est,
        -hhAbove1000Value_prcnt_est, -hhIncomeBelow10_prcnt_est_allAges_b,
        -hhIncomeBelow10_prcnt_est_allAges_h, -hhShareOfIncome_5thQuintile_est,
        -hhIncomeBelow10_prcnt_est_allAges_w, -movedDiff_prcnt_est_1AndOlder,
        -movedDiff_prcnt_est_1AndOlder_b, -movedDiff_prcnt_est_1AndOlder_w,
        -movedDiff_prcnt_est_1AndOlder_h, -blackToWhiteMoversDiff_ratio_est,
        -hispToWhiteMoversDiff_ratio_est, -hhOwnsNoVehicle_prcnt_est,
        -lessThanHs_prcnt_est_25older, -hhIncomebelow10_prcnt_est,
        -ratioIncomeToPovertyAbove200_prcnt_est_povertyUniverse,
        -singleDad_prcnt_est, -hhIncomeabove75_prcnt_est,
        -hhIncomeAbove75_prcnt_est_allAges_b,
        -hhIncomeAbove75_prcnt_est_allAges_w,
        -hhIncomeAbove75_prcnt_est_allAges_h, -matches(".*_w")
    )

write_csv(df_final, here("3_merge_data", "output", "merged_data.csv"))
gzip(here("3_merge_data", "output", "merged_data.csv"), remove = T, overwrite = T)
