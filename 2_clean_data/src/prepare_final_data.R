library(here)
library(dplyr)
library(purrr)
library(readr)

################################################################### Directories.
read_dir <- here("2_clean_data", "output")

################################################################## Read in data.
acs <- read_csv(file.path(read_dir, "acs_county_clean.csv.gz"))
cbp <- read_csv(file.path(read_dir, "cbp_county_clean.csv"))
ipumsTs <- read_csv(file.path(read_dir, "ipumsTS_county_clean.csv"))
pep <- read_csv(file.path(read_dir, "pep_county_clean.csv"))
sahie <- read_csv(file.path(read_dir, "sahie_county_clean.csv"))
saipe <- read_csv(file.path(read_dir, "saipe_county_clean.csv"))
vera <-
    read_csv(file.path(read_dir, "vera_county_clean.csv")) |>
    mutate(year = as.character(year))

############################################################ Merge all the data.
all_data <-
    list(acs, cbp, ipumsTs, pep, sahie, saipe) |>
    map(function(df) {df |> mutate(year = as.character(year))}) |>
    reduce(
        function(x, y) {
            full_join(x, y, by = c("year", "full_fips", "state", "county"))
        }
    ) |>
    mutate(
        nr_businesses_per10k = nr_businesses * 10000 / pep_pop_nr_est,
        nr_socialAssociations_per10k = nr_social_associations * 10000 / pep_pop_nr_est,
        nr_socialSupportServices_per10k = nr_social_support_services * 10000 / pep_pop_nr_est,
        nr_socialAssociationsAndSupport_per10k = nr_social_associations_and_support * 10000 / pep_pop_nr_est,
        nr_allSocialBeautyBusinesses_per10k = nr_all_social_and_beauty_businesses * 10000 / pep_pop_nr_est
    ) |>
    select(-matches("businesses$|associations$|services$|support$")) |>
    right_join(vera, by = c("year", "full_fips", "state", "county"))
