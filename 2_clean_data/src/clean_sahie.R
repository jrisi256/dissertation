library(here)
library(dplyr)
library(readr)
library(purrr)
library(tidyr)

read_dir <- here("1_get_data", "output")
save_dir <- here("2_clean_data", "output")

# Read in data.
sahie_county <- read_csv(file.path(read_dir, "sahie_county.csv"))

# Variable names and labels.
var_labels <-
    c(
        sahie_uninsured_nr_est = "NUI_PT",
        sahie_uninsured_nr_moe = "NUI_MOE",
        sahie_uninsured_prcnt_est = "PCTUI_PT",
        sahie_uninsured_prcnt_moe = "PCTUI_MOE",
        sahie_uninsured_nr_denom = "NIPR_PT"
    )

# Restructure data for analysis.
sahie_county_clean <-
    sahie_county |>
    pivot_wider(
        id_cols = c("year", "state", "county", "full_fips"),
        names_from = AGE_DESC,
        values_from = PCTUI_PT
    ) |>
    rename(
        "sahie_uninsured_prcnt_est_0to19" = "Under 19 years",
        "sahie_uninsured_prcnt_est_under65" = "Under 65 years"
    )

# Save cleaned data.
write_csv(sahie_county_clean, file.path(save_dir, "sahie_county_clean.csv"))
