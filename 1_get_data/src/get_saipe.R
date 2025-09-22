library(here)
library(dplyr)
library(readr)
library(purrr)
library(censusapi)

out_dir <- here("1_get_data", "output")

# Get a list of all Census data sets.
available_census_datasets <- listCensusApis()

# Get the SAIPE (Small Area Income and Poverty Estimates) data set specifically.
saipe_dataset <-
    available_census_datasets |>
    filter(name == "timeseries/poverty/saipe")

# Get metadata for SAIPE.
saipe_metadata <- listCensusMetadata(saipe_dataset$name, type = "variables")

# API Call to capture SAIPE data.
get_saipe_data <- function(saipe_data, region, vars_arg) {
    getCensus(
        name = saipe_data$name,
        region = paste0(region, ":*"),
        time = "from 1989",
        vars = c(vars_arg, "GEOID")
    ) |>
        select(-matches("us")) |>
        rename("year" = "time", "full_fips" = "GEOID")
}

# Variable names and labels.
vars <-
    c(
        "SAEMHI_PT", "SAEMHI_MOE", "SAEPOV0_17_PT", "SAEPOV0_17_MOE",
        "SAEPOVALL_PT", "SAEPOVALL_MOE", "SAEPOVRT0_17_PT", "SAEPOVRT0_17_MOE",
        "SAEPOVRTALL_PT", "SAEPOVRTALL_MOE", "SAEPOVU_0_17", "SAEPOVU_ALL"
    )

# Query API to get requested data.
saipe_data <-
    map(
        list(country = "us", state = "state", county = "county"),
        get_saipe_data,
        saipe_data = saipe_dataset,
        vars_arg = vars
    )

# Save data
pwalk(
    list(saipe_data, names(saipe_data)),
    function(df, file_name, dir) {
        write_csv(df, file.path(dir, paste0("saipe_", file_name, ".csv")))
    },
    dir = out_dir
)
