library(here)
library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(censusapi)

out_dir <- here("1_get_data", "output")

# Get a list of all Census data sets.
available_census_datasets <- listCensusApis()

# Get the SAHIE (Small Area Health Insurance Estimates) data set specifically.
sahie_dataset <-
    available_census_datasets |>
    filter(name == "timeseries/healthins/sahie")

# Get metadata for SAHIE.
sahie_metadata <- listCensusMetadata(sahie_dataset$name, type = "variables")

# Variables.
vars <- c("NUI_PT", "NUI_MOE", "PCTUI_PT", "PCTUI_MOE", "NIPR_PT")

# API Call to capture SAHIE data.
get_sahie_data <- function(sahie_data, region, vars_arg) {
    vars_vec <- NULL
    
    # Race data is not available at the county level.
    if(region != "county") {
        vars_vec <- c("RACECAT", "RACE_DESC", "SEXCAT", "SEX_DESC")
    }
    
    df <-
        getCensus(
            name = sahie_data$name,
            region = paste0(region, ":*"),
            time = "from 2006",
            vars = c(vars_arg, vars_vec, "GEOID", "AGE_DESC", "AGECAT")
        ) |>
        # Drop the US column from national-level data, and drop the CAT columns.
        select(-matches("CAT|us")) |>
        rename("year" = "time", "full_fips" = "GEOID") |>
        # Keep only these age categories.
        filter(str_detect(AGE_DESC, "Under 65|Under 19"))
    
    # Keep only White, Black, Hispanic, and all race racial/ethnic categories.
    if(region != "county") {
        df <-
            df |>
            filter(str_detect(RACE_DESC, "All Races|Black|White|Hispanic.*any"))
    }
    
    return(df)
}

# Query the API to get the requested data.
sahie_data <-
    map(
        list(country = "us", state = "state", county = "county"),
        get_sahie_data,
        sahie_data = sahie_dataset,
        vars_arg = vars
    )

# Save data
pwalk(
    list(sahie_data, names(sahie_data)),
    function(df, file_name, dir) {
        write_csv(df, file.path(dir, paste0("sahie_", file_name, ".csv")))
    },
    dir = out_dir
)
