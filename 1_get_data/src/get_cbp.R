library(here)
library(dplyr)
library(readr)
library(purrr)
library(tidyr)
library(R.utils)
library(stringr)
library(censusapi)

out_dir <- here("1_get_data", "output")

################################################################################
# Set-up
# Get a list of all Census data sets.
available_census_datasets <- listCensusApis()

# CBP data is vintage-based. It is not a time series.
cbp_datasets <- available_census_datasets |> filter(name == "cbp")

# SIC and NAICS industry codes
sic_1986 <-
    c(
        "00", "07", 10, 15, 19, 40, 50, 52, 60, 70, 99, 6732, 7230, 7240, 7930,
        7932, 7933, 7992, 7997, 8300, 8310, 8360, 8600, 8610, 8620, 8630, 8640,
        8650, 8660, 8690
    )
sic_1987 <-
    c(
        "00", "07", 10, 15, 20, 40, 50, 52, 60, 70, 99, 6732, 7230, 7240, 7930,
        7932, 7933, 7992, 7997, 8300, 8310, 8360, 8600, 8610, 8620, 8630, 8640,
        8650, 8660, 8690
    )
sic_1988_1997 <-
    c(
        "00", "07", 10, 15, 20, 40, 50, 52, 60, 70, 99, 6732, 7230, 7240, 7930,
        7991, 7992, 7997, 8300, 8320, 8330, 8350, 8360, 8390, 8600, 8610, 8620,
        8630, 8640, 8650, 8660, 8690
)
naics_1998_2002 <-
    c(
        11, 21:23, 42, 51:56, 61:62, 71:72, 81, 99, "00", "31-33", "44-45", "48-49", 95,
        "623210", "623220", "623312", "623990", "624110", "624190", "6242",
        "624310", "624410", "713910", "713940", "713950", "713990", "81211",
        "813110", "8132", "8133", "813410", "813910", "813920", "813930", "813940"
    )
naics_2003_2023 <-
    c(
        11, 21:23, 42, 51:56, 61:62, 71:72, 81, 99, "00", "31-33", "44-45", "48-49",
        "623210", "623220", "623312", "623990", "624110", "624190", "6242",
        "624310", "624410", "713910", "713940", "713950", "713990", "81211",
        "813110", "8132", "8133", "813410", "813910", "813920", "813930", "813940"
    )

api_args_1986 <- expand_grid(year = 1986, ind_code = sic_1986)
api_args_1987 <- expand_grid(year = 1987, ind_code = sic_1987)
api_args_1988_1997 <- expand_grid(year = 1988:1997, ind_code = sic_1988_1997)
api_args_1998_2002 <- expand_grid(year = 1998:2002, ind_code = naics_1998_2002)
api_args_2003_2023 <- expand_grid(year = 2003:2023, ind_code = naics_2003_2023)
api_args <-
    bind_rows(
        api_args_1986, api_args_1987, api_args_1988_1997, api_args_1998_2002,
        api_args_2003_2023
    )

################################################################################
# Function for querying CBP API.
get_cbp_data <- function(vintage_arg, region_arg, ind_code_arg, regionin_arg = NULL) {
    print(paste0("YEAR: ", vintage_arg, "\n"))
    print(paste0("GEOGRAPHY: ", region_arg, "\n"))
    print(paste0("INDUSTRY CODE: ", ind_code_arg, "\n"))
    print(paste0("REGION IN: ", regionin_arg))
    
    # Main variables and grouping variables.
    vars <- c(nr_businesses = "ESTAB")
    groups <- c("EMPSZES")
    
    # County-level data cannot be disaggregated by legal corporate status.
    # Older vintages do not have corporate legal status, either.
    if(region_arg != "county" & vintage_arg >= 2008 & vintage_arg <= 2011) {
        groups <- c(groups, "LFO", "LFO_TTL")
    } else if(region_arg != "county" & vintage_arg >= 2012) {
        groups <- c(groups, "LFO", "LFO_LABEL")
    }
    
    # Employee-size label variable name changes depending on the vintage.
    if(vintage_arg %in% 1986:2011) {
        groups <- c(groups, "EMPSZES_TTL")
    } else if(vintage_arg >= 2012) {
        groups <- c(groups, "EMPSZES_LABEL")
    }
    
    print(paste0("VARS: ", c(vars, groups, "YEAR")))
    
    # The vintage determines which industry codes we will use.
    if(vintage_arg %in% 2017:2023) {
        groups <- c(groups, "NAICS2017_LABEL")
        df <-
            getCensus(
                name = "cbp",
                vintage = vintage_arg,
                region = paste0(region_arg, ":*"),
                regionin = regionin_arg,
                vars = c(vars, groups, "YEAR"),
                NAICS2017 = ind_code_arg
            )
    } else if(vintage_arg %in% 2012:2016) {
        groups <- c(groups, "NAICS2012_LABEL")
        df <-
            getCensus(
                name = "cbp",
                vintage = vintage_arg,
                region = paste0(region_arg, ":*"),
                regionin = regionin_arg,
                vars = c(vars, groups, "YEAR"),
                naics2012 = ind_code_arg
            )
    } else if(vintage_arg %in% 2008:2011) {
        groups <- c(groups, "NAICS2007_TTL")
        df <-
            getCensus(
                name = "cbp",
                vintage = vintage_arg,
                region = paste0(region_arg, ":*"),
                regionin = regionin_arg,
                vars = c(vars, groups, "YEAR"),
                naics2007 = ind_code_arg
            )
    } else if(vintage_arg %in% 2003:2007) {
        groups <- c(groups, "NAICS2002_TTL")
        df <-
            getCensus(
                name = "cbp",
                vintage = vintage_arg,
                region = paste0(region_arg, ":*"),
                regionin = regionin_arg,
                vars = c(vars, groups, "YEAR"),
                naics2002 = ind_code_arg
            )
    } else if(vintage_arg %in% 1998:2002) {
        groups <- c(groups, "NAICS1997_TTL")
        df <-
            getCensus(
                name = "cbp",
                vintage = vintage_arg,
                region = paste0(region_arg, ":*"),
                regionin = regionin_arg,
                vars = c(vars, groups, "YEAR"),
                naics1997 = ind_code_arg
            )
    } else if(vintage_arg %in% 1986:1997) {
        groups <- c(groups, "SIC_TTL")
        df <-
            getCensus(
                name = "cbp",
                vintage = vintage_arg,
                region = paste0(region_arg, ":*"),
                regionin = regionin_arg,
                vars = c(vars, groups, "YEAR"),
                sic = ind_code_arg
            )
    }
    
    # Drop redundant columns and standardize column names.
    df <-
        df |>
        select(-matches("EMPSZES$|us|LFO$")) |>
        rename(
            vars,
            "year" = "YEAR",
            any_of(
                c(
                    "emplysize" = "EMPSZES_TTL", "emplysize" = "EMPSZES_LABEL",
                    "ind_code" = "NAICS2017", "ind_code" = "NAICS2012",
                    "ind_code" = "NAICS2007", "ind_code" = "NAICS2002",
                    "ind_code" = "NAICS1997", "ind_code" = "SIC",
                    "ind_label" = "NAICS2017_LABEL", "ind_label" = "NAICS2012_LABEL",
                    "ind_label" = "NAICS2007_TTL", "ind_label" = "NAICS2002_TTL",
                    "ind_label" = "NAICS1997_TTL", "ind_label" = "SIC_TTL",
                    "legal_status" = "LFO_TTL", "legal_status" = "LFO_LABEL"
                )
            )
        ) |>
        mutate(
            ind_code = trimws(as.character(ind_code)),
            ind_code = 
                case_when(
                    is.na(ind_code) & str_detect(ind_label, "Manufacturing") ~ "31-33",
                    is.na(ind_code) & str_detect(ind_label, "Retail") ~ "44-45",
                    is.na(ind_code) & str_detect(ind_label, "Transportation") ~ "48-49",
                    ind_code == 0 ~ "00",
                    !is.na(ind_code) ~ ind_code
                ),
            ind_code_category = str_remove(groups[length(groups)], "_TTL|_LABEL")
        )
    
    # The region dictates how we construct the FIPS code.
    if(region_arg == "us") {
        df <- df |> mutate(full_fips = "00000")
    } else if(region_arg == "state") {
        df <-
            df |>
            mutate(
                state = if_else(str_length(state) == 1, paste0(0, state), state),
                full_fips = paste0(state, "000")
            )
    } else if(region_arg == "county") {
        df <-
            df |>
            mutate(
                state = if_else(str_length(state) == 1, paste(0, state), state),
                county =
                    case_when(
                        str_length(county) == 1 ~ paste0("00", county),
                        str_length(county) == 2 ~ paste0("0", county),
                        str_length(county) == 3 ~ county
                    ),
                full_fips = paste0(state, county)
            )
    }
    
    return(df)
}

################################################################################
# Query data at the country level.
cbp_data_country <-
    pmap(
        list(api_args$year, api_args$ind_code),
        get_cbp_data,
        region_arg = "us"
    ) |>
    bind_rows() |>
    mutate(
        legal_status = trimws(legal_status),
        legal_status = if_else(is.na(legal_status), "All establishments", legal_status)
    )

################################################################################
# Query data at the state level.
cbp_data_state <-
    pmap(
        list(api_args$year, api_args$ind_code),
        get_cbp_data,
        region_arg = "state"
    ) |>
    bind_rows() |>
    mutate(
        legal_status = trimws(legal_status),
        legal_status = if_else(is.na(legal_status), "All establishments", legal_status)
    )

################################################################################
# Query data at the county level.
state_codes_part1 <-
    c(
        "01", "02", "04", "05", "06", "08", "09", "10", "12", "13", "15", "16",
        "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
        "29"
    ) |>
    paste(collapse = ",") %>%
    paste0("state:", .)

state_codes_part2 <-
    c(
        "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41",
        "42", "44", "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
        "56"
    ) |>
    paste0(collapse = ",") %>%
    paste0("state:", .)

cbp_data_county <-
    map(
        list(state_codes_part1, state_codes_part2),
        function(state_code) {
            pmap(
                list(api_args$year, api_args$ind_code),
                get_cbp_data,
                region_arg = "county",
                regionin_arg = state_code
            ) |>
                bind_rows()
        }
    ) |>
    bind_rows() |>
    mutate(
        state = str_replace_all(state, "\\s", ""),
        full_fips = str_replace_all(full_fips, "\\s", "")
    )

################################################################################
# Save the data.
write_csv(cbp_data_country, file.path(out_dir, "cbp_country.csv"))
write_csv(cbp_data_state, file.path(out_dir, "cbp_state.csv"))
gzip(file.path(out_dir, "cbp_state.csv"), remove = T, overwrite = T)
write_csv(cbp_data_county, file.path(out_dir, "cbp_county.csv"))
gzip(file.path(out_dir, "cbp_county.csv"), remove = T, overwrite = T)
