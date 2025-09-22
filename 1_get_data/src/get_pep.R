library(here)
library(dplyr)
library(readr)
library(purrr)
library(R.utils)
library(censusapi)

out_dir <- here("1_get_data", "output")
in_dir <- here("1_get_data", "input")

################################################################################
# Allows us to chunk up our PEP (Population Estimates Program) API requests.
stateFips <-
    c(
        "01", "02", "04", "05", "06", "08", "09", "10", "12", "13", "15", "16",
        "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
        "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40",
        "41", "42", "44", "45", "46", "47", "48", "49", "50", "51", "53", "54",
        "55", "56"
    )

stateFipsP1 <- stateFips[1:25] |> paste(collapse = ",") %>% paste0("state:", .)
stateFipsP2 <- stateFips[26:50] |> paste(collapse = ",") %>% paste0("state:", .)

################################################################################
# Get data from the 1990 - 1999 decade.
pep_1990_99_county_df <-
    map(
        list(stateFipsP1, stateFipsP2),
        function(states) {
            getCensus(
                name = "pep/int_charagegroups",
                vintage = 1990,
                vars = c("POP", "YEAR", "AGEGRP", "RACE_SEX", "HISP"),
                region = "county:*",
                regionin = states
            )
        }
    ) |>
    bind_rows() |>
    mutate(full_fips = paste0(state, county))

write_csv(pep_1990_99_county_df, file.path(out_dir, "pep_1999-2000_county.csv"))
gzip(file.path(out_dir, "pep_1999-2000_county.csv"), remove = T, overwrite = T)

################################################################################
# Get data from 2000 - 2009. The API is too slow so I must do it manually.
# Names of files on Census FTP server.
pep_2000_2009_county_fileNames <-
    stateFips %>%
    paste0("co-est00int-alldata-", ., ".csv")

# Download links on Census FTP server.
pep_2000_2009_county_links <-
    paste0(
        "https://www2.census.gov/programs-surveys/popest/datasets/2000-2010/intercensal/county/",
        pep_2000_2009_county_fileNames
    )

# Download each file.
pwalk(
    list(pep_2000_2009_county_links, pep_2000_2009_county_fileNames),
    function(link, dir, file_name) {
        download.file(link, file.path(dir, file_name))
    },
    dir = in_dir
)

# Name of the downloaded files.
pep_2000_2009_county_files <-
    list.files(in_dir, full.names = T, pattern = "co-est00int-alldata-")

# Combine all counties from all states into one data frame.
pep_2000_09_county_df <-
    map(
        pep_2000_2009_county_files,
        function(file) {read_csv(file, col_types = cols(STATE = "c"))}
    ) |>
    bind_rows() |>
    rename("state" = "STATE", "county" = "COUNTY") |>
    mutate(full_fips = paste0(state, county)) |>
    select(-SUMLEV, -CTYNAME, -STNAME)

write_csv(pep_2000_09_county_df, file.path(out_dir, "pep_2000-2009_county.csv"))
gzip(file.path(out_dir, "pep_2000-2009_county.csv"), remove = T, overwrite = T)

################################################################################
# Get data from 2010 - 2019. The API is too slow so I must do it manually.
download.file(
    "https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/asrh/CC-EST2020-ALLDATA.csv",
    file.path(in_dir, "CC-EST2020-ALLDATA.csv"),
    method = "wget"
)

pep_2010_19_county_df <-
    read_csv(file.path(in_dir, "CC-EST2020-ALLDATA.csv")) |>
    rename("state" = "STATE", "county" = "COUNTY") |>
    mutate(full_fips = paste0(state, county)) |>
    select(-SUMLEV, -CTYNAME, -STNAME)

write_csv(pep_2010_19_county_df, file.path(out_dir, "pep_2010-2019_county.csv"))
gzip(file.path(out_dir, "pep_2010-2019_county.csv"), remove = T, overwrite = T)

################################################################################
# Get data from 2020-2024. The data is not on the API so I must do it manually.
download.file(
    "https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/asrh/cc-est2024-alldata.csv",
    file.path(in_dir, "cc-est2024-alldata.csv")
)

pep_2020_24_county_df <-
    read_csv(file.path(in_dir, "cc-est2024-alldata.csv")) |>
    rename("state" = "STATE", "county" = "COUNTY") |>
    mutate(full_fips = paste0(state, county)) |>
    select(-SUMLEV, -CTYNAME, -STNAME)

write_csv(pep_2020_24_county_df, file.path(out_dir, "pep_2020-2024_county.csv"))
gzip(file.path(out_dir, "pep_2020-2024_county.csv"), remove = T, overwrite = T)
