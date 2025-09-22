library(here)
library(purrr)
library(ipumsr)

download_dir <- here("1_get_data", "output")

# Define the spec for the extract request and the request itself.
# Submit the extract request, wait for the download, and then download the data.
download_ipums <- function(dataset_arg, table_args, geog_arg, dir) {
    spec <- ds_spec(dataset_arg, data_table = table_args, geog_levels = geog_arg)
    request <- define_extract_nhgis(datasets = spec)
    submit <- submit_extract(request)
    wait <- wait_for_extract(submit)
    download <- download_extract(wait, download_dir = dir)
    return(download)
}

# Define 1) extract request spec and 2) the request (include breakdown values).
# Submit the extract request, wait for the download, and then download the data.
download_ipums_breakdown <- function(dataset_arg, table_args, geog_arg, dir) {
    spec <-
        ds_spec(
            dataset_arg,
            data_table = table_args,
            geog_levels = geog_arg,
            breakdown_values = c("bs03.ge0000", "bs04.ch01", "bs04.ch02", "bs04.ch19")
        )
    request <- define_extract_nhgis(datasets = spec)
    submit <- submit_extract(request)
    wait <- wait_for_extract(submit)
    download <- download_extract(wait, download_dir = dir)
    return(download)
}

################################################################################
# See all data sets available from IPUMS.
datasets <- get_metadata_nhgis(type = "datasets")

# See all relevant data sets from the 1980 Census.
meta_dataset_STF1 <- get_metadata_nhgis(dataset = "1980_STF1")
meta_dataset_STF2a <- get_metadata_nhgis(dataset = "1980_STF2a")
meta_dataset_STF2b <- get_metadata_nhgis(dataset = "1980_STF2b")
meta_dataset_STF3 <- get_metadata_nhgis(dataset = "1980_STF3")
meta_dataset_STF4Ha <- get_metadata_nhgis(dataset = "1980_STF4Ha")
meta_dataset_STF4Hb <- get_metadata_nhgis(dataset = "1980_STF4Hb")
meta_dataset_STF4Pa <- get_metadata_nhgis(dataset = "1980_STF4Pa")
meta_dataset_STF4Pb <- get_metadata_nhgis(dataset = "1980_STF4Pb")

# E.g. how to get info on a specific table in a specific data set.
meta_table <- get_metadata_nhgis(dataset = "1980_STF3", data_table = "NT19")

################################################################################
# Set up the arguments for downloading IPUMS data (without breakdowns).
args_dataset <-
    list(
        "1980_STF1", "1980_STF3", "1980_STF4Ha", "1980_STF4Hb", "1980_STF4Pa",
        "1980_STF4Pb"
    )

args_table <- 
    list(
        c("NT38", "NT39", "NT17"),
        c("NT127", "NT134", "NT121A", "NT123", "NT118A", "NT115", "NT47"),
        c("NTHA62A"),
        c("NTHB51", "NTHB52", "NTHB16A"),
        c("NTPA125", "NTPA82"),
        c("NTPB14")
    )

args_geog <- as.list(rep("county", length(args_dataset)))

names <-
    args_dataset %>%
    file.path(download_dir, .) %>%
    paste0(., ".csv.zip") |>
    as.list()

################################################################################
# Set up the arguments for downloading IPUMS data (with breakdowns).
breakdown_args_dataset <- list("1980_STF4Pb")

breakdown_args_table <- 
    list(
        c(
            "NTPB119", "NTPB72A", "NTPB71A", "NTPB48", "NTPB51A", "NTPB18",
            "NTPB26", "NTPB114", "NTPB12", "NTPB14"
        )
    )

breakdown_args_geog <- as.list(rep("county", length(breakdown_args_dataset)))

names_breakdown <-
    breakdown_args_dataset %>%
    file.path(download_dir, .) %>%
    paste0(., "_breakdown.csv.zip") |>
    as.list()

################################################################################
# Download IPUMS 1980 census files (without breakdowns).
ipums_filepaths <-
    pmap(
        list(args_dataset, args_table, args_geog),
        download_ipums,
        dir = download_dir
    )

# Rename files for easy access later.
pwalk(list(ipums_filepaths, names), function(from, to) {file.rename(from, to)})

# Download IPUMS 1980 census files (with breakdowns).
ipums_filepaths_breakdown <-
    pmap(
        list(breakdown_args_dataset, breakdown_args_table, breakdown_args_geog),
        download_ipums_breakdown,
        dir = download_dir
    )

# Rename breakdown files for easy access later.
pwalk(
    list(ipums_filepaths_breakdown, names_breakdown),
    function(from, to) {file.rename(from, to)}
)
