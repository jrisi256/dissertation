library(here)
library(ipumsr)

request <- define_extract_nhgis("nhgis", shapefiles = c("us_county_2021_tl2021", "us_state_2021_tl2021"))
submit <- submit_extract(request)
wait <- wait_for_extract(submit)
download <- download_extract(wait, download_dir = here("1_get_data", "output"))
file.rename(download, here("1_get_data", "output", "ipums_shapefileCountyState2021.zip"))
