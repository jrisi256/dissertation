library(here)
library(R.utils)

download.file(
    "https://github.com/vera-institute/incarceration-trends/raw/refs/heads/main/incarceration_trends_county.csv",
    here("1_get_data", "output", "vera_county.csv")
)

gzip(file.path("1_get_data", "output", "vera_county.csv"), remove = T, overwrite = T)
