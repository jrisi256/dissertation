library(here)
library(readr)
library(dplyr)
library(rsample)
library(parsnip)
library(recipes)

################################################################################
# Read in full data.
data <- read_csv(here("4_clean_missing_data", "output", "data_no_missing.csv"))

################################################################################
# Set train and test sets.
set.seed(69)
split <- group_initial_split(data, prop = 0.8, group = full_fips)
train <- training(split)
test <- testing(split)

################################################################################
# Set model and engine.
lmreg_model <- linear_reg() |> set_engine("glmnet")

################################################################################
# Set recipe and data pre-processing steps.
recipe_prison_admissions <-
    recipe(train) |>
    update_role(total_prison_adm_rate, new_role = "outcome") |>
    update_role(-matches("^state$|^county$|full_fips|^year$|total_prison"), new_role = "predictor") |>
    update_role(matches("^state$|^county$|full_fips|^year$"), new_role = "id") |>
    step_normalize(all_predictors())
