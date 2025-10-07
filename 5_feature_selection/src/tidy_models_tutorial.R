library(here)
library(tune)
library(readr)
library(tidyr)
library(dplyr)
library(purrr)
library(dials)
library(future)
library(stringr)
library(recipes)
library(rsample)
library(parsnip)
library(ggplot2)
library(yardstick)
library(workflows)
library(workflowsets)

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
# Model fitting with parsnip.
ind_vars <- train |> select(-matches("^state$|^county$|full_fips|^year$|total_prison"))
dep_vars <- train |> select("total_prison_adm_rate")

# Model fitting with Parsnip.
# Step 1: Specify the model (e.g., linear regression, random forest, GAM, etc.)
# Step 2: Specify the engine i.e., the package you want to use to estimate the model.
# Step 3: Set the mode i.e., numeric outcomes or categorical outcomes.

lm_model <- linear_reg() |> set_engine("lm")
lm_fit_parsnip <- lm_model |> fit_xy(x = ind_vars, y = dep_vars)
predictions_parsnip <- predict(lm_fit_parsnip, test)

################################################################################
# Workflows allow one to bundle data processing steps with the modeling process.
# Step 1: You need to set your parsnip model object (model + engine).
# Step 2: One can then add variables and/or the formula directly.
workflow <-
    workflow() |>
    add_model(lm_model) |>
    add_variables(
        outcome = total_prison_adm_rate,
        predictors = -matches("^state$|^county$|full_fips|^year$|total_prison")
    )

lm_fit_workflow <- fit(workflow, train)
predictions_workflow <- predict(lm_fit_workflow, test)

# The workflowsets package allows one to create many different workflows by
# combining sets of pre-processing steps with sets of model specifications.
preprocessing <-
    list(
        "prison_admissions" =
            workflow_variables(
                total_prison_adm_rate,
                -matches("^state$|^county$|full_fips|^year$|total_prison")
            ),
        "prison_population" =
            workflow_variables(
                total_prison_pop_rate,
                -matches("^state$|^county$|full_fips|^year$|total_prison")
            )
    )

# A resulting workflow set will cross every pre-processing step with every
# model type. It will return a data frame where each row represents specific
# information about a specific workflow. The actual workflow itself is stored
# in the info column and then in the resulting data frame, it is in the workflow
# column.
workflow_set <-
    workflow_set(preproc = preprocessing, models = list("lm" = lm_model))

estimated_models <-
    workflow_set |>
    mutate(
        fit = map(info, function(df) {fit(df$workflow[[1]], train)}),
        predictions = map(fit, function(fitted_model) {predict(fitted_model, test)})
    )

################################################################################
# Recipes allow one to capture any data transformations and bundle it with your
# modeling workflow. The goal is really to prevent any data leakage, remember.
# So it's important, as much as possible, to properly separate operations done
# on train and test. Even as something as simple as normalizing the data.
# The key to recipes are the step functions which apply data transformation
# steps to the appropriate columns.
recipe <-
    recipe(train) |>
    update_role(total_prison_adm_rate, new_role = "outcome") |>
    update_role(-matches("^state$|^county$|full_fips|^year$|total_prison"), new_role = "predictor") |>
    update_role(matches("^state$|^county$|full_fips|^year$"), new_role = "id") |>
    step_normalize(all_predictors())

# You can also add recipes to workflows thus combining the pre-processing steps
# with your modeling steps.
workflow_with_recipe <-
    workflow() |>
    add_model(lm_model) |>
    # Note that when we add the recipe, the independent and dependent variables
    # are also added so you do not need to explicitly set them yourself.
    add_recipe(recipe)

lm_fit_with_recipe <- fit(workflow_with_recipe, train)
predictions_recipe <- predict(lm_fit_with_recipe, test)

################################################################################
# We can use the yardstick package to determine model efficacy.
model_results <-
    test |>
    select(matches("^state$|^county$|full_fips|^year$"), total_prison_adm_rate) |>
    mutate(
        predicted_adm_parsnip = predictions_parsnip$.pred,
        predicted_adm_recipe = predictions_recipe$.pred,
        predicted_adm_workflow = predictions_workflow$.pred
    )

model_metrics <- metric_set(rmse, rsq, mae)

model_performance <-
    model_metrics(
        model_results,
        truth = total_prison_adm_rate,
        estimate = predicted_adm_recipe
    )

################################################################################
# Let's get into cross-validation, shall we?
# Here we conduct an aside where we estimate a random forest model.
rf_model <-
    rand_forest(trees = 50) |>
    set_engine("ranger") |>
    set_mode("regression")

rf_workflow <-
    workflow() |>
    add_model(rf_model) |>
    add_variables(
        outcome = total_prison_adm_rate,
        predictors = -matches("^state$|^county$|full_fips|^year$|total_prison")
    )

rf_fit <- rf_workflow |> fit(train)

rf_fit_predictions <- predict(rf_fit, test)
model_results <- model_results |> mutate(predictions_rf = rf_fit_predictions$.pred)

model_performance_rf <-
    model_metrics(
        model_results,
        truth = total_prison_adm_rate,
        estimate = predictions_rf
    )

# Now let's actually get into cross-validation. For some reason, tidymodels
# calls k-fold cross-validation v-fold. The cross-validation performance metrics
# are averaged across all estimated models and predictions. As a reminder, having
# more folds (preferred) tends to decrease bias but increase variance. To decrease
# variance, it is generally preferred to simply repeat the fold process. If you
# wanted to reduce variance, you could also decrease the number of folds, but
# this has the unfortunate side-effect of increasing bias.
validation_folds <- vfold_cv(train, v = 10, repeats = 3)

# Allows you to control certain settings for the cross-validation process.
cv_settings <- control_resamples(save_pred = T, save_workflow = T)

# Here is where we are actually going to set-up the cross-validation.
# This returns a data frame with each train/test split, the predictions from each
# fitted model, and the performance metrics. As well as anything else you specify.
plan(sequential)
start <- Sys.time()
rf_fit_with_cv <-
    rf_workflow |>
    # This replaces the normal fit function call.
    fit_resamples(
        resamples = validation_folds, control = cv_settings, metrics = model_metrics
    )
end <- Sys.time()
sequential_time <- end - start

# Thankfully, there are convenience functions which aggregate the performance
# metrics across all the re-samples.
rf_cv_performance <- collect_metrics(rf_fit_with_cv)
rf_cv_performance_non_agg <- collect_metrics(rf_fit_with_cv, summarize = F)
# And convenience functions which aggregate the predictions which is pretty cool.
rf_cv_predictions <- collect_predictions(rf_fit_with_cv)
# These convenience functions just make it easier to interact with the fitted
# object which, at first glance, is unwieldy because it is gnarly nested data
# frame.

# Cross-validation is also very easy to make run in parallel.
parallel::detectCores(logical = T) # Check number of the cores available.
plan(multisession, workers = 2)
start_p <- Sys.time()
rf_fit_with_cv_parallel <-
    rf_workflow |>
    # This replaces the normal fit function call.
    fit_resamples(
        resamples = validation_folds, control = cv_settings, metrics = model_metrics
    )
end_p <- Sys.time()
parallel_time <- end_p - start_p

################################################################################
# Let's compare different models and use cross-validation to see which one
# does better within the training set. For this exercise, we will see if we can
# predict admissions or population better.
# Step 1. Create your recipes.
recipe_admissions <-
    recipe(train) |>
    update_role(total_prison_adm_rate, new_role = "outcome") |>
    update_role(-matches("^state$|^county$|full_fips|^year$|total_prison"), new_role = "predictor") |>
    update_role(matches("^state$|^county$|full_fips|^year$"), new_role = "id") |>
    step_normalize(all_predictors())

recipe_population <-
    recipe(train) |>
    update_role(total_prison_pop_rate, new_role = "outcome") |>
    update_role(-matches("^state$|^county$|full_fips|^year$|total_prison"), new_role = "predictor") |>
    update_role(matches("^state$|^county$|full_fips|^year$"), new_role = "id") |>
    step_normalize(all_predictors())

recipes <-
    list(
        "prison_admissions" = recipe_admissions,
        "prison_population" = recipe_population
    )

# Step 2. Create a workflow set which is just a cross of recipes and models.
model_comparison <-
    workflow_set(
        preproc = recipes,
        models = list("lm" = lm_model, "rf" = rf_model)
    )

# Step 3. We want to run cross-validation on each of these workflows now. How
# can do that efficiently? Using workflow_map. You supply the workflow argument.
# You supply the function you want to apply to that workflow. Then you supply the
# arguments to the function you will be applying to each workflow.
validation_folds <- vfold_cv(train, v = 3, repeats = 2)

model_comparison_cv <-
    model_comparison |>
    workflow_map(
        fn = "fit_resamples",
        verbose = T,
        seed = 69,
        resamples = validation_folds,
        control = cv_settings,
        metrics = model_metrics
    )

# We can use the convenience functions from before to collect performance metrics
# and what not.
model_comparison_predictions <-
    collect_predictions(model_comparison_cv) |>
    pivot_longer(cols = matches("total_prison"), names_to = "outcome", values_to = "value") |>
    filter(!is.na(value))

ggplot(model_comparison_predictions, aes(x = value, y = .pred)) +
    geom_point() +
    facet_wrap(~outcome+model) +
    theme_bw() +
    geom_abline(slope = 1, intercept = 0, color = "red")

model_comparison_metrics <-
    collect_metrics(model_comparison_cv) |>
    mutate(outcome = if_else(str_detect(wflow_id, "admissions"), "adm", "pop"))

ggplot(model_comparison_metrics, aes(x = model, y = mean)) +
    geom_point(aes(color = outcome)) +
    geom_errorbar(aes(ymin = mean - std_err, ymax = mean + std_err, color = outcome), width = 0.1) +
    facet_wrap(~.metric, scale = "free_y") +
    theme_bw()

################################################################################
# Now let's get into the true reason for the season. Cross-validation for
# hyperparameter tuning. Two general strategies are: 1) grid searches and 2)
# iterative search. Grid searches involve generating a set of hyperparameters
# and traversing through the space to find the optimal configuration. The main
# problems are: A) generating a representative space of hyperparameters (how
# do you know you've covered the entire terrain so to speak), and B) ensuring
# your space is parsimonious so you can efficiently travel through the space.
# On the other hand, you have iterative search which is usually where you use prior
# hyperparameter states to inform your current choice of hyperparameters. Usually,
# can be quite computationally expensive to do.


