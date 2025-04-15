library(dplyr)
library(purrr)
library(here)
library(readr)
library(stringr)
library(censusapi)
library(mlr)
library(cspp)
library(kknn)
library(xgboost)
library(ggplot2)
library(parallel)
library(reghelper)
library(ggcorrplot)
library(randomForest)
library(parallelMap)

datasets <- listCensusApis()
acs <- datasets %>% filter(str_detect(title, "ACS|American Community"), type == "Aggregate")
acs_unique <- acs %>% distinct(name, .keep_all = T) %>% filter(!str_detect(title, "3-Year"))
profiles_subjects <- acs %>% filter(name %in% c("acs/acs5/profile", "acs/acs5/subject"))
var_tables <- pmap(list(acs_unique$name, acs_unique$vintage), function(name, vintage) {listCensusMetadata(name = name, vintage = vintage, type = "variables")})
names(var_tables) <- acs_unique$name

################################################################################
# Variables where I only need the percent
################################################################################
prcnt_var_codes <-
    c(
        "DP03_0009", "DP03_0119", "DP03_0120", "DP03_0123",
        "DP03_0126", "DP03_0128", "DP03_0129", "DP03_0133"
    )
prcnt_estimates <- paste0(prcnt_var_codes, "PE")
prcnt_moe <- paste0(prcnt_var_codes, "PM")
prcnt_vars <- c(prcnt_estimates, prcnt_moe)

prcnt_var_names <-
    c(
        "unemployment", "family_poverty", "families_with_own_children_poverty", "married_families_with_own_children_poverty",
        "female_headed_alone_with_own_children_poverty", "individual_poverty", "under_18_poverty", "over_18_poverty"
        
    )
prcnt_estimate_names <- paste0(prcnt_var_names, "_prcnt")
prcnt_estimate_moe_names <- paste0(prcnt_var_names, "_prcnt_moe")
prcnt_names <- c(prcnt_estimate_names, prcnt_estimate_moe_names)
names(prcnt_vars) <- prcnt_names

################################################################################
# Variables where I only need the estimate.
################################################################################
est_var_codes <-
    c(
        "DP02_0001", "DP02_0016", "DP02_0017", "DP02_0025", "DP02_0031",
        "DP02_0037", "DP02_0039", "DP02_0040", "DP02_0041", "DP02_0042",
        "DP02_0043", "DP02_0044", "DP02_0059", "DP02_0069", "DP02_0071",
        "DP02_0079", "DP02_0088", "DP02_0112", "DP03_0001", "DP03_0008",
        "DP03_0010", "DP03_0014", "DP03_0016", "DP03_0062",
        "DP03_0063", "DP03_0065", "DP03_0067", "DP03_0069", "DP03_0071",
        "DP03_0073", "DP03_0075", "DP03_0086", "DP03_0087", "DP03_0092",
        "DP03_0093", "DP03_0094", "DP03_0100", "DP04_0001", "DP04_0004",
        "DP04_0005", "DP04_0048", "DP04_0049", "DP04_0110", "DP04_0116",
        "DP04_0117", "DP04_0125", "DP04_0136", "DP04_0143", "DP05_0004",
        "DP05_0018", "DP05_0028", "DP02_0038"
    )
est_estimates <- paste0(est_var_codes, "E")
est_moe <- paste0(est_var_codes, "M")
est_vars <- c(est_estimates, est_moe)

est_var_names <-
    c(
        "total_hh", "avg_hh", "avg_family", "male_15_and_older", "female_15_and_older",
        "female_15_to_50_birth", "births_per_1000_unmarried_women_15_to_50", "births_per_1000_women_15_to_50", "births_per_1000_women_15_to_19", "births_per_1000_women_20_to_34",
        "births_per_1000_women_35_to_50", "grandparents_living_with_grandchildren", "pop_25_and_older", "civ_pop_18_and_older", "pop_total_civilian_noninst",
        "pop_1_and_older", "total_population", "pop_5_and_older", "pop_16_and_older", "civilian_labor_force",
        "females_16_and_older", "hh_with_own_children_under_6", "hh_with_own_children_6_and_17", "median_hh_income",
        "mean_hh_income", "mean_earnings", "mean_ss", "mean_retirement", "mean_ssi",
        "mean_cash_public_asst", "total_families", "median_family_income", "mean_family_income", "median_earnings",
        "male_fulltime_median_earnings", "female_fulltime_median_earnings", "pop_total_civilian_noninst_under_19", "total_housing_units", "homeowner_vacancy_rate",
        "rental_vacancy_rate", "avg_hh_size_owner", "avg_hh_size_renter", "housing_units_with_mortage_exclude_noncomputed", "hu_with_mortage_costs_not_computed",
        "housing_units_without_mortage_exclude_noncomputed", "hu_without_mortage_costs_not_computed", "occupied_units_paying_rent_exclude_not_computed", "rent_not_computed", "sex_ratio",
        "median_age", "sex_ratio_over_18", "female_15_to_50_birth_unmarried"
    )
est_estimate_names <- paste0(est_var_names, "_nr")
est_estimate_moe_names <- paste0(est_var_names, "_nr_moe")
est_names <- c(est_estimate_names, est_estimate_moe_names)
names(est_vars) <- est_names

################################################################################
# Variables where I need the estimate and the percentage.
################################################################################
var_codes <-
    c(
        "DP02_0002", "DP02_0003", "DP02_0004", "DP02_0005", "DP02_0006",
        "DP02_0007", "DP02_0008", "DP02_0010", "DP02_0011", "DP02_0012",
        "DP02_0027", "DP02_0033", "DP02_0038", "DP02_0045", "DP02_0063",
        "DP02_0067", "DP02_0068", "DP02_0070", "DP02_0072", "DP02_0080", "DP02_0081",
        "DP02_0089", "DP02_0094", "DP02_0096", "DP02_0097", "DP02_0113", "DP03_0002",
        "DP03_0011", "DP03_0015", "DP03_0017", "DP03_0052", "DP03_0053",
        "DP03_0054", "DP03_0055", "DP03_0056", "DP03_0057", "DP03_0058",
        "DP03_0059", "DP03_0060", "DP03_0061", "DP03_0064", "DP03_0066",
        "DP03_0068", "DP03_0070", "DP03_0072", "DP03_0074", "DP03_0076",
        "DP03_0077", "DP03_0078", "DP03_0079", "DP03_0080", "DP03_0081",
        "DP03_0082", "DP03_0083", "DP03_0084", "DP03_0085", "DP03_0096",
        "DP03_0097", "DP03_0101", "DP04_0002", "DP04_0003", "DP04_0046",
        "DP04_0047", "DP04_0051", "DP04_0052", "DP04_0053", "DP04_0054",
        "DP04_0055", "DP04_0056", "DP04_0073", "DP04_0074", "DP04_0075",
        "DP04_0077", "DP04_0078", "DP04_0079", "DP04_0111", "DP04_0112",
        "DP04_0113", "DP04_0114", "DP04_0115", "DP04_0118", "DP04_0119",
        "DP04_0120", "DP04_0121", "DP04_0122", "DP04_0123", "DP04_0124",
        "DP04_0137", "DP04_0138", "DP04_0139", "DP04_0140", "DP04_0141",
        "DP04_0142", "DP05_0002", "DP05_0003", "DP05_0021", "DP05_0026",
        "DP05_0027", "DP05_0076", "DP05_0082", "DP05_0083", "DP05_0084",
        "DP05_0085", "DP05_0086", "DP05_0087", "DP05_0088"
    )
estimates <- paste0(var_codes, "E")
estimate_moes <- paste0(var_codes, "M")
percents <- paste0(var_codes, "PE")
percent_moes <- paste0(var_codes, "PM")
vars <- c(estimates, estimate_moes, percents, percent_moes)

var_names <-
    c(
        "married_hh", "marriedWithChildren_hh", "cohabit_hh", "cohabitWithChildren_hh", "maleNoPartner_hh",
        "maleNoPartnerWithChildren_hh", "maleNoPartnerAlone_hh", "femaleNoPartner_hh", "femaleNoPartnerWithChildren_hh", "femaleNoPartnerAlone_hh",
        "male_15_and_older_married", "female_15_and_older_married", "female_15_to_50_birth_unmarried", "grandparents_responsible_for_grandchildren", "some_college_no_degree",
        "hs_or_higher", "bachelor_or_higher", "veterans", "disability", "lived_in_same_house_1", "not_lived_in_same_house_1",
        "native_born", "foreign_born", "foreign_born_citizen", "foreign_born_not_citizen", "speaks_english_only", "labor_force",
        "female_labor_force", "all_parents_in_labor_force_with_children_under_6", "all_parents_in_labor_force_with_children_6_and_17", "hh_income_less_than_10k", "hh_income_10k_15k",
        "hh_income_15k_25k", "hh_income_25k_35k", "hh_income_35k_50k", "hh_income_50k_75k", "hh_income_75k_100k",
        "hh_income_100k_150k", "hh_income_150k_200k", "hh_income_200k_more", "hh_with_earnings", "hh_with_ss",
        "hh_with_retirement", "hh_with_ssi", "hh_with_cash_public_asst", "hh_with_snap", "family_income_less_than_10k",
        "family_income_10k_15k", "family_income_15k_25k", "family_income_25k_35k", "family_income_35k_50k", "family_income_50k_75k",
        "family_income_75k_100k", "family_income_100k_150k", "family_income_150k_200k", "family_income_200k_more", "health_insurance",
        "no_health_insurance", "no_health_insurance_under_19", "occupied_housing_units", "vacant_housing_units",  "owner_occupied",
        "renter_occupied", "moved_in_2021_or_later", "moved_in_2018_2020", "moved_in_2010_2017", "moved_in_2000_2009",
        "moved_in_1990_1999", "moved_in_1989_or_earlier", "lacks_complete_plumbing", "lacks_complete_kitchen", "lacks_telephone",
        "one_occupant_per_room_or_less", "one_to_one_point_five_per_room", "more_than_one_point_five_per_room", "hu_with_mortage_costs_less_than_20_prcnt_income", "hu_with_mortage_costs_20_to_25_prcnt_income",
        "hu_with_mortage_costs_25_to_30_prcnt_income", "hu_with_mortage_costs_30_to_35_prcnt_income", "hu_with_mortage_costs_more_than_35_prcnt_income", "hu_without_mortage_costs_less_than_10_prcnt_income", "hu_without_mortage_costs_10_to_15_prcnt_income",
        "hu_without_mortage_costs_15_to_20_prcnt_income", "hu_without_mortage_costs_20_to_25_prcnt_income", "hu_without_mortage_costs_25_to_30_prcnt_income", "hu_without_mortage_costs_30_to_35_prcnt_income", "hu_without_mortage_costs_more_than_35_prcnt",
        "rent_less_than_15_prcnt_income", "rent_15_to_20_prcnt_income", "rent_20_to_25_prcnt_income", "rent_25_to_30_prcnt_income", "rent_30_to_35_prcnt_income",
        "rent_more_than_35_prcnt_income", "male", "female", "under_18", "male_over_18",
        "female_over_18", "hispanic", "white_alone_not_hispanic", "black_alone_not_hispanic", "aian_alone_not_hispanic",
        "asian_alone_not_hispanic", "nhopi_alone_not_hispanic", "other_alone_not_hispanic", "multiracial_not_hispanic"
    )
estimate_names <- paste0(var_names, "_nr")
estimate_moe_names <- paste0(var_names, "_nr_moe")
prcnt_names <- paste0(var_names, "_prcnt")
prcnt_moe_names <- paste0(var_names, "_prcnt_moe")
names <- c(estimate_names, estimate_moe_names, prcnt_names, prcnt_moe_names)
names(vars) <- names

################################################################################
# Grab estimates from the subject tables.
################################################################################
subject_var_codes <-
    c(
        "S0101_C01_023", "S0101_C02_023", "S0101_C03_023", "S0101_C04_023",
        "S0101_C05_023", "S0101_C06_023", "S0901_C01_001", "S0901_C01_015E",
        "S0901_C01_016", "S0901_C01_017", "S0901_C01_018", "S0901_C01_021",
        "S0901_C01_024", "S0901_C01_025", "S0901_C01_028", "S0901_C01_031",
        "S0901_C01_032", "S0901_C01_033", "S0901_C01_034", "S0901_C01_036",
        "S0901_C01_037", "S0901_C02_001", "S0901_C03_001", "S0901_C04_001",
        "S0902_C01_001", "S0902_C01_017", "S1301_C01_001", "S1301_C01_002",
        "S1301_C01_003", "S1301_C01_004", "S1301_C02_002", "S1301_C02_003",
        "S1301_C02_004", "S1301_C03_002", "S1301_C03_003", "S1301_C03_004",
        "S1001_C01_001", "S1001_C02_001", "S1001_C03_001"
    )
subject_estimates <- paste0(subject_var_codes, "E")
subject_moe <- paste0(subject_var_codes, "M")
subject_vars <- c(subject_estimates, subject_moe)

subject_var_names <-
    c(
        "pop_18_to_24_nr", "pop_18_to_24_prcnt", "male_18_to_24_nr", "male_18_to_24_prcnt",
        "female_18_to_24_nr", "female_18_to_24_prcnt", "children_under_18_in_hh_nr", "child_lives_with_parent_prcnt",
        "child_lives_with_grandparent_prcnt", "child_lives_with_other_relative_prcnt", "foster_child_or_unrelated_prcnt", "unmarried_partner_present_prcnt",
        "children_3_to_17_in_hh_nr", "children_3_to_17_enrolled_school_nr", "children_3_to_17_not_enrolled_school_nr", "children_living_in_hh_ssi_asst_snap_prcnt",
        "children_in_hh_poverty_calculated_nr", "children_in_poverty_prcnt", "children_not_in_poverty_prcnt", "children_in_owner_occupied_hu_prcnt",
        "children_in_renter_occupied_hu_prcnt", "children_under_18_in_married_hh_nr", "children_under_18_in_malealone_hh_nr", "children_under_18_in_femalealone_hh_nr",
        "pop_15_to_19_nr", "pop_15_to_19_not_work_school_prcnt", "female_15_to_50_nr", "female_15_to_19_nr",
        "female_20_to_34_nr", "female_35_to_50_nr", "female_15_to_19_births_nr", "female_20_to_34_births_nr",
        "female_35_to_50_births_nr", "female_15_to_19_births_prcnt", "female_20_to_34_births_prcnt", "female_35_to_50_births_prcnt",
        "grandchildren_living_with_grandparent_householder_nr", "grandchildren_living_with_grandparent_householder_responsible_nr", "grandchildren_living_with_grandparent_householder_responsible_no_parent_nr"
    )
subject_name_moes <- paste0(subject_var_names, "_moe")
subject_names <- c(subject_var_names, subject_name_moes)
names(subject_vars) <- subject_names

################################################################################
# Grab estimates.
################################################################################
all_vars <- c(prcnt_vars, est_vars, vars)

download_data <- function(year, var_names) {
    test <-
        getCensus(
            name = "acs/acs5/profile",
            vintage = year,
            vars = all_vars,
            region = "county:*"
        ) %>%
        rename(all_of(var_names))
    
    test2 <-
        test %>%
        select(
            state, county, matches("prcnt"), -matches("moe"), -matches("nr$"),
            -matches("moved_in")
        ) %>%
        select(
            -not_lived_in_same_house_1_prcnt, -native_born_prcnt, -foreign_born_citizen_prcnt,
            -health_insurance_prcnt, -occupied_housing_units_prcnt, -renter_occupied_prcnt, -female_prcnt
        ) %>%
        mutate(year = year)
}

acs_data <-
    map(
        2017:2021,
        download_data,
        var_names = all_vars
    ) %>%
    bind_rows()

acs_data_missing <-
    acs_data %>%
    mutate(
        across(-matches("state|county|year"), function(col) {if_else(col < 0, NA_real_, col)}),
        year = year - 5,
        full_fips = paste0(state, county)
    )

incarceration_data <-
    read_csv(
        here("..", "incarceration", "2_visualize_chrr_wphi", "input", "incarceration_trends_vera.csv.gz")
    ) %>%
    mutate(
        full_fips = if_else(str_length(fips) == 4, paste0("0", fips), as.character(fips))
    ) %>%
    select(full_fips, year, total_prison_pop_rate)

acs_incarceration_data <-
    inner_join(
        acs_data_missing,
        incarceration_data
    )

missings <- map(acs_incarceration_data, ~sum(is.na(.x)))
missingsPrcnt <- unlist(missings) / nrow(acs_incarceration_data) * 100
missing_state <- table(acs_incarceration_data$state, is.na(acs_incarceration_data$total_prison_pop_rate))

acs_incarceration_data_no_missing <-
    acs_incarceration_data %>%
    select(-nhopi_alone_not_hispanic_prcnt, -county) %>%
    filter(!is.na(total_prison_pop_rate)) %>%
    mutate(state = as.factor(state), full_fips = as.factor(full_fips), year = as.factor(year))

missings <- map(acs_incarceration_data_no_missing, ~sum(is.na(.x)))
missingsPrcnt <- unlist(missings) / nrow(acs_incarceration_data_no_missing) * 100
missing_state <- table(acs_incarceration_data_no_missing$state, is.na(acs_incarceration_data_no_missing$total_prison_pop_rate))

set.seed(42)
train <- acs_incarceration_data_no_missing %>% sample_frac(0.70)
test <- anti_join(acs_incarceration_data_no_missing, train)

imputeMethod <- imputeLearner("regr.rpart")
trainImputed <- impute(as.data.frame(select(train, -state, -year, -full_fips)),
                       classes = list(numeric = imputeMethod))
testImputed <- reimpute(test, trainImputed$desc)

incarcerationTaskTrain <-
    makeRegrTask(data = trainImputed$data,
                 target = "total_prison_pop_rate")

knn <- makeLearner("regr.kknn")
forest <- makeLearner("regr.randomForest", importance = T)
xgb <- makeLearner("regr.xgboost")

# We will be using 10-fold cross-validation for hyperparameter tuning
kFold10 <- makeResampleDesc("CV", iters = 10)

# For kNN, we will search across the entire parameter space
gridSearch <- makeTuneControlGrid()

# Tree-based algos will explore 100 different combinations of hyperparameters
randSearch <- makeTuneControlRandom(maxit = 100)

## Tune the hyperparameters for kNN

#k-nearest neighbors only has one parameter to tune (k or the number of neighbors to consider).

# I'll also note we will be using **RMSE** (Root Mean Squared Error) to evaluate
# the performance of our hyperparameters (less susceptible to the influence of outliers). 
# I'll also be reporting **MAE** (Mean Absolute Error) for fun.
knnParamSpace <- makeParamSet(makeDiscreteParam("k", values = 1:10))

tunedKnn <- tuneParams(knn,
                       task = incarcerationTaskTrain,
                       resampling = kFold10,
                       par.set = knnParamSpace,
                       control = gridSearch,
                       measures = list(rmse, mae))

tunedKnn

# Did we find a local or global minimum for our hyperparameter?
knnTuningData <- generateHyperParsEffectData(tunedKnn)

plotHyperParsEffect(knnTuningData, x = "k",
                    y = "rmse.test.rmse",
                    plot.type = "line") +
    theme_bw()

## Tune the hyperparameters for random forest
forestParamSpace <-
    makeParamSet(makeIntegerParam("ntree", lower = 100, upper = 100),
                 makeIntegerParam("mtry", lower = 5, upper = 10),
                 makeIntegerParam("nodesize", lower = 1, upper = 5),
                 makeIntegerParam("maxnodes", lower = 5, upper = 10))

ptm <- proc.time()

parallelStartSocket(cpus = detectCores())

tunedRandomForest <- tuneParams(forest,
                                task = incarcerationTaskTrain,
                                resampling = kFold10,
                                par.set = forestParamSpace,
                                control = randSearch,
                                measures = list(rmse, mae))

parallelStop()

proc.time() - ptm

tunedRandomForest

## Tune the hyperparameters for XGBoost
xgbParamSpace <-
    makeParamSet(makeNumericParam("eta", lower = 0, upper = 1),
                 makeNumericParam("gamma", lower = 0, upper = 10),
                 makeIntegerParam("max_depth", lower = 1, upper = 20),
                 makeNumericParam("min_child_weight", lower = 1, upper = 10),
                 makeNumericParam("subsample", lower = 0.5, upper = 1),
                 makeNumericParam("colsample_bytree", lower = 0.5, upper = 1),
                 makeIntegerParam("nrounds", lower = 50, upper = 50))

ptm <- proc.time()

tunedXgb <- tuneParams(xgb,
                       task = incarcerationTaskTrain,
                       resampling = kFold10,
                       par.set = xgbParamSpace,
                       control = randSearch,
                       measures = list(rmse, mae))

proc.time() - ptm

tunedXgb

# Train random forest and XGBoost models (and OLS for comparison)
# We are also going to check and see if the RMSE converges with the number of trees trained.

# train our OLS regression
trainedOls <- lm(total_prison_pop_rate ~ .,
                 data = trainImputed$data)

# set hyperparameter for k-nearest neighbor
tunedKnnPars <- setHyperPars(knn, par.vals = tunedKnn$x)
trainedKnn <- train(tunedKnnPars, incarcerationTaskTrain)

# set hyperparameters and train the random forest
tunedRandomForestPars <- setHyperPars(forest, par.vals = tunedRandomForest$x)
trainedRandomForest <- train(tunedRandomForestPars, incarcerationTaskTrain)

# set hyperparameters and train xgboost
tunedXgbPars <- setHyperPars(xgb, par.vals = tunedXgb$x)
trainedXgb <- train(tunedXgbPars, incarcerationTaskTrain)

# Check to see if our MSE converges for random forest
randomForestData <- trainedRandomForest$learner.model
plot(randomForestData)

# Check to see if our MSE converges for XGBoost
xgbData <- trainedXgb$learner.model
ggplot(xgbData$evaluation_log, aes(iter, train_rmse)) +
    geom_line() +
    geom_point() +
    theme_bw()

# See how well our models do predicting out of sample on the test set.
predictOls <-
    as_tibble(predict(trainedOls,
                      newdata = testImputed)) %>%
    bind_cols(testImputed) %>%
    mutate(error = value - total_prison_pop_rate,
           error_sq = error ^ 2,
           error_abs = abs(error),
           sum_abs_error = sum(error_abs),
           mae = sum_abs_error / n(),
           sum_sq_error = sum(error_sq),
           rmse = sqrt(sum_sq_error / n()))

predictKnn <-
    as_tibble(predict(trainedKnn,
                      newdata = testImputed)) %>%
    mutate(error = truth - response,
           error_sq = error ^ 2,
           error_abs = abs(error),
           sum_abs_error = sum(error_abs),
           mae = sum_abs_error / n(),
           sum_sq_error = sum(error_sq),
           rmse = sqrt(sum_sq_error / n()))

predictRandomForest <-
    as_tibble(predict(trainedRandomForest,
                      newdata = testImputed)) %>%
    mutate(error = truth - response,
           error_sq = error ^ 2,
           error_abs = abs(error),
           sum_abs_error = sum(error_abs),
           mae = sum_abs_error / n(),
           sum_sq_error = sum(error_sq),
           rmse = sqrt(sum_sq_error / n()))

predictXgb <-
    as_tibble(predict(trainedXgb,
                      newdata = select(testImputed, -state, -year, -full_fips, -total_prison_pop_rate))) %>%
    mutate(truth = testImputed$total_prison_pop_rate) %>%
    mutate(error = truth - response,
           error_sq = error ^ 2,
           error_abs = abs(error),
           sum_abs_error = sum(error_abs),
           mae = sum_abs_error / n(),
           sum_sq_error = sum(error_sq),
           rmse = sqrt(sum_sq_error / n()))

cat("RMSE\n")
cat("ols: ", unique(predictOls$rmse), "\n")
cat("knn: ", unique(predictKnn$rmse), "\n")
cat("random forest: ", unique(predictRandomForest$rmse), "\n")
cat("xgb: ", unique(predictXgb$rmse), "\n")
cat("MAE\n")
cat("ols: ", unique(predictOls$mae), "\n")
cat("knn: ", unique(predictKnn$mae), "\n")
cat("random forest: ", unique(predictRandomForest$mae), "\n")
cat("xgb: ", unique(predictXgb$mae), "\n")

# Let's visualize the predictions and how well they line up with the actual values
ggplot(predictOls, aes(x = total_prison_pop_rate, y = value)) +
    geom_point() +
    geom_abline(intercept = 0, slope = 1) +
    theme_bw() +
    labs(title = "OLS predicted vs. truth")

ggplot(predictKnn, aes(x = truth, y = response)) +
    geom_point() +
    geom_abline(intercept = 0, slope = 1) +
    theme_bw() +
    labs(title = "kNN predicted vs. truth")

ggplot(predictRandomForest, aes(x = truth, y = response)) +
    geom_point() +
    geom_abline(intercept = 0, slope = 1) +
    theme_bw() +
    labs(title = "Random Forest predicted vs. truth")

ggplot(predictXgb, aes(x = truth, y = response)) +
    geom_point() +
    geom_abline(intercept = 0, slope = 1) +
    theme_bw() +
    labs(title = "Xgb predicted vs. truth")

# See which features were most important in random forest vs. xgboost

# I standardized the coefficients in the OLS regression model to facilitate coefficient comparison.
# By standardizing the coefficients, I mean to say I divided by each variable's standard deviation.
# Coefficients should be interpreted as standard deviation increases rather than unit increases.
# Random Forest for regression reports two measures of variable importance;
    # The first measure captures how much the prediction error or accuracy suffers
    # when the values for that variable are randomly permuted (accuracy as measured by mean squared error).
    # You can train a random forest to minimize MAE as well, not sure how to do this in R.
    # The second measure captures how well the variable splits nodes or decreases node impurities.
    # For regressions, this captures how well the variable minimizes the sum of squared residuals
    # (the numerator in mean squared error) when splitting.
# Xgboost reports variable importance as the improvement in accuracy brought by that feature in the branches which use it.
# The idea being that adding a new split using the variable in question made your predictions more accurate than before.
# It's calculated by looking at each feature's contribution from each tree in the model.

randomForestFeaturesGini <-
    getFeatureImportance(trainedRandomForest)$res %>%
    mutate(model = "randomForestNode")

randomForestFeaturesAcc <-
    getFeatureImportance(trainedRandomForest, type = 1)$res %>%
    mutate(model = "randomForestAcc")

xgboostFeatures <-
    getFeatureImportance(trainedXgb)$res %>%
    mutate(model = "xgboost")

vars <- gsub(".z", "", rownames(beta(trainedOls, y = F)$coefficients))
olsStdFeatures <-
    beta(trainedOls, y = F)$coefficients %>%
    as_tibble() %>%
    mutate(variable = vars,
           model = "olsStdz",
           Estimate = abs(Estimate)) %>%
    filter(variable != "(Intercept)") %>%
    select(Estimate, variable, model) %>%
    rename(importance = Estimate)

features <- bind_rows(randomForestFeaturesGini,
                      xgboostFeatures,
                      randomForestFeaturesAcc,
                      olsStdFeatures)

ggplot(features, aes(x = reorder(variable, importance), y = importance)) +
    geom_bar(stat = "identity") +
    facet_wrap(~ model, scales = "free_x") +
    coord_flip() +
    theme_bw()
