library(here)
library(readr)
library(dplyr)
library(purrr)
library(R.utils)

read_dir <- here("1_get_data", "output")
save_dir <- here("2_clean_data", "output")
acs_df <- read_csv(file.path(read_dir, "acs_county.csv.gz"))

################################################################################
# Clean educational data.
df_edu <-
    acs_df |>
    mutate(
        acs_lessThanHs_prcnt_est_25older_b =
            if_else(totalBlack25AndOlder != 0, (maleBlackLessThanHS + femaleBlackLessThanHS) / totalBlack25AndOlder, 0),
        acs_hs_prcnt_est_25older_b =
            if_else(totalBlack25AndOlder != 0, (maleBlackHS + femaleBlackHS) / totalBlack25AndOlder, 0),
        acs_someCollege_prcnt_est_25older_b =
            if_else(totalBlack25AndOlder != 0, (maleBlackSomeCollege + femaleBlackSomeCollege) / totalBlack25AndOlder, 0),
        acs_college_prcnt_est_25older_b = 
            if_else(totalBlack25AndOlder != 0, (maleBlackCollege + femaleBlackCollege) / totalBlack25AndOlder, 0),
        acs_lessThanHs_prcnt_est_25older_h =
            if_else(totalHisp25AndOlder != 0, (maleHispLessThanHS + femaleHispLessThanHS) / totalHisp25AndOlder, 0),
        acs_hs_prcnt_est_25older_h =
            if_else(totalHisp25AndOlder != 0, (maleHispHS + femaleHispHS) / totalHisp25AndOlder, 0),
        acs_someCollege_prcnt_est_25older_h =
            if_else(totalHisp25AndOlder != 0, (maleHispSomeCollege + femaleHispSomeCollege) / totalHisp25AndOlder, 0),
        acs_college_prcnt_est_25older_h = 
            if_else(totalHisp25AndOlder == 0, (maleHispCollege + femaleHispCollege) / totalHisp25AndOlder, 0),
        acs_lessThanHs_prcnt_est_25older_w =
            if_else(totalWhite25AndOlder == 0, (maleWhiteLessThanHS + femaleWhiteLessThanHS) / totalWhite25AndOlder, 0),
        acs_hs_prcnt_est_25older_w =
            if_else(totalWhite25AndOlder == 0, (maleWhiteHS + femaleWhiteHS) / totalWhite25AndOlder, 0),
        acs_someCollege_prcnt_est_25older_w =
            if_else(totalWhite25AndOlder == 0, (maleWhiteSomeCollege + femaleWhiteSomeCollege) / totalWhite25AndOlder, 0),
        acs_college_prcnt_est_25older_w = 
            if_else(totalWhite25AndOlder == 0, (maleWhiteCollege + femaleWhiteCollege) / totalWhite25AndOlder, 0),
        acs_blackToWhiteCollege_ratio_est =
            if_else(acs_college_prcnt_est_25older_w == 0, acs_college_prcnt_est_25older_b / acs_college_prcnt_est_25older_w, 0),
        acs_hispToWhiteCollege_ratio_est =
            if_else(acs_college_prcnt_est_25older_w == 0, acs_college_prcnt_est_25older_h / acs_college_prcnt_est_25older_w, 0)
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*prcnt|ratio"))

################################################################################
# Clean poverty data.
df_poverty <-
    acs_df |>
    mutate(
        acs_inPoverty_prcnt_est_allAges_b =
            if_else(black_pop_povertyStatusCalculable != 0, black_inPoverty / black_pop_povertyStatusCalculable, 0),
        acs_inPoverty_prcnt_est_allAges_w =
            if_else(white_pop_povertyStatusCalculable != 0, white_inPoverty / white_pop_povertyStatusCalculable, 0),
        acs_inPoverty_prcnt_est_allAges_h =
            if_else(hisp_pop_povertyStatusCalculable != 0, hisp_inPoverty / hisp_pop_povertyStatusCalculable, 0),
        acs_blackToWhitePoverty_ratio_est =
            if_else(acs_inPoverty_prcnt_est_allAges_w != 0, acs_inPoverty_prcnt_est_allAges_b / acs_inPoverty_prcnt_est_allAges_w, 0),
        acs_hispToWhitePoverty_ratio_est =
            if_else(acs_inPoverty_prcnt_est_allAges_w != 0, acs_inPoverty_prcnt_est_allAges_h / acs_inPoverty_prcnt_est_allAges_w, 0)
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*prcnt|ratio"))

################################################################################
# Clean income, poverty, and welfare data.
df_income_black <-
    acs_df |>
    mutate(
        acs_hhIncomeBelow10_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, black_below10 / total_nr_black_hh, 0),
        acs_hhIncome10to15_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, black_from10to15 / total_nr_black_hh, 0),
        acs_hhIncome15to20_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, black_from15to20 / total_nr_black_hh, 0),
        acs_hhIncome20to25_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, black_from20to25 / total_nr_black_hh, 0),
        acs_hhIncome25to30_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, black_from25to30 / total_nr_black_hh, 0),
        acs_hhIncome30to35_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, black_from30to35 / total_nr_black_hh, 0),
        acs_hhIncome35to40_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, black_from35to40 / total_nr_black_hh, 0),
        acs_hhIncome40to50_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, (black_from40to45 + black_from45to50) / total_nr_black_hh, 0),
        acs_hhIncome50to75_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, (black_from50to60 + black_from60to75) / total_nr_black_hh, 0),
        acs_hhIncome75to100_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, black_from75to100 / total_nr_black_hh, 0),
        acs_hhIncome100to125_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, black_from100to125 / total_nr_black_hh, 0),
        acs_hhIncome125to150_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, black_from125to150 / total_nr_black_hh, 0),
        acs_hhIncomeAbove150_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, (black_from150to200 + black_above200) / total_nr_black_hh, 0),
        acs_hhIncomeAbove75_prcnt_est_allAges_b =
            if_else(
                total_nr_black_hh != 0,
                (black_from75to100 + black_from100to125 + black_from125to150 + black_from150to200 + black_above200) / total_nr_black_hh,
                0
            )
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*prcnt|ratio"))

df_income_white <-
    acs_df |>
    mutate(
        acs_hhIncomeBelow10_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, white_below10 / total_nr_white_hh, 0),
        acs_hhIncome10to15_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, white_from10to15 / total_nr_white_hh, 0),
        acs_hhIncome15to20_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, white_from15to20 / total_nr_white_hh, 0),
        acs_hhIncome20to25_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, white_from20to25 / total_nr_white_hh, 0),
        acs_hhIncome25to30_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, white_from25to30 / total_nr_white_hh, 0),
        acs_hhIncome30to35_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, white_from30to35 / total_nr_white_hh, 0),
        acs_hhIncome35to40_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, white_from35to40 / total_nr_white_hh, 0),
        acs_hhIncome40to50_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, (white_from40to45 + white_from45to50) / total_nr_white_hh, 0),
        acs_hhIncome50to75_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, (white_from50to60 + white_from60to75) / total_nr_white_hh, 0),
        acs_hhIncome75to100_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, white_from75to100 / total_nr_white_hh, 0),
        acs_hhIncome100to125_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, white_from100to125 / total_nr_white_hh, 0),
        acs_hhIncome125to150_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, white_from125to150 / total_nr_white_hh, 0),
        acs_hhIncomeAbove150_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, (white_from150to200 + white_above200) / total_nr_white_hh, 0),
        acs_hhIncomeAbove75_prcnt_est_allAges_w =
            if_else(
                total_nr_white_hh != 0,
                (white_from75to100 + white_from100to125 + white_from125to150 + white_from150to200 + white_above200) / total_nr_white_hh,
                0
            )
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*prcnt|ratio"))

df_income_hisp <-
    acs_df |>
    mutate(
        acs_hhIncomeBelow10_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, hisp_below10 / total_nr_hisp_hh, 0),
        acs_hhIncome10to15_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, hisp_from10to15 / total_nr_hisp_hh, 0),
        acs_hhIncome15to20_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, hisp_from15to20 / total_nr_hisp_hh, 0),
        acs_hhIncome20to25_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, hisp_from20to25 / total_nr_hisp_hh, 0),
        acs_hhIncome25to30_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, hisp_from25to30 / total_nr_hisp_hh, 0),
        acs_hhIncome30to35_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, hisp_from30to35 / total_nr_hisp_hh, 0),
        acs_hhIncome35to40_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, hisp_from35to40 / total_nr_hisp_hh, 0),
        acs_hhIncome40to50_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, (hisp_from40to45 + hisp_from45to50) / total_nr_hisp_hh, 0),
        acs_hhIncome50to75_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, (hisp_from50to60 + hisp_from60to75) / total_nr_hisp_hh, 0),
        acs_hhIncome75to100_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, hisp_from75to100 / total_nr_hisp_hh, 0),
        acs_hhIncome100to125_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, hisp_from100to125 / total_nr_hisp_hh, 0),
        acs_hhIncome125to150_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, hisp_from125to150 / total_nr_hisp_hh, 0),
        acs_hhIncomeAbove150_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, (hisp_from150to200 + hisp_above200) / total_nr_hisp_hh, 0),
        acs_hhIncomeAbove75_prcnt_est_allAges_h =
            if_else(
                total_nr_hisp_hh != 0,
                (hisp_from75to100 + hisp_from100to125 + hisp_from125to150 + hisp_from150to200 + hisp_above200) / total_nr_hisp_hh,
                0
            )
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*prcnt|ratio"))

df_median_income <-
    acs_df |>
    mutate(
        acs_hhIncome_median_est_allAges_b = if_else(black_medianHHIncome < 0, NA, black_medianHHIncome),
        acs_hhIncome_median_est_allAges_w = if_else(white_medianHHIncome < 0, NA, white_medianHHIncome),
        acs_hhIncome_median_est_allAges_h = if_else(hisp_medianHHIncome < 0, NA, hisp_medianHHIncome),
        acs_medianHhIncomeBlackToWhite_ratio_est = acs_hhIncome_median_est_allAges_b / acs_hhIncome_median_est_allAges_w,
        acs_medianHhIncomeHispToWhite_ratio_est = acs_hhIncome_median_est_allAges_h / acs_hhIncome_median_est_allAges_w
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*median"))

df_welfare <-
    acs_df |>
    mutate(acs_hhReceiveCpaSnap_prcnt_est = total_nr_hh_publicAsst_Snap / total_nr_hh) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*prcnt"))

df_income_inequality <-
    acs_df |>
    mutate(
        acs_hhShareOfIncome_1stQuintile_est = shareOfIncome_lowest_quintile,
        acs_hhShareOfIncome_2ndQuintile_est = shareOfIncome_second_quintile,
        acs_hhShareOfIncome_3rdQuintile_est = shareOfIncome_third_quintile,
        acs_hhShareOfIncome_4thQuintile_est = shareOfIncome_fourth_quintile,
        acs_hhShareOfIncome_5thQuintile_est = shareOfIncome_fifth_quintile,
        acs_hhShareOfIncome_top5Prcnt_est = shareOfIncome_top5prcnt,
        acs_giniCoefficient_index_est = gini_coefficient
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

df_snap <-
    acs_df |>
    mutate(
        acs_hhSnap_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, blackHH_Snap / total_nr_black_hh, 0),
        acs_hhSnap_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, whiteHH_Snap / total_nr_white_hh, 0),
        acs_hhSnap_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, hispHH_Snap / total_nr_hisp_hh, 0),
        acs_hhSnapBlackToWhite_ratio_est =
            if_else(acs_hhSnap_prcnt_est_allAges_w != 0, acs_hhSnap_prcnt_est_allAges_b / acs_hhSnap_prcnt_est_allAges_w, 0),
        acs_hhSnapHispToWhite_ratio_est =
            if_else(acs_hhSnap_prcnt_est_allAges_w != 0, acs_hhSnap_prcnt_est_allAges_h / acs_hhSnap_prcnt_est_allAges_w, 0)
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

################################################################################
# Clean labor data.
df_labor <-
    acs_df |>
    mutate(
        nr_employed_black =
            black_employed_male_16to64 + black_employed_male_65andOlder +
            black_employed_female_16to64 + black_employed_female_65andOlder,
        nr_unemployed_black =
            black_unemployed_male_16to64 + black_unemployed_male_65andOlder +
            black_unemployed_female_16to64 + black_unemployed_female_65andOlder,
        nr_civilianLaborForceBlack =
            black_unemployed_male_16to64 + black_unemployed_male_65andOlder +
            black_unemployed_female_16to64 + black_unemployed_female_65andOlder +
            black_employed_male_16to64 + black_employed_male_65andOlder +
            black_employed_female_16to64 + black_employed_female_65andOlder,
        nr_total_blackCivilian_16andOlder_pop =  black_pop_male_16andOlder + black_pop_female_16andOlder,
        acs_ur_prcnt_est_16andOlder_b = if_else(nr_civilianLaborForceBlack != 0, nr_unemployed_black / nr_civilianLaborForceBlack, 0),
        acs_lfpr_prcnt_est_16andOlder_b =
            if_else(nr_total_blackCivilian_16andOlder_pop != 0, nr_civilianLaborForceBlack / nr_total_blackCivilian_16andOlder_pop, 0),
        acs_epr_prcnt_est_16andOlder_b =
            if_else(nr_total_blackCivilian_16andOlder_pop != 0, nr_employed_black/ nr_total_blackCivilian_16andOlder_pop, 0),
        nr_employed_white =
            white_employed_male_16to64 + white_employed_male_65andOlder +
            white_employed_female_16to64 + white_employed_female_65andOlder,
        nr_unemployed_white =
            white_unemployed_male_16to64 + white_unemployed_male_65andOlder +
            white_unemployed_female_16to64 + white_unemployed_female_65andOlder,
        nr_civilianLaborForceWhite =
            white_unemployed_male_16to64 + white_unemployed_male_65andOlder +
            white_unemployed_female_16to64 + white_unemployed_female_65andOlder +
            white_employed_male_16to64 + white_employed_male_65andOlder +
            white_employed_female_16to64 + white_employed_female_65andOlder,
        nr_total_whiteCivilian_16andOlder_pop =  white_pop_male_16andOlder + white_pop_female_16andOlder,
        acs_ur_prcnt_est_16andOlder_w = if_else(nr_civilianLaborForceWhite != 0, nr_unemployed_white / nr_civilianLaborForceWhite, 0),
        acs_lfpr_prcnt_est_16andOlder_w =
            if_else(nr_total_whiteCivilian_16andOlder_pop != 0, nr_civilianLaborForceWhite / nr_total_whiteCivilian_16andOlder_pop, 0),
        acs_epr_prcnt_est_16andOlder_w =
            if_else(nr_total_whiteCivilian_16andOlder_pop != 0, nr_employed_white/ nr_total_whiteCivilian_16andOlder_pop, 0),
        nr_employed_hisp =
            hisp_employed_male_16to64 + hisp_employed_male_65andOlder +
            hisp_employed_female_16to64 + hisp_employed_female_65andOlder,
        nr_unemployed_hisp =
            hisp_unemployed_male_16to64 + hisp_unemployed_male_65andOlder +
            hisp_unemployed_female_16to64 + hisp_unemployed_female_65andOlder,
        nr_civilianLaborForceHisp =
            hisp_unemployed_male_16to64 + hisp_unemployed_male_65andOlder +
            hisp_unemployed_female_16to64 + hisp_unemployed_female_65andOlder +
            hisp_employed_male_16to64 + hisp_employed_male_65andOlder +
            hisp_employed_female_16to64 + hisp_employed_female_65andOlder,
        nr_total_hispCivilian_16andOlder_pop =  hisp_pop_male_16andOlder + hisp_pop_female_16andOlder,
        acs_ur_prcnt_est_16andOlder_h = if_else(nr_civilianLaborForceHisp != 0, nr_unemployed_hisp / nr_civilianLaborForceHisp, 0),
        acs_lfpr_prcnt_est_16andOlder_h =
            if_else(nr_total_hispCivilian_16andOlder_pop != 0, nr_civilianLaborForceHisp / nr_total_hispCivilian_16andOlder_pop, 0),
        acs_epr_prcnt_est_16andOlder_h =
            if_else(nr_total_hispCivilian_16andOlder_pop != 0, nr_employed_hisp/ nr_total_hispCivilian_16andOlder_pop, 0),
        acs_lfprBlackToWhite_ratio_est_16andOlder =
            if_else(acs_lfpr_prcnt_est_16andOlder_w != 0, acs_lfpr_prcnt_est_16andOlder_b / acs_lfpr_prcnt_est_16andOlder_w, 0),
        acs_urBlackToWhite_ratio_est_16andOlder =
            if_else(acs_ur_prcnt_est_16andOlder_w != 0, acs_ur_prcnt_est_16andOlder_b / acs_ur_prcnt_est_16andOlder_w, 0),
        acs_lfprHispToWhite_ratio_est_16andOlder =
            if_else(acs_lfpr_prcnt_est_16andOlder_w != 0, acs_lfpr_prcnt_est_16andOlder_h / acs_lfpr_prcnt_est_16andOlder_w, 0),
        acs_urHispToWhite_ratio_est_16andOlder =
            if_else(acs_ur_prcnt_est_16andOlder_w != 0, acs_ur_prcnt_est_16andOlder_h / acs_ur_prcnt_est_16andOlder_w, 0)
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

################################################################################
# Housing problems, housing costs, and housing value data.
df_housing_costs <-
    acs_df |>
    mutate(
        acs_hhCostsLessThan10PrcntIncome_prcnt_est =
            (rent_prcnt_of_income_lessThan10 + mortgage_prcnt_of_income_lessThan10 + hhCosts_prcnt_of_income_lessThan10) / total_nr_hh,
        acs_hhCostsBetween10and15PrcntIncome_prcnt_est =
            (rent_prcnt_of_income_10to15 + mortgage_prcnt_of_income_10to15 + hhCosts_prcnt_of_income_10to15) / total_nr_hh,
        acs_hhCostsBetween15and20PrcntIncome_prcnt_est =
            (rent_prcnt_of_income_15to20 + mortgage_prcnt_of_income_15to20 + hhCosts_prcnt_of_income_15to20) / total_nr_hh,
        acs_hhCostsBetween20and25PrcntIncome_prcnt_est =
            (rent_prcnt_of_income_20to25 + mortgage_prcnt_of_income_20to25 + hhCosts_prcnt_of_income_20to25) / total_nr_hh,
        acs_hhCostsBetween25and30PrcntIncome_prcnt_est =
            (rent_prcnt_of_income_25to30 + mortgage_prcnt_of_income_25to30 + hhCosts_prcnt_of_income_25to30) / total_nr_hh,
        acs_hhCostsBetween30and35PrcntIncome_prcnt_est =
            (rent_prcnt_of_income_30to35 + mortgage_prcnt_of_income_30to35 + hhCosts_prcnt_of_income_30to35) / total_nr_hh,
        acs_hhCostsBetween35and40PrcntIncome_prcnt_est =
            (rent_prcnt_of_income_35to40 + mortgage_prcnt_of_income_35to40 + hhCosts_prcnt_of_income_35to40) / total_nr_hh,
        acs_hhCostsBetween40and50PrcntIncome_prcnt_est =
            (rent_prcnt_of_income_40to50 + mortgage_prcnt_of_income_40to50 + hhCosts_prcnt_of_income_40to50) / total_nr_hh,
        acs_hhCostsAbove50PrcntIncome_prcnt_est =
            (rent_prcnt_of_income_above50 + mortgage_prcnt_of_income_above50 + hhCosts_prcnt_of_income_above50) / total_nr_hh,
        acs_grossRentPrcntIncome_median_est = if_else(median_rent_prcnt_of_income < 0, NA, median_rent_prcnt_of_income),
        acs_grossMortgagePrcntIncome_median_est = if_else(median_mortgage_prcnt_of_income < 0, NA, median_mortgage_prcnt_of_income),
        acs_hhCostsPrcntIncome_median_est = if_else(median_hhCosts_prcnt_of_income < 0, NA, median_hhCosts_prcnt_of_income)
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

df_vehicle <-
    acs_df |>
    mutate(
        acs_hhOwnsNoVehicle_prcnt_est = if_else(total_nr_hh != 0, (owner_no_vehicle + renter_no_vehicle) / total_nr_hh, 0),
        acs_hhOwns1Vehicle_prcnt_est = if_else(total_nr_hh != 0, (owner_one_vehicle + renter_one_vehicle) / total_nr_hh, 0),
        acs_hhOwns2Vehicle_prcnt_est = if_else(total_nr_hh != 0, (owner_two_vehicle + renter_no_vehicle) / total_nr_hh, 0),
        acs_hhOwns3Vehicle_prcnt_est = if_else(total_nr_hh != 0, (owner_three_vehicle + renter_no_vehicle) / total_nr_hh, 0),
        acs_hhOwns4Vehicle_prcnt_est = if_else(total_nr_hh != 0, (owner_three_vehicle + renter_no_vehicle) / total_nr_hh, 0),
        acs_hhOwns5orMoreVehicle_prcnt_est = if_else(total_nr_hh != 0, (owner_fiveOrMore_vehicle + renter_fiveOrMore_vehicle) / total_nr_hh, 0)
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

df_housing_problems <-
    acs_df |>
    mutate(
        acs_hhNoPhone_prcnt_est =
            if_else(total_nr_hh != 0, (total_nr_owner_occupied_units_no_telphone + total_nr_renter_occupied_units_no_telephone) / total_nr_hh, 0),
        acs_hhKitchenProblems_prcnt_est = if_else(total_nr_hh != 0, lacks_kitchen / total_nr_hh, 0),
        acs_hhBathroomProblems_prcnt_est = if_else(total_nr_hh != 0, lacks_plumbing / total_nr_hh, 0),
        acs_hhLessThan5PeronPerRoom_prcnt_est =
            if_else(total_nr_hh != 0, (owner_nr_people_per_room_below5 + renter_nr_people_per_room_below5) / total_nr_hh, 0),
        acs_hhBetweeen51and100PersonPerRoom_prcnt_est =
            if_else(total_nr_hh != 0, (owner_nr_people_per_room_51to100 + renter_nr_people_per_room_51to100) / total_nr_hh, 0),
        acs_hhBetweeen101and150PersonPerRoom_prcnt_est =
            if_else(total_nr_hh != 0, (owner_nr_people_per_room_101to150 + renter_nr_people_per_room_101to150) / total_nr_hh, 0),
        acs_hhBetweeen151and200PersonPerRoom_prcnt_est =
            if_else(total_nr_hh != 0, (owner_nr_people_per_room_151to2 + renter_nr_people_per_room_151to2) / total_nr_hh, 0),
        acs_hhAbove200PersonPerRoom_prcnt_est =
            if_else(total_nr_hh != 0, (owner_nr_people_per_room_above2 + renter_nr_people_per_room_above2) / total_nr_hh, 0),
        acs_hhWith0Problem_prcnt_est =
            if_else(total_nr_hh != 0, (owner_occupied_no_problems + renter_occupied_no_problems) / total_nr_hh, 0),
        acs_hhWith1Problem_prcnt_est =
            if_else(total_nr_hh != 0, (owner_occupied_one_problem + renter_occupied_one_problem) / total_nr_hh, 0),
        acs_hhWith2Problem_prcnt_est =
            if_else(total_nr_hh != 0, (owner_occupied_two_problems + renter_occupied_two_problems) / total_nr_hh, 0),
        acs_hhWith3Problem_prcnt_est =
            if_else(total_nr_hh != 0, (owner_occupied_three_problems + renter_occupied_three_problems) / total_nr_hh, 0),
        acs_hhWith4Problem_prcnt_est =
            if_else(total_nr_hh != 0, (owner_occupied_all_problems + renter_occupied_all_problems) / total_nr_hh, 0)
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

df_housing_values <-
    acs_df |>
    mutate(
        acs_hhLessThan50Value_prcnt_est =
            if_else(
                total_nr_owner_occupied_units != 0,
                (
                    house_value_owner_occupied_below10 + house_value_owner_occupied_10to15 +
                        house_value_owner_occupied_15to20 + house_value_owner_occupied_20to25 +
                        house_value_owner_occupied_25to30 + house_value_owner_occupied_30to35 +
                        house_value_owner_occupied_35to40 + house_value_owner_occupied_40to50
                ) /
                    total_nr_owner_occupied_units,
                0
            ),
        acs_hhBetween50And100Value_prcnt_est =
            if_else(
                total_nr_owner_occupied_units != 0,
                (
                    house_value_owner_occupied_50to60 + house_value_owner_occupied_60to70 +
                        house_value_owner_occupied_70to80 + house_value_owner_occupied_80to90 +
                        house_value_owner_occupied_90to100
                ) / 
                    total_nr_owner_occupied_units,
                0
            ),
        acs_hhBetween100And150Value_prcnt_est =
            if_else(
                total_nr_owner_occupied_units != 0,
                (house_value_owner_occupied_100to125 + house_value_owner_occupied_125to150) / total_nr_owner_occupied_units,
                0
            ),
        acs_hhBetween150And200Value_prcnt_est =
            if_else(
                total_nr_owner_occupied_units != 0,
                (house_value_owner_occupied_150to175 + house_value_owner_occupied_175to200) / total_nr_owner_occupied_units,
                0
            ),
        acs_hhBetween200And300Value_prcnt_est =
            if_else(
                total_nr_owner_occupied_units != 0,
                (house_value_owner_occupied_200to250 + house_value_owner_occupied_250to300) / total_nr_owner_occupied_units,
                0
            ),
        acs_hhBetween300And500Value_prcnt_est =
            if_else(
                total_nr_owner_occupied_units != 0,
                (house_value_owner_occupied_300to400 + house_value_owner_occupied_400to500) / total_nr_owner_occupied_units,
                0
            ),
        acs_hhBetween500And1000Value_prcnt_est =
            if_else(
                total_nr_owner_occupied_units != 0,
                (house_value_owner_occupied_500to750 + house_value_owner_occupied_750to1000) / total_nr_owner_occupied_units,
                0
            ),
        acs_hhAbove1000Value_prcnt_est =
            if_else(
                total_nr_owner_occupied_units != 0,
                (
                    house_value_owner_occupied_1000to1500 + house_value_owner_occupied_1500to2000 + house_value_owner_occupied_above2000
                ) /
                    total_nr_owner_occupied_units,
                0
            ),
        acs_homeValue_25thPtile_est = if_else(house_value_owner_occupied_25thPtile >= 0, house_value_owner_occupied_25thPtile, NA),
        acs_homeValue_median_est = if_else(house_value_owner_occupied_median >= 0, house_value_owner_occupied_median, NA),
        acs_homeValue_75thPtile_est = if_else(house_value_owner_occupied_75thPtile >= 0, house_value_owner_occupied_75thPtile, NA)
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

################################################################################
# Clean occupants per room by race data.
df_occupants_per_room_by_race <-
    acs_df |>
    mutate(
        acs_hhOccupantsPerRoomAbove101_prcnt_est_allAges_b =
            if_else(total_nr_black_hh != 0, nr_people_per_room_above101_black / total_nr_black_hh, 0),
        acs_hhOccupantsPerRoomAbove101_prcnt_est_allAges_w =
            if_else(total_nr_white_hh != 0, nr_people_per_room_above101_white / total_nr_white_hh, 0),
        acs_hhOccupantsPerRoomAbove101_prcnt_est_allAges_h =
            if_else(total_nr_hisp_hh != 0, nr_people_per_room_above101_hisp / total_nr_hisp_hh, 0),
        acs_ratioBlackWhiteOvercrowding_ratio_est =
            if_else(
                acs_hhOccupantsPerRoomAbove101_prcnt_est_allAges_w != 0,
                acs_hhOccupantsPerRoomAbove101_prcnt_est_allAges_b / acs_hhOccupantsPerRoomAbove101_prcnt_est_allAges_w,
                0
            ),
        acs_ratioHispWhiteOvercrowding_ratio_est =
            if_else(
                acs_hhOccupantsPerRoomAbove101_prcnt_est_allAges_w != 0,
                acs_hhOccupantsPerRoomAbove101_prcnt_est_allAges_h / acs_hhOccupantsPerRoomAbove101_prcnt_est_allAges_w,
                0
            )
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

################################################################################
# Rental data by race.
df_renters_by_race <-
    acs_df |>
    mutate(
        acs_renters_prcnt_est_allAges_b = if_else(total_nr_black_hh != 0, nr_renter_black / total_nr_black_hh, 0),
        acs_renters_prcnt_est_allAges_w = if_else(total_nr_white_hh != 0, nr_renter_white / total_nr_white_hh, 0),
        acs_renters_prcnt_est_allAges_h = if_else(total_nr_hisp_hh != 0, nr_renter_hisp / total_nr_hisp_hh, 0),
        acs_BlackToWhiteRenters_ratio_est =
            if_else(acs_renters_prcnt_est_allAges_w != 0, acs_renters_prcnt_est_allAges_b / acs_renters_prcnt_est_allAges_w, 0),
        acs_HispToWhiteRenters_ratio_est =
            if_else(acs_renters_prcnt_est_allAges_w != 0, acs_renters_prcnt_est_allAges_h / acs_renters_prcnt_est_allAges_w, 0)
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

################################################################################
# Presence of own children by race.
df_paoc_race <-
    acs_df |>
    mutate(
        acs_singleMom_prcnt_est_allAges_b = 
            if_else(
                total_nr_black_hh != 0,
                (femaleHHAlone_related_children_black_in_poverty + femaleHHAlone_related_children_black_above_poverty) / total_nr_black_hh,
                0
            ),
        acs_singleParent_prcnt_est_allAges_b = 
            if_else(
                total_nr_black_hh != 0,
                (
                    femaleHHAlone_related_children_black_in_poverty +
                        femaleHHAlone_related_children_black_above_poverty +
                        maleHHAlone_related_children_black_in_poverty +
                        maleHHAlone_related_children_black_above_poverty
                ) /
                    total_nr_black_hh,
                0
            ),
        acs_singleMom_prcnt_est_allAges_w = 
            if_else(
                total_nr_white_hh != 0,
                (femaleHHAlone_related_children_white_in_poverty + femaleHHAlone_related_children_white_above_poverty) / total_nr_white_hh,
                0
            ),
        acs_singleParent_prcnt_est_allAges_w = 
            if_else(
                total_nr_white_hh != 0,
                (
                    femaleHHAlone_related_children_white_in_poverty +
                        femaleHHAlone_related_children_white_above_poverty +
                        maleHHAlone_related_children_white_in_poverty +
                        maleHHAlone_related_children_white_above_poverty
                ) /
                    total_nr_white_hh,
                0
            ),
        acs_singleMom_prcnt_est_allAges_h = 
            if_else(
                total_nr_hisp_hh != 0,
                (femaleHHAlone_related_children_hisp_in_poverty + femaleHHAlone_related_children_hisp_above_poverty) / total_nr_hisp_hh,
                0
            ),
        acs_singleParent_prcnt_est_allAges_h = 
            if_else(
                total_nr_hisp_hh != 0,
                (
                    femaleHHAlone_related_children_hisp_in_poverty +
                        femaleHHAlone_related_children_hisp_above_poverty +
                        maleHHAlone_related_children_hisp_in_poverty +
                        maleHHAlone_related_children_hisp_above_poverty
                ) /
                    total_nr_hisp_hh,
                0
            ),
        acs_BlackToWhiteSingleMom_ratio_est =
            if_else(acs_singleMom_prcnt_est_allAges_w != 0, acs_singleMom_prcnt_est_allAges_b / acs_singleMom_prcnt_est_allAges_w, 0),
        acs_HispToWhiteSingleMom_ratio_est =
            if_else(acs_singleMom_prcnt_est_allAges_w != 0, acs_singleMom_prcnt_est_allAges_h / acs_singleMom_prcnt_est_allAges_w, 0),
        acs_BlackToWhiteSingleParent_ratio_est =
            if_else(acs_singleParent_prcnt_est_allAges_w != 0, acs_singleParent_prcnt_est_allAges_b / acs_singleParent_prcnt_est_allAges_w, 0),
        acs_HispToWhiteSingleParent_ratio_est =
            if_else(acs_singleParent_prcnt_est_allAges_w != 0, acs_singleParent_prcnt_est_allAges_h / acs_singleParent_prcnt_est_allAges_w, 0)
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

################################################################################
# Residential mobility.
df_mobility <-
    acs_df |>
    mutate(
        ###################################### All races.
        nr_moved_past_year = (total_nr_moved_same_county + total_nr_moved_same_state + total_nr_moved_same_country + total_nr_moved_diff_country),
        acs_yearMoved_median_est = if_else(median_year_moved_into_unit >= 0, year - median_year_moved_into_unit, NA),
        acs_movedPastYear_prcnt_est_1AndOlder = if_else(total_population != 0, nr_moved_past_year / total_population, 0),
        acs_movedWithinCounty_prcnt_est_1AndOlder = if_else(nr_moved_past_year != 0, total_nr_moved_same_county / nr_moved_past_year, 0),
        acs_movedWithinState_prcnt_est_1AndOlder = if_else(nr_moved_past_year != 0, total_nr_moved_same_state / nr_moved_past_year, 0),
        acs_movedDiff_prcnt_est_1AndOlder =
            if_else(nr_moved_past_year != 0, (total_nr_moved_same_country + total_nr_moved_diff_country) / nr_moved_past_year, 0),
        ######################################### Black individuals.
        nr_moved_past_year_black =
            total_nr_moved_same_county_black + total_nr_moved_same_state_black +
            total_nr_moved_same_country_black + total_nr_moved_diff_country_black,
        acs_movedPastYear_prcnt_est_1AndOlder_b = if_else(total_black_pop != 0, nr_moved_past_year_black / total_black_pop, 0),
        acs_movedWithinCounty_prcnt_est_1AndOlder_b =
            if_else(nr_moved_past_year_black != 0, total_nr_moved_same_county_black / nr_moved_past_year_black, 0),
        acs_movedWithinState_prcnt_est_1AndOlder_b =
            if_else(nr_moved_past_year_black != 0, total_nr_moved_same_state_black / nr_moved_past_year_black, 0),
        acs_movedDiff_prcnt_est_1AndOlder_b =
            if_else(
                nr_moved_past_year_black != 0,
                (total_nr_moved_same_country_black + total_nr_moved_diff_country_black) / nr_moved_past_year_black,
                0
            ),
        ######################################### White individuals.
        nr_moved_past_year_white =
            total_nr_moved_same_county_white + total_nr_moved_same_state_white +
            total_nr_moved_same_country_white + total_nr_moved_diff_country_white,
        acs_movedPastYear_prcnt_est_1AndOlder_w =
            if_else(total_white_pop != 0, nr_moved_past_year_white / total_white_pop, 0),
        acs_movedWithinCounty_prcnt_est_1AndOlder_w =
            if_else(nr_moved_past_year_white != 0, total_nr_moved_same_county_white / nr_moved_past_year_white, 0),
        acs_movedWithinState_prcnt_est_1AndOlder_w =
            if_else(nr_moved_past_year_white != 0, total_nr_moved_same_state_white / nr_moved_past_year_white, 0),
        acs_movedDiff_prcnt_est_1AndOlder_w =
            if_else(
                nr_moved_past_year_white != 0,
                (total_nr_moved_same_country_white + total_nr_moved_diff_country_white) / nr_moved_past_year_white,
                0
            ),
        ######################################### Hispanic individuals.
        nr_moved_past_year_hisp =
            total_nr_moved_same_county_hisp + total_nr_moved_same_state_hisp +
            total_nr_moved_same_country_hisp + total_nr_moved_diff_country_hisp,
        acs_movedPastYear_prcnt_est_1AndOlder_h =
            if_else(total_hisp_pop != 0, nr_moved_past_year_hisp / total_hisp_pop, 0),
        acs_movedWithinCounty_prcnt_est_1AndOlder_h =
            if_else(nr_moved_past_year_hisp != 0, total_nr_moved_same_county_hisp / nr_moved_past_year_hisp, 0),
        acs_movedWithinState_prcnt_est_1AndOlder_h =
            if_else(nr_moved_past_year_hisp != 0, total_nr_moved_same_state_hisp / nr_moved_past_year_hisp, 0),
        acs_movedDiff_prcnt_est_1AndOlder_h =
            if_else(
                nr_moved_past_year_hisp != 0,
                (total_nr_moved_same_country_hisp + total_nr_moved_diff_country_hisp) / nr_moved_past_year_hisp,
                0
            ),
        ######################################### Black to white ratios.
        acs_BlackToWhiteMovers_ratio_est =
            if_else(
                acs_movedPastYear_prcnt_est_1AndOlder_w != 0,
                acs_movedPastYear_prcnt_est_1AndOlder_b / acs_movedPastYear_prcnt_est_1AndOlder_w,
                0
            ),
        acs_BlackToWhiteMoversWithinCounty_ratio_est =
            if_else(
                acs_movedWithinCounty_prcnt_est_1AndOlder_w != 0,
                acs_movedWithinCounty_prcnt_est_1AndOlder_b / acs_movedWithinCounty_prcnt_est_1AndOlder_w,
                0
            ),
        acs_BlackToWhiteMoversWithinState_ratio_est =
            if_else(
                acs_movedWithinState_prcnt_est_1AndOlder_w != 0,
                acs_movedWithinState_prcnt_est_1AndOlder_b / acs_movedWithinState_prcnt_est_1AndOlder_w,
                0
            ),
        acs_BlackToWhiteMoversDiff_ratio_est =
            if_else(
                acs_movedDiff_prcnt_est_1AndOlder_w != 0,
                acs_movedDiff_prcnt_est_1AndOlder_b / acs_movedDiff_prcnt_est_1AndOlder_w,
                0
            ),
        ######################################### Hispanic to white ratios.
        acs_HispToWhiteMovers_ratio_est =
            if_else(
                acs_movedPastYear_prcnt_est_1AndOlder_w != 0,
                acs_movedPastYear_prcnt_est_1AndOlder_h / acs_movedPastYear_prcnt_est_1AndOlder_w,
                0
            ),
        acs_HispToWhiteMoversWithinCounty_ratio_est =
            if_else(
                acs_movedWithinCounty_prcnt_est_1AndOlder_w != 0,
                acs_movedWithinCounty_prcnt_est_1AndOlder_h / acs_movedWithinCounty_prcnt_est_1AndOlder_w,
                0
            ),
        acs_HispToWhiteMoversWithinState_ratio_est =
            if_else(
                acs_movedWithinState_prcnt_est_1AndOlder_w != 0,
                acs_movedWithinState_prcnt_est_1AndOlder_h / acs_movedWithinState_prcnt_est_1AndOlder_w,
                0
            ),
        acs_HispToWhiteMoversDiff_ratio_est =
            if_else(
                acs_movedDiff_prcnt_est_1AndOlder_w != 0,
                acs_movedDiff_prcnt_est_1AndOlder_h / acs_movedDiff_prcnt_est_1AndOlder_w,
                0
            )
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

################################################################################
# Child variables.
df_child <-
    acs_df |>
    mutate(
        acs_receiveCpaSnapSSI_prcnt_est_ages17AndYounger =
            if_else(total_nr_children_in_hh != 0, children_in_hh_with_publicAsstOrSnapOrSSI / total_nr_children_in_hh, 0),
        acs_notInSchoolOrLf_prcnt_est_ages16to19 =
            if_else(
                total_pop_16_to_19 != 0,
                (
                    not_in_school_or_labor_force_HSGrad_male +
                        not_in_school_or_labor_force_notHSGrad_male +
                        not_in_school_or_labor_force_HSGrad_female +
                        not_in_school_or_labor_force_notHSGrad_female
                ) /
                  total_pop_16_to_19,
                0
            ),
        acs_liveInSingleParentHh_prcnt_est_ages17AndYounger =
            if_else(
                total_nr_children_in_hh_exclude_marriage != 0,
                (total_nr_children_in_maleAloneHH + total_nr_children_in_femaleAloneHH) / total_nr_children_in_hh_exclude_marriage,
                0
            ),
        acs_doNotLiveWithParent_prcnt_est_ages17AndYounger =
            if_else(
                total_nr_children_in_hh_exclude_marriage != 0,
                (
                    total_nr_children_in_grandparentHH + total_nr_children_in_otherRelativesHH + total_nr_children_in_fosterHH
                ) /
                    total_nr_children_in_hh_exclude_marriage,
                0
            ),
        nr_not_in_poverty_black =
            if_else(
                year > 2012,
                black_abovePoverty_under6 + black_abovePoverty_6to11 + black_abovePoverty_12to17,
                black_abovePoverty_under5 + black_abovePoverty_age5 + black_abovePoverty_6to11 + black_abovePoverty_12to17
            ),
        nr_in_poverty_black =
            if_else(
                year > 2012,
                black_poverty_under6 + black_poverty_6to11 + black_poverty_12to17,
                black_poverty_under5 + black_poverty_age5 + black_poverty_6to11 + black_poverty_12to17
            ),
        acs_inPoverty_prcnt_est_ages17AndYounger_b =
            if_else(nr_not_in_poverty_black != 0, nr_in_poverty_black / (nr_in_poverty_black + nr_not_in_poverty_black), 0),
        nr_not_in_poverty_white =
            if_else(
                year > 2012,
                white_abovePoverty_under6 + white_abovePoverty_6to11 + white_abovePoverty_12to17,
                white_abovePoverty_under5 + white_abovePoverty_age5 + white_abovePoverty_6to11 + white_abovePoverty_12to17
            ),
        nr_in_poverty_white =
            if_else(
                year > 2012,
                white_poverty_under6 + white_poverty_6to11 + white_poverty_12to17,
                white_poverty_under5 + white_poverty_age5 + white_poverty_6to11 + white_poverty_12to17
            ),
        acs_inPoverty_prcnt_est_ages17AndYounger_w =
            if_else(nr_not_in_poverty_white != 0, nr_in_poverty_white / (nr_in_poverty_white + nr_not_in_poverty_white), 0),
        nr_not_in_poverty_hisp =
            if_else(
                year > 2012,
                hisp_abovePoverty_under6 + hisp_abovePoverty_6to11 + hisp_abovePoverty_12to17,
                hisp_abovePoverty_under5 + hisp_abovePoverty_age5 + hisp_abovePoverty_6to11 + hisp_abovePoverty_12to17
            ),
        nr_in_poverty_hisp =
            if_else(
                year > 2012,
                hisp_poverty_under6 + hisp_poverty_6to11 + hisp_poverty_12to17,
                hisp_poverty_under5 + hisp_poverty_age5 + hisp_poverty_6to11 + hisp_poverty_12to17
            ),
        acs_inPoverty_prcnt_est_ages17AndYounger_h =
            if_else(nr_not_in_poverty_hisp != 0, nr_in_poverty_hisp / (nr_in_poverty_hisp + nr_not_in_poverty_hisp), 0),
        acs_BlackToWhiteChildPoverty_ratio_est =
            if_else(
                acs_inPoverty_prcnt_est_ages17AndYounger_w != 0,
                acs_inPoverty_prcnt_est_ages17AndYounger_b / acs_inPoverty_prcnt_est_ages17AndYounger_w,
                0
            ),
        acs_HispToWhiteChildPoverty_ratio_est =
            if_else(
                acs_inPoverty_prcnt_est_ages17AndYounger_w != 0,
                acs_inPoverty_prcnt_est_ages17AndYounger_b / acs_inPoverty_prcnt_est_ages17AndYounger_w,
                0
            )
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

################################################################################
# Marriage by race.
df_marriage_by_race <-
    acs_df |>
    mutate(
        acs_married_prcnt_est_15AndOlder_b =
            if_else(total_pop_age15_black != 0, (black_male_married + black_female_married) / total_pop_age15_black, 0),
        acs_married_prcnt_est_15AndOlder_w =
            if_else(total_pop_age15_white != 0, (white_male_married + white_female_married) / total_pop_age15_white, 0),
        acs_married_prcnt_est_15AndOlder_h =
            if_else(total_pop_age15_hisp != 0, (hisp_male_married + hisp_female_married) / total_pop_age15_hisp, 0),
        acs_everMarried_prcnt_est_15AndOlder_b =
            if_else(
                total_pop_age15_black != 0,
                (
                    black_male_separated + black_male_widowed + black_male_divorced +
                        black_female_separated + black_female_widowed + black_female_divorced
                ) /
                    total_pop_age15_black,
                0
            ),
        acs_everMarried_prcnt_est_15AndOlder_w =
            if_else(
                total_pop_age15_white != 0,
                (
                    white_male_separated + white_male_widowed + white_male_divorced +
                        white_female_separated + white_female_widowed + white_female_divorced
                ) /
                    total_pop_age15_white,
                0
            ),
        acs_everMarried_prcnt_est_15AndOlder_h =
            if_else(
                total_pop_age15_hisp != 0,
                (
                    hisp_male_separated + hisp_male_widowed + hisp_male_divorced +
                        hisp_female_separated + hisp_female_widowed + hisp_female_divorced
                ) /
                    total_pop_age15_hisp,
                0
            ),
        acs_BlackToWhiteMarried_ratio_est =
            if_else(acs_married_prcnt_est_15AndOlder_w != 0, acs_married_prcnt_est_15AndOlder_b / acs_married_prcnt_est_15AndOlder_w, 0),
        acs_BlackToWhiteEverMarried_ratio_est =
            if_else(acs_everMarried_prcnt_est_15AndOlder_w != 0, acs_everMarried_prcnt_est_15AndOlder_b / acs_everMarried_prcnt_est_15AndOlder_w, 0),
        acs_HispToWhiteMarried_ratio_est =
            if_else(acs_married_prcnt_est_15AndOlder_w != 0, acs_married_prcnt_est_15AndOlder_h / acs_married_prcnt_est_15AndOlder_w, 0),
        acs_HispToWhiteEverMarried_ratio_est =
            if_else(acs_everMarried_prcnt_est_15AndOlder_w != 0, acs_everMarried_prcnt_est_15AndOlder_h / acs_everMarried_prcnt_est_15AndOlder_w, 0)
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

################################################################################
# Fertility
df_fertility <-
    acs_df |>
    mutate(
        acs_nrBirthsPer10000Unmarried_rate_est_ages15to50_allRaces_f =
            if_else(
                nr_unmarriedWomen_didNotBirth_ages15to50 != 0,
                nr_unmarriedWomen_gaveBirth_ages15to50 * 10000 / nr_unmarriedWomen_didNotBirth_ages15to50,
                0
            ),
        acs_nrBirthsPer10000Unmarried_rate_est_ages15to50_b_f =
            if_else(
                nr_unmarriedWomen_didNotBirth_ages15to50_black != 0,
                nr_unmarriedWomen_gaveBirth_ages15to50_black * 10000 / nr_unmarriedWomen_didNotBirth_ages15to50_black,
                0
            ),
        acs_nrBirthsPer10000Unmarried_rate_est_ages15to50_w_f =
            if_else(
                nr_unmarriedWomen_didNotBirth_ages15to50_white != 0,
                nr_unmarriedWomen_gaveBirth_ages15to50_white * 10000 / nr_unmarriedWomen_didNotBirth_ages15to50_white,
                0
            ),
        acs_nrBirthsPer10000Unmarried_rate_est_ages15to50_h_f =
            if_else(
                nr_unmarriedWomen_didNotBirth_ages15to50_hisp != 0,
                nr_unmarriedWomen_gaveBirth_ages15to50_hisp * 10000 / nr_unmarriedWomen_didNotBirth_ages15to50_hisp,
                0
            ),
        acs_BlackToWhiteUnmmarriedBirthRate_ratio_est =
            if_else(
                acs_nrBirthsPer10000Unmarried_rate_est_ages15to50_w_f != 0,
                acs_nrBirthsPer10000Unmarried_rate_est_ages15to50_b_f / acs_nrBirthsPer10000Unmarried_rate_est_ages15to50_w_f,
                0
            ),
        acs_HispToWhiteUnmmarriedBirthRate_ratio_est =
            if_else(
                acs_nrBirthsPer10000Unmarried_rate_est_ages15to50_w_f != 0,
                acs_nrBirthsPer10000Unmarried_rate_est_ages15to50_h_f / acs_nrBirthsPer10000Unmarried_rate_est_ages15to50_w_f,
                0
            )
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

################################################################################
# Health insurance.
df_health_insurance <-
    acs_df |>
    mutate(
        acs_hasHealthInsurance_prcnt_est_allAges_b =
            if_else(
                total_pop_civilianNonInst_black != 0,
                (
                    has_healthInsurance_under19_black + has_healthInsurance_19to64_black + has_healthInsurance_above65_black
                ) /
                    total_pop_civilianNonInst_black,
                0
            ),
        acs_hasHealthInsurance_prcnt_est_allAges_w =
            if_else(
                total_pop_civilianNonInst_white != 0,
                (
                    has_healthInsurance_under19_white + has_healthInsurance_19to64_white + has_healthInsurance_above65_white
                ) /
                    total_pop_civilianNonInst_white,
                0
            ),
        acs_hasHealthInsurance_prcnt_est_allAges_h =
            if_else(
                total_pop_civilianNonInst_hisp != 0,
                (
                    has_healthInsurance_under19_hisp + has_healthInsurance_19to64_hisp + has_healthInsurance_above65_hisp
                ) /
                    total_pop_civilianNonInst_hisp,
                0
            ),
        acs_BlackToWhiteHealthInsurance_ratio_est =
            if_else(
                acs_hasHealthInsurance_prcnt_est_allAges_w != 0,
                acs_hasHealthInsurance_prcnt_est_allAges_b / acs_hasHealthInsurance_prcnt_est_allAges_w,
                0
            ),
        acs_HispToWhiteHealthInsurance_ratio_est =
            if_else(
                acs_hasHealthInsurance_prcnt_est_allAges_w != 0,
                acs_hasHealthInsurance_prcnt_est_allAges_h / acs_hasHealthInsurance_prcnt_est_allAges_w,
                0
            ),
        acs_hasHealthInsurance_prcnt_est_under18_b =
            if_else(black_6to18_healthInsurance_denom != 0, has_healthInsurance_under19_black / black_6to18_healthInsurance_denom, 0),
        acs_hasHealthInsurance_prcnt_est_under18_w =
            if_else(white_6to18_healthInsurance_denom != 0, has_healthInsurance_under19_white / white_6to18_healthInsurance_denom, 0),
        acs_hasHealthInsurance_prcnt_est_under18_h =
            if_else(hisp_6to18_healthInsurance_denom != 0, has_healthInsurance_under19_hisp / hisp_6to18_healthInsurance_denom, 0),
        acs_BlackToWhiteChildHealthInsurance_ratio_est =
            if_else(
                acs_hasHealthInsurance_prcnt_est_under18_w != 0,
                acs_hasHealthInsurance_prcnt_est_under18_b / acs_hasHealthInsurance_prcnt_est_under18_w,
                0
            ),
        acs_HispToWhiteChildHealthInsurance_ratio_est =
            if_else(
                acs_hasHealthInsurance_prcnt_est_under18_w != 0,
                acs_hasHealthInsurance_prcnt_est_under18_h / acs_hasHealthInsurance_prcnt_est_under18_w,
                0
            )
    ) |>
    select(matches("^year$|full_fips|^state$|^county$|acs.*"))

################################################################################
# Save results.
df_final <-
    reduce(
        list(
            df_child, df_edu, df_fertility, df_health_insurance,
            df_housing_costs, df_housing_problems, df_housing_values,
            df_income_black, df_income_hisp, df_income_inequality,
            df_income_white, df_labor, df_marriage_by_race, df_median_income,
            df_mobility, df_occupants_per_room_by_race, df_paoc_race,
            df_poverty, df_renters_by_race, df_snap, df_vehicle, df_welfare
        ),
        function(x, y) {
            full_join(x, y, by = c("year", "full_fips", "state", "county"))
        }
    ) |>
    mutate(year = paste0(year - 4, "-", year))

write_csv(df_final, file.path(save_dir, "acs_county_clean.csv"))
gzip(file.path(save_dir, "acs_county_clean.csv"), remove = T, overwrite = T)
