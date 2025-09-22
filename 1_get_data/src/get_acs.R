library(here)
library(readr)
library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(R.utils)
library(censusapi)

save_dir <- here("1_get_data", "output")

################################################################################
# Set-up.
# Get a list of all Census data sets.
available_census_datasets <- listCensusApis()

# ACS 5-year profile, subject, and detailed tables.
acs_pro_tbls <- available_census_datasets |> filter(name == "acs/acs5/profile")
acs_sub_tbls <- available_census_datasets |> filter(name == "acs/acs5/subject")
acs_det_tbls <- available_census_datasets |> filter(name == "acs/acs5")

# Full list of variables from the ACS 5-year detailed tables.
full_var_acs_det_tbl <-
    map(
        acs_det_tbls$vintage,
        function(vintage) {
            listCensusMetadata(name = "acs/acs5", vintage = vintage) |>
                mutate(year = vintage)
        }
    ) |>
    bind_rows()

################################################################################
# Labels.
edu_var_labels <-
    c(
        "totalBlack25AndOlder" = "C15002B_001",
        "maleBlackLessThanHS" = "C15002B_003", "maleBlackHS" = "C15002B_004",
        "maleBlackSomeCollege" = "C15002B_005", "maleBlackCollege" = "C15002B_006",
        "femaleBlackLessThanHS" = "C15002B_008", "femaleBlackHS" = "C15002B_009",
        "femaleBlackSomeCollege" = "C15002B_010", "femaleBlackCollege" = "C15002B_011",
        "totalWhite25AndOlder" = "C15002H_001",
        "maleWhiteLessThanHS" = "C15002H_003", "maleWhiteHS" = "C15002H_004",
        "maleWhiteSomeCollege" = "C15002H_005", "maleWhiteCollege" = "C15002H_006",
        "femaleWhiteLessThanHS" = "C15002H_008", "femaleWhiteHS" = "C15002H_009",
        "femaleWhiteSomeCollege" = "C15002H_010", "femaleWhiteCollege" = "C15002H_011",
        "totalHisp25AndOlder" = "C15002I_001",
        "maleHispLessThanHS" = "C15002I_003", "maleHispHS" = "C15002I_004",
        "maleHispSomeCollege" = "C15002I_005", "maleHispCollege" = "C15002I_006",
        "femaleHispLessThanHS" = "C15002I_008", "femaleHispHS" = "C15002I_009",
        "femaleHispSomeCollege" = "C15002I_010", "femaleHispCollege" = "C15002I_011"
    )

labor_var_labels <-
    c(
        "black_pop_male_16andOlder" = "C23002B_002",
        "black_employed_male_16to64" = "C23002B_007",
        "black_unemployed_male_16to64" = "C23002B_008",
        "black_notInLaborForce_male_16to64" = "C23002B_009",
        "black_employed_male_65andOlder" = "C23002B_012",
        "black_unemployed_male_65andOlder" = "C23002B_013",
        "black_notInLaborForce_male_65andOlder" = "C23002B_014",
        "black_pop_female_16andOlder" = "C23002B_015",
        "black_employed_female_16to64" = "C23002B_020",
        "black_unemployed_female_16to64" = "C23002B_021",
        "black_notInLaborForce_female_16to64" = "C23002B_022",
        "black_employed_female_65andOlder" = "C23002B_025",
        "black_unemployed_female_65andOlder" = "C23002B_026",
        "black_notInLaborForce_female_65andOlder" = "C23002B_027",
        "white_pop_male_16andOlder" = "C23002H_002",
        "white_employed_male_16to64" = "C23002H_007",
        "white_unemployed_male_16to64" = "C23002H_008",
        "white_notInLaborForce_male_16to64" = "C23002H_009",
        "white_employed_male_65andOlder" = "C23002H_012",
        "white_unemployed_male_65andOlder" = "C23002H_013",
        "white_notInLaborForce_male_65andOlder" = "C23002H_014",
        "white_pop_female_16andOlder" = "C23002H_015",
        "white_employed_female_16to64" = "C23002H_020",
        "white_unemployed_female_16to64" = "C23002H_021",
        "white_notInLaborForce_female_16to64" = "C23002H_022",
        "white_employed_female_65andOlder" = "C23002H_025",
        "white_unemployed_female_65andOlder" = "C23002H_026",
        "white_notInLaborForce_female_65andOlder" = "C23002H_027",
        "hisp_pop_male_16andOlder" = "C23002I_002",
        "hisp_employed_male_16to64" = "C23002I_007",
        "hisp_unemployed_male_16to64" = "C23002I_008",
        "hisp_notInLaborForce_male_16to64" = "C23002I_009",
        "hisp_employed_male_65andOlder" = "C23002I_012",
        "hisp_unemployed_male_65andOlder" = "C23002I_013",
        "hisp_notInLaborForce_male_65andOlder" = "C23002I_014",
        "hisp_pop_female_16andOlder" = "C23002I_015",
        "hisp_employed_female_16to64" = "C23002I_020",
        "hisp_unemployed_female_16to64" = "C23002I_021",
        "hisp_notInLaborForce_female_16to64" = "C23002I_022",
        "hisp_employed_female_65andOlder" = "C23002I_025",
        "hisp_unemployed_female_65andOlder" = "C23002I_026",
        "hisp_notInLaborForce_female_65andOlder" = "C23002I_027"
    )

poverty_var_labels <-
    c(
        "black_pop_povertyStatusCalculable" = "B17001B_001",
        "black_inPoverty" = "B17001B_002",
        "white_pop_povertyStatusCalculable" = "B17001H_001",
        "white_inPoverty" = "B17001H_002",
        "hisp_pop_povertyStatusCalculable" = "B17001I_001",
        "hisp_inPoverty" = "B17001I_002"
    )

income_var_labels <-
    c(
        "total_nr_black_hh" = "B19001B_001", "black_below10" = "B19001B_002",
        "black_from10to15" = "B19001B_003", "black_from15to20" = "B19001B_004",
        "black_from20to25" = "B19001B_005", "black_from25to30" = "B19001B_006",
        "black_from30to35" = "B19001B_007", "black_from35to40" = "B19001B_008",
        "black_from40to45" = "B19001B_009", "black_from45to50" = "B19001B_010",
        "black_from50to60" = "B19001B_011", "black_from60to75" = "B19001B_012",
        "black_from75to100" = "B19001B_013", "black_from100to125" = "B19001B_014",
        "black_from125to150" = "B19001B_015", "black_from150to200" = "B19001B_016",
        "black_above200" = "B19001B_017",
        "total_nr_white_hh" = "B19001H_001", "white_below10" = "B19001H_002",
        "white_from10to15" = "B19001H_003", "white_from15to20" = "B19001H_004",
        "white_from20to25" = "B19001H_005", "white_from25to30" = "B19001H_006",
        "white_from30to35" = "B19001H_007", "white_from35to40" = "B19001H_008",
        "white_from40to45" = "B19001H_009", "white_from45to50" = "B19001H_010",
        "white_from50to60" = "B19001H_011", "white_from60to75" = "B19001H_012",
        "white_from75to100" = "B19001H_013", "white_from100to125" = "B19001H_014",
        "white_from125to150" = "B19001H_015", "white_from150to200" = "B19001H_016",
        "white_above200" = "B19001H_017",
        "total_nr_hisp_hh" = "B19001I_001", "hisp_below10" = "B19001I_002",
        "hisp_from10to15" = "B19001I_003", "hisp_from15to20" = "B19001I_004",
        "hisp_from20to25" = "B19001I_005", "hisp_from25to30" = "B19001I_006",
        "hisp_from30to35" = "B19001I_007", "hisp_from35to40" = "B19001I_008",
        "hisp_from40to45" = "B19001I_009", "hisp_from45to50" = "B19001I_010",
        "hisp_from50to60" = "B19001I_011", "hisp_from60to75" = "B19001I_012",
        "hisp_from75to100" = "B19001I_013", "hisp_from100to125" = "B19001I_014",
        "hisp_from125to150" = "B19001I_015", "hisp_from150to200" = "B19001I_016",
        "hisp_above200" = "B19001I_017",
        "black_medianHHIncome" = "B19013B_001", "white_medianHHIncome" = "B19013H_001",
        "hisp_medianHHIncome" = "B19013I_001",
        "total_nr_hh" = "B19058_001", "total_nr_hh_publicAsst_Snap" = "B19058_002",
        "shareOfIncome_lowest_quintile" = "B19082_001",
        "shareOfIncome_second_quintile" = "B19082_002",
        "shareOfIncome_third_quintile" = "B19082_003",
        "shareOfIncome_fourth_quintile" = "B19082_004",
        "shareOfIncome_fifth_quintile" = "B19082_005",
        "shareOfIncome_top5prcnt" = "B19082_006",
        "gini_coefficient" = "B19083_001",
        "blackHH_Snap" = "B22005B_002", "whiteHH_Snap" = "B22005H_002",
        "hispHH_Snap" = "B22005I_002"
    )

hh_costs_var_labels <-
    c(
        "rent_prcnt_of_income_lessThan10" = "B25070_002",
        "rent_prcnt_of_income_10to15" = "B25070_003",
        "rent_prcnt_of_income_15to20" = "B25070_004",
        "rent_prcnt_of_income_20to25" = "B25070_005",
        "rent_prcnt_of_income_25to30" = "B25070_006",
        "rent_prcnt_of_income_30to35" = "B25070_007",
        "rent_prcnt_of_income_35to40" = "B25070_008",
        "rent_prcnt_of_income_40to50" = "B25070_009",
        "rent_prcnt_of_income_above50" = "B25070_010",
        "median_rent_prcnt_of_income" = "B25071_001",
        "mortgage_prcnt_of_income_lessThan10" = "B25091_003",
        "mortgage_prcnt_of_income_10to15" = "B25091_004",
        "mortgage_prcnt_of_income_15to20" = "B25091_005",
        "mortgage_prcnt_of_income_20to25" = "B25091_006",
        "mortgage_prcnt_of_income_25to30" = "B25091_007",
        "mortgage_prcnt_of_income_30to35" = "B25091_008",
        "mortgage_prcnt_of_income_35to40" = "B25091_009",
        "mortgage_prcnt_of_income_40to50" = "B25091_010",
        "mortgage_prcnt_of_income_above50" = "B25091_011",
        "median_mortgage_prcnt_of_income" = "B25092_002",
        "hhCosts_prcnt_of_income_lessThan10" = "B25091_014",
        "hhCosts_prcnt_of_income_10to15" = "B25091_015",
        "hhCosts_prcnt_of_income_15to20" = "B25091_016",
        "hhCosts_prcnt_of_income_20to25" = "B25091_017",
        "hhCosts_prcnt_of_income_25to30" = "B25091_018",
        "hhCosts_prcnt_of_income_30to35" = "B25091_019",
        "hhCosts_prcnt_of_income_35to40" = "B25091_020",
        "hhCosts_prcnt_of_income_40to50" = "B25091_021",
        "hhCosts_prcnt_of_income_above50" = "B25091_022",
        "median_hhCosts_prcnt_of_income" = "B25092_003",
        "nr_renter_black" = "B25003B_003",
        "nr_renter_white" = "B25003H_003",
        "nr_renter_hisp" = "B25003I_003",
        "mortgage_prcnt_of_income_above30_black" = "B25140B_003",
        "mortgage_prcnt_of_income_above50_black" = "B25140B_004",
        "hhCosts_prcnt_of_income_above30_black" = "B25140B_007",
        "hhCosts_prcnt_of_income_above50_black" = "B25140B_008",
        "rent_prcnt_of_income_above30_black" = "B25140B_011",
        "rent_prcnt_of_income_above50_black" = "B25140B_012",
        "mortgage_prcnt_of_income_above30_white" = "B25140H_003",
        "mortgage_prcnt_of_income_above50_white" = "B25140H_004",
        "hhCosts_prcnt_of_income_above30_white" = "B25140H_007",
        "hhCosts_prcnt_of_income_above50_white" = "B25140H_008",
        "rent_prcnt_of_income_above30_white" = "B25140H_011",
        "rent_prcnt_of_income_above50_white" = "B25140H_012",
        "mortgage_prcnt_of_income_above30_hisp" = "B25140I_003",
        "mortgage_prcnt_of_income_above50_hisp" = "B25140I_004",
        "hhCosts_prcnt_of_income_above30_hisp" = "B25140I_007",
        "hhCosts_prcnt_of_income_above50_hisp" = "B25140I_008",
        "rent_prcnt_of_income_above30_hisp" = "B25140I_011",
        "rent_prcnt_of_income_above50_hisp" = "B25140I_012",
        "total_nr_owner_occupied_units_no_telphone" = "B25043_007",
        "total_nr_renter_occupied_units_no_telephone" = "B25043_016",
        "owner_no_vehicle" = "B25044_003", "owner_one_vehicle" = "B25044_004",
        "owner_two_vehicle" = "B25044_005", "owner_three_vehicle" = "B25044_006",
        "owner_four_vehicle" = "B25044_007", "owner_fiveOrMore_vehicle" = "B25044_008",
        "renter_no_vehicle" = "B25044_010", "renter_one_vehicle" = "B25044_011",
        "renter_two_vehicle" = "B25044_012", "renter_three_vehicle" = "B25044_013",
        "renter_four_vehicle" = "B25044_014", "renter_fiveOrMore_vehicle" = "B25044_015",
        "lacks_plumbing" = "B25048_003", "lacks_kitchen" = "B25052_003",
        "owner_nr_people_per_room_below5" = "B25014_003",
        "owner_nr_people_per_room_51to100" = "B25014_004",
        "owner_nr_people_per_room_101to150" = "B25014_005",
        "owner_nr_people_per_room_151to2" = "B25014_006",
        "owner_nr_people_per_room_above2" = "B25014_007",
        "renter_nr_people_per_room_below5" = "B25014_009",
        "renter_nr_people_per_room_51to100" = "B25014_010",
        "renter_nr_people_per_room_101to150" = "B25014_011",
        "renter_nr_people_per_room_151to2" = "B25014_012",
        "renter_nr_people_per_room_above2" = "B25014_013",
        "nr_people_per_room_above101_black" = "B25014B_003",
        "nr_people_per_room_above101_white" = "B25014H_003",
        "nr_people_per_room_above101_hisp" = "B25014I_003",
        "total_nr_owner_occupied_units" = "B25075_001",
        "house_value_owner_occupied_below10" = "B25075_002",
        "house_value_owner_occupied_10to15" = "B25075_003",
        "house_value_owner_occupied_15to20" = "B25075_004",
        "house_value_owner_occupied_20to25" = "B25075_005",
        "house_value_owner_occupied_25to30" = "B25075_006",
        "house_value_owner_occupied_30to35" = "B25075_007",
        "house_value_owner_occupied_35to40" = "B25075_008",
        "house_value_owner_occupied_40to50" = "B25075_009",
        "house_value_owner_occupied_50to60" = "B25075_010",
        "house_value_owner_occupied_60to70" = "B25075_011",
        "house_value_owner_occupied_70to80" = "B25075_012",
        "house_value_owner_occupied_80to90" = "B25075_013",
        "house_value_owner_occupied_90to100" = "B25075_014",
        "house_value_owner_occupied_100to125" = "B25075_015",
        "house_value_owner_occupied_125to150" = "B25075_016",
        "house_value_owner_occupied_150to175" = "B25075_017",
        "house_value_owner_occupied_175to200" = "B25075_018",
        "house_value_owner_occupied_200to250" = "B25075_019",
        "house_value_owner_occupied_250to300" = "B25075_020",
        "house_value_owner_occupied_300to400" = "B25075_021",
        "house_value_owner_occupied_400to500" = "B25075_022",
        "house_value_owner_occupied_500to750" = "B25075_023",
        "house_value_owner_occupied_750to1000" = "B25075_024",
        "house_value_owner_occupied_1000to1500" = "B25075_025",
        "house_value_owner_occupied_1500to2000" = "B25075_026",
        "house_value_owner_occupied_above2000" = "B25075_027",
        "house_value_owner_occupied_25thPtile" = "B25076_001",
        "house_value_owner_occupied_75thPtile" = "B25078_001",
        "house_value_owner_occupied_median" = "B25077_001",
        "house_value_owner_occupied_median_black" = "B25077B_001",
        "house_value_owner_occupied_median_white" = "B25077H_001",
        "house_value_owner_occupied_median_hisp" = "B25077I_001",
        "owner_occupied_one_problem" = "B25123_003",
        "owner_occupied_two_problems" = "B25123_004",
        "owner_occupied_three_problems" = "B25123_005",
        "owner_occupied_all_problems" = "B25123_006",
        "owner_occupied_no_problems" = "B25123_007",
        "renter_occupied_one_problem" = "B25123_009",
        "renter_occupied_two_problems" = "B25123_010",
        "renter_occupied_three_problems" = "B25123_011",
        "renter_occupied_all_problems" = "B25123_012",
        "renter_occupied_no_problems" = "B25123_013"
    )

children_var_labels_2009_2018 <- c("total_nr_children_in_marriedHH" = "B09005_003")
children_var_labels_2019_2023 <- c("total_nr_children_in_marriedHH" = "B09005_002")
children_poverty_var_labels_2009_2012 <-
    c(
        "black_poverty_under5" = "B17020B_003",
        "black_poverty_age5" = "B17020B_004",
        "black_poverty_6to11" = "B17020B_005",
        "black_poverty_12to17" = "B17020B_006",
        "black_abovePoverty_under5" = "B17020B_011",
        "black_abovePoverty_age5" = "B17020B_012",
        "black_abovePoverty_6to11" = "B17020B_013",
        "black_abovePoverty_12to17" = "B17020B_014",
        "white_poverty_under5" = "B17020H_003",
        "white_poverty_age5" = "B17020H_004",
        "white_poverty_6to11" = "B17020H_005",
        "white_poverty_12to17" = "B17020H_006",
        "white_abovePoverty_under5" = "B17020H_011",
        "white_abovePoverty_age5" = "B17020H_012",
        "white_abovePoverty_6to11" = "B17020H_013",
        "white_abovePoverty_12to17" = "B17020H_014",
        "hisp_poverty_under5" = "B17020I_003",
        "hisp_poverty_age5" = "B17020I_004",
        "hisp_poverty_6to11" = "B17020I_005",
        "hisp_poverty_12to17" = "B17020I_006",
        "hisp_abovePoverty_under5" = "B17020I_011",
        "hisp_abovePoverty_age5" = "B17020I_012",
        "hisp_abovePoverty_6to11" = "B17020I_013",
        "hisp_abovePoverty_12to17" = "B17020I_014"
    )
children_poverty_var_labels_2013_2023 <-
    c(
        "black_poverty_under6" = "B17020B_003",
        "black_poverty_6to11" = "B17020B_004",
        "black_poverty_12to17" = "B17020B_005",
        "black_abovePoverty_under6" = "B17020B_011",
        "black_abovePoverty_6to11" = "B17020B_012",
        "black_abovePoverty_12to17" = "B17020B_013",
        "white_poverty_under6" = "B17020H_003",
        "white_poverty_6to11" = "B17020H_004",
        "white_poverty_12to17" = "B17020H_005",
        "white_abovePoverty_under6" = "B17020H_011",
        "white_abovePoverty_6to11" = "B17020H_012",
        "white_abovePoverty_12to17" = "B17020H_013",
        "hisp_poverty_under6" = "B17020I_003",
        "hisp_poverty_6to11" = "B17020I_004",
        "hisp_poverty_12to17" = "B17020I_005",
        "hisp_abovePoverty_under6" = "B17020I_011",
        "hisp_abovePoverty_6to11" = "B17020I_012",
        "hisp_abovePoverty_12to17" = "B17020I_013"
    )
children_var_labels <-
    c(
        "maleHHAlone_related_children" = "B11004_010",
        "femaleHHAlone_related_children" = "B11004_016",
        "maleHHAlone_related_children_black_in_poverty" = "B17010B_011",
        "femaleHHAlone_related_children_black_in_poverty" = "B17010B_017",
        "maleHHAlone_related_children_black_above_poverty" = "B17010B_031",
        "femaleHHAlone_related_children_black_above_poverty" = "B17010B_037",
        "maleHHAlone_related_children_white_in_poverty" = "B17010H_011",
        "femaleHHAlone_related_children_white_in_poverty" = "B17010H_017",
        "maleHHAlone_related_children_white_above_poverty" = "B17010H_031",
        "femaleHHAlone_related_children_white_above_poverty" = "B17010H_037",
        "maleHHAlone_related_children_hisp_in_poverty" = "B17010I_011",
        "femaleHHAlone_related_children_hisp_in_poverty" = "B17010I_017",
        "maleHHAlone_related_children_hisp_above_poverty" = "B17010I_031",
        "femaleHHAlone_related_children_hisp_above_poverty" = "B17010I_037",
        "total_nr_children_in_hh_exclude_marriage" = "B09005_001",
        "total_nr_children_in_maleAloneHH" = "B09005_004",
        "total_nr_children_in_femaleAloneHH" = "B09005_005",
        "total_nr_children_in_parentHH" = "B09018_002",
        "total_nr_children_in_grandparentHH" = "B09018_006",
        "total_nr_children_in_otherRelativesHH" = "B09018_007",
        "total_nr_children_in_fosterHH" = "B09018_008",
        "total_nr_children_in_hh" = "B09010_001",
        "children_in_hh_with_publicAsstOrSnapOrSSI" = "B09010_002",
        "total_pop_16_to_19" = "B14005_001",
        "not_in_school_or_labor_force_HSGrad_male" = "B14005_011",
        "not_in_school_or_labor_force_notHSGrad_male" = "B14005_015",
        "not_in_school_or_labor_force_HSGrad_female" = "B14005_025",
        "not_in_school_or_labor_force_notHSGrad_female" = "B14005_029",
        "black_6to18_healthInsurance_denom" = "C27001B_002",
        "white_6to18_healthInsurance_denom" = "C27001H_002",
        "hisp_6to18_healthInsurance_denom" = "C27001I_002"
    )

marriage_var_labels <-
    c(
        "total_pop_age15_black" = "B12002B_001",
        "black_male_married" = "B12002B_004",
        "black_male_separated" = "B12002B_005",
        "black_male_widowed" = "B12002B_006",
        "black_male_divorced" = "B12002B_007",
        "black_female_married" = "B12002B_010",
        "black_female_separated" = "B12002B_011",
        "black_female_widowed" = "B12002B_012",
        "black_female_divorced" = "B12002B_013",
        "total_pop_age15_white" = "B12002H_001",
        "white_male_married" = "B12002H_004",
        "white_male_separated" = "B12002H_005",
        "white_male_widowed" = "B12002H_006",
        "white_male_divorced" = "B12002H_007",
        "white_female_married" = "B12002H_010",
        "white_female_separated" = "B12002H_011",
        "white_female_widowed" = "B12002H_012",
        "white_female_divorced" = "B12002H_013",
        "total_pop_age15_hisp" = "B12002I_001",
        "hisp_male_married" = "B12002I_004",
        "hisp_male_separated" = "B12002I_005",
        "hisp_male_widowed" = "B12002I_006",
        "hisp_male_divorced" = "B12002I_007",
        "hisp_female_married" = "B12002I_010",
        "hisp_female_separated" = "B12002I_011",
        "hisp_female_widowed" = "B12002I_012",
        "hisp_female_divorced" = "B12002I_013"
    )

fertility_var_labels <-
    c(
        "nr_marriedWomen_gaveBirth_ages15to50" = "B13002_003",
        "nr_unmarriedWomen_gaveBirth_ages15to50" = "B13002_007",
        "nr_marriedWomen_didNotBirth_ages15to50" = "B13002_012",
        "nr_unmarriedWomen_didNotBirth_ages15to50" = "B13002_016",
        "nr_marriedWomen_gaveBirth_ages15to50_black" = "B13002B_003",
        "nr_unmarriedWomen_gaveBirth_ages15to50_black" = "B13002B_004",
        "nr_marriedWomen_didNotBirth_ages15to50_black" = "B13002B_006",
        "nr_unmarriedWomen_didNotBirth_ages15to50_black" = "B13002B_007",
        "nr_marriedWomen_gaveBirth_ages15to50_white" = "B13002H_003",
        "nr_unmarriedWomen_gaveBirth_ages15to50_white" = "B13002H_004",
        "nr_marriedWomen_didNotBirth_ages15to50_white" = "B13002H_006",
        "nr_unmarriedWomen_didNotBirth_ages15to50_white" = "B13002H_007",
        "nr_marriedWomen_gaveBirth_ages15to50_hisp" = "B13002I_003",
        "nr_unmarriedWomen_gaveBirth_ages15to50_hisp" = "B13002I_004",
        "nr_marriedWomen_didNotBirth_ages15to50_hisp" = "B13002I_006",
        "nr_unmarriedWomen_didNotBirth_ages15to50_hisp" = "B13002I_007"
    )

health_insurace_var_labels <-
    c(
        "total_pop_civilianNonInst_black" = "C27001B_001",
        "has_healthInsurance_under19_black" = "C27001B_003",
        "has_healthInsurance_19to64_black" = "C27001B_006",
        "has_healthInsurance_above65_black" = "C27001B_009",
        "total_pop_civilianNonInst_white" = "C27001H_001",
        "has_healthInsurance_under19_white" = "C27001H_003",
        "has_healthInsurance_19to64_white" = "C27001H_006",
        "has_healthInsurance_above65_white" = "C27001H_009",
        "total_pop_civilianNonInst_hisp" = "C27001I_001",
        "has_healthInsurance_under19_hisp" = "C27001I_003",
        "has_healthInsurance_19to64_hisp" = "C27001I_006",
        "has_healthInsurance_above65_hisp" = "C27001I_009"
    )

residence_var_labels <-
    c(
        "median_year_moved_into_unit" = "B25039_001",
        "total_population" = "B07001_001",
        "total_nr_moved_same_county" = "B07001_033",
        "total_nr_moved_same_state" = "B07001_049",
        "total_nr_moved_same_country" = "B07001_065",
        "total_nr_moved_diff_country" = "B07001_081",
        "total_black_pop" = "B07004B_001",
        "total_nr_moved_same_county_black" = "B07004B_003",
        "total_nr_moved_same_state_black" = "B07004B_004",
        "total_nr_moved_same_country_black" = "B07004B_005",
        "total_nr_moved_diff_country_black" = "B07004B_006",
        "total_white_pop" = "B07004H_001",
        "total_nr_moved_same_county_white" = "B07004H_003",
        "total_nr_moved_same_state_white" = "B07004H_004",
        "total_nr_moved_same_country_white" = "B07004H_005",
        "total_nr_moved_diff_country_white" = "B07004H_006",
        "total_hisp_pop" = "B07004I_001",
        "total_nr_moved_same_county_hisp" = "B07004I_003",
        "total_nr_moved_same_state_hisp" = "B07004I_004",
        "total_nr_moved_same_country_hisp" = "B07004I_005",
        "total_nr_moved_diff_country_hisp" = "B07004I_006"
    )

################################################################################
# Turn labels into ACS variables.
turn_label_into_var <- function(vector_of_labels, years) {
    est <- vector_of_labels |> paste0("E")
    names(est) <- names(vector_of_labels)
    moe <- vector_of_labels |> paste0("M")
    names(moe) <- names(vector_of_labels) |> paste0("_moe")
    return(list("estimates" = est, "moe" = moe, "years" = years))
}

# Years of availability for the ACS.
years_acs <- 
    list(
        2009:2023, 2009:2018, 2019:2023, 2009:2012, 2013:2023, 2009:2023,
        2009:2023, 2009:2023, 2009:2023, 2009:2023, 2009:2023, 2009:2023,
        2009:2023, 2009:2023
    )

acs_vars <-
    pmap(
        list(
            list(
                "children" = children_var_labels,
                "children_2009_2018" = children_var_labels_2009_2018,
                "children_2019_2023" = children_var_labels_2019_2023,
                "children_poverty_2009_2012" = children_poverty_var_labels_2009_2012,
                "children_poverty_2013_2023" = children_poverty_var_labels_2013_2023,
                "edu" = edu_var_labels, "fertility" = fertility_var_labels,
                "health_insurance" = health_insurace_var_labels,
                "hh_costs" = hh_costs_var_labels, "income" = income_var_labels,
                "labor" = labor_var_labels, "marriage" = marriage_var_labels,
                "poverty" = poverty_var_labels, "residence" = residence_var_labels
            ),
            years_acs
        ),
        turn_label_into_var
    )

################################################################################
# Ensure each variable code refers to the same underlying concept each release.
check_acs_vars_match <- function(full_acs_vars, vector_of_vars) {
    df <-
        tibble(
            name = vector_of_vars[["estimates"]],
            var_name = names(vector_of_vars[["estimates"]])
        )
    
    full_acs_vars <- full_acs_vars |> filter(year %in% vector_of_vars[["years"]])
    
    matches_df <-
        full_acs_vars |>
        inner_join(df, by = "name") |>
        mutate(
            label = str_remove_all(tolower(label), "[[:punct:]]"),
            concept = str_remove_all(tolower(concept), "[[:punct:]]")
        ) |>
        group_by(name, var_name, label, concept) |>
        summarise(n = n(), years = paste(sort(unique(year)), collapse = "|")) |>
        ungroup() |>
        nest_by(name) |>
        arrange(name) |>
        mutate(
            nr_unique_entries = nrow(data),
            nr_years = sum(data$n),
            min_year = min(unlist(str_split(data$years, "\\|"))),
            max_year = max(unlist(str_split(data$years, "\\|"))),
            var_name = unique(data$var_name)
        )
    
    return(matches_df)
}

matches_df <-
    map(acs_vars, check_acs_vars_match, full_acs_vars = full_var_acs_det_tbl)

################################################################################
# Query the ACS API.
get_acs_data <- function(name_arg, vintage_arg, region_arg, vars_arg) {
    getCensus(
        name = name_arg,
        vintage = vintage_arg,
        region = region_arg,
        vars = vars_arg
    ) |>
        mutate(year = vintage_arg, full_fips = paste0(state, county))
}

# Drop the data field.
df_acs_query_params <-
    map(matches_df, function(df) {df |> select(-data)}) |>
    bind_rows()

# Expand data so each year+variable has its own row, and it's nested by year.
df_acs_query_params_years <-
    pmap(
        list(
            df_acs_query_params$name, df_acs_query_params$var_name,
            df_acs_query_params$min_year, df_acs_query_params$max_year
        ),
        function(name_arg, var_arg, min_arg, max_arg) {
            expand_grid(name = name_arg, var_name = var_arg, year = min_arg:max_arg)
        }
    ) |>
    bind_rows() |>
    nest_by(year)

# Query the API and save results.
df_acs_results <-
    pmap(
        list(df_acs_query_params_years$year, df_acs_query_params_years$data),
        function(year, df) {
            var_codes <- df$name
            names(var_codes) <- df$var_name
            moe_codes <- str_replace(df$name, "E$", "M")
            names(moe_codes) <- paste0(df$var_name, "_moe")
            all_codes <- c(var_codes, moe_codes)
            
            getCensus(
                name = "acs/acs5",
                vintage = year,
                region = "county:*",
                vars = all_codes
            ) |>
                rename(any_of(all_codes)) |>
                mutate(full_fips = paste0(state, county), year = year)
        }
    ) |>
    bind_rows()

write_csv(df_acs_results, file.path(save_dir, "acs_county.csv"))
gzip(file.path(save_dir, "acs_county.csv"), remove = T, overwrite = T)
