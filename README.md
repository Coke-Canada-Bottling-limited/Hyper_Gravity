# Hyper_Gravity

This file indicates the overall structure of working notebooks used in Hyper projects.

## Demostats & Socialvalues & Store profiling (Previous Method)
This file is modified from Harold's note book "Demostats & socialvalues".
I modified it to fit our need, the overall gravity calculation logic is the same as the original workflow.

A few pipelines are processed in this notebook:
1. Preprocess and map the demostats value to postal code area.
* preprocessing from the source data:
    demo_stats_2023_variables_list
    combined_ldu_p1
    combined_ldu_p2
    environics.demostats2023_18
    environics.epccf_unique

* Generated file:
    environics.demostats_LDU_2023_cleaned

2.Social value file preprocessing:
* files used during preprocessing:
    environics.socialvalues
    social_values_2023_variables_list
* Generated file:
    environics.socialvalues_2023_cleaned

3.Filter for relevant features (Deprecated)
    used to manually select the features we need, replaced by master demostats table now.

4.Demostats variable processing.
Melt and join all the demostats tables, with the pivoting of tables for purpose of exporting to excel.
* files used:
    environics.demostats_LDU_2023_cleaned
    environics.demostats_LDU_2026_cleaned
    environics.demostats_LDU_2028_cleaned
    environics.demostats_LDU_2033_cleaned
    environics.demostats_LDU_2018_cleaned

* Generated file:
    environics.ldu_feature_vals_melted_df

5.Crossjoin Stores to Postal Codes
On the top of demostats results, joining the store information and do store filtering based on adjacent provinces. The gravity calculation is done at the end of this step.

* files used:
    hyper.unioned_stores_52weeks_2024_0927
    combined_ldu_p1
    combined_ldu_p2

* Generated files:
    hyper.store_LDU_attraction_0927
    hyper.store_LDU_attraction_f_halfp_0927
    (NOTE: filter out stores with less than 0.5% attraction)
    hyper.store_LDU_attraction_fullp_filtered_0910
    (NOTE: filter out stores with less than 1% attraction)

6. Store profiling 
* files used:
    hyper.store_LDU_attraction_f_halfp_0927 
    hyper.ldu_feature_vals_melted_df_master_variable_0920
    default.language_mapping 



## Demostats Immigration combined table

1. Applying demostats master table for variable selection. Joining the tables for a cross years result.
The immigration data is also included during table union.
We are taking three years of demostats data and two years of immigration data right now.

The datasets are preprocessed, pivoted and melted into usable format.
* preprocessing from the source data:
    environics.demostats_2023_all_pc_vars
    environics.demostats_2028_all_pc_vars
    environics.demostats_2033_all_pc_vars
    environics.newtocanada_2024q1p1_all_pc_vars
    environics.newtocanada_2024q1p2_all_pc_vars

* Generated file:
    hyper.ldu_feature_vals_melted_df_master_immi_combined

2. Store profiling 
* files used:
    hyper.store_LDU_attraction_f_halfp_0927
    hyper.ldu_feature_vals_melted_df_master_immi_combined
    default.language_mapping 

* Generated file:
    hyper.demostats_store_profiles_immi_combined


## SV variable provessing
Processing the social value tables and generate the similar results as demostats store profiling.

* data files used:
    environics.socialvalues_2024_all_pc_vars
    environics.socialvalues_2033_all_pc_vars
    hyper.store_LDU_attraction_f_halfp

* middle files created:
    hyper.ldu_feature_vals_sv

* result generated:
    hyper.demostats_store_sv_1015


## Gravity Competition
This table was used to process data and calculate the customer_group/banner/channel level competition.
The final results filtered out the cases when certain competition didn't affect the attraction of row.

* data files used:
    hyper.store_LDU_attraction_fullp_filtered

* middle files created:
    hyper.store_LDU_attraction_filtered_comp

* result generated:
    hyper.store_LDU_attraction_filtered_fullp_comp

## UNION THREE SOURCE DATA

Preprocessing and unioning three sources of data, including POS data, shipment data and Costco data.

Renaming and filtering of columns were done here.

* data files used:
    datamart.out_pos_store_fv_all_upc
    environics.costco_source_2024_07_14
    environics.shipment_stores_source_2024_07_14

* result generated:
 hyper.unioned_stores_52weeks_2024