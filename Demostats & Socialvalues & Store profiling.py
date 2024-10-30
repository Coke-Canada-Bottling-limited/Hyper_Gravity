# Databricks notebook source
# from shapely import wkt
import math
import numpy as np
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    ArrayType,
    MapType,
    DateType,
    DoubleType,
    BooleanType,
    LongType,
)

# COMMAND ----------

# DBTITLE 1,Pull Demostats Descriptor Columns from Variables List
# filtering and grouping the languages

variable_desc = spark.sql(
    """
SELECT
  /*CASE
    WHEN Description = 'Somali' THEN 'African Languages'
    WHEN Description IN ('Cantonese', 'Chinese N.O.S', 'Mandarin') THEN 'Chinese Languages'
    WHEN Description IN (
      'Czech',
      'Hungarian',
      'Polish',
      'Romanian',
      'Russian',
      'Serbian',
      'Ukrainian'
    ) THEN 'East European Languages'
    WHEN Description IN (
      'Bengali',
      'Gujarati',
      'Hindi',
      'Panjabi',
      'Tamil',
      'Urdu'
    ) THEN 'Languages of India'
    WHEN Description = 'Creoles' THEN 'Mixed Languages'
    WHEN Description IN ('Japanese', 'Korean', 'Vietnamese') THEN 'Other East Asian Languages'
    WHEN Description IN ('Croatian', 'Greek', 'Italian', 'Portuguese') THEN 'South European Languages'
    WHEN Description = 'Tagalog' THEN 'Southeast Asian Languages'
    WHEN Description IN (
      'English & French',
      'English & French & Non-Official',
      'English & Non-Official',
      'French & Non-Official',
      'Other Languages'
    ) THEN 'Unspecified or Multilingual'
    WHEN Description IN ('Arabic', 'Persian', 'Turkish') THEN 'West Asian Languages'
    WHEN Description IN (
      'Dutch',
      'French',
      'German',
      'Spanish'
    ) THEN 'West European Languages'
    ELSE Description
  END */Description,
  Category,
  Variable variable
FROM
  demo_stats_2023_variables_list
"""
)
variable_desc.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Dissemination Area

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT GEO.PRABB, GEO.CMANAME, P1.* FROM environics.demostats2023_18 P1
# MAGIC LEFT JOIN (SELECT DISTINCT PRCDDA, CASE WHEN CMANAME LIKE '%,%' THEN LEFT(CASE WHEN CMANAME LIKE 'Trois%' THEN REPLACE(CMANAME, '�', 'è') WHEN CMANAME LIKE 'Sept%' THEN REPLACE(CMANAME, '�', 'Î') ELSE REPLACE(CMANAME, '�', 'é') END, CHARINDEX(',', CMANAME)-1) ELSE REPLACE(CMANAME, '�', 'é') END CMANAME, PRABB FROM environics.geo_2023) GEO
# MAGIC ON P1.CODE = GEO.PRCDDA
# MAGIC WHERE P1.GEO = 'PRCDDA'

# COMMAND ----------

# char operations and table joining

ds_da_2018 = spark.sql(
"""
SELECT GEO.PRABB, GEO.CMANAME, P1.* FROM environics.demostats2023_18 P1
LEFT JOIN (SELECT DISTINCT PRCDDA, CASE WHEN CMANAME LIKE '%,%' THEN LEFT(CASE WHEN CMANAME LIKE 'Trois%' THEN REPLACE(CMANAME, '�', 'è') WHEN CMANAME LIKE 'Sept%' THEN REPLACE(CMANAME, '�', 'Î') ELSE REPLACE(CMANAME, '�', 'é') END, CHARINDEX(',', CMANAME)-1) ELSE REPLACE(CMANAME, '�', 'é') END CMANAME, PRABB FROM environics.geo_2023) GEO
ON P1.CODE = GEO.PRCDDA
WHERE P1.GEO = 'PRCDDA'
"""
)

# extract the values from the variables column, categorizing the year brackets.
# join with table of feature names.

values = ds_da_2018.columns[4:]
ds_da_cleaned_2018 = ds_da_2018.melt(
        ids=['CODE', 'CMANAME', 'PRABB'], values=values, 
        variableColumnName="variable", 
        valueColumnName="value").join(variable_desc, on='variable', how='left').withColumn('YEAR', when(substring('variable', 0, 3)=='EHY', 2018).when(substring('variable', 0, 3)=='ECY', 2023).when(substring('variable', 0, 3)=='P3Y', 2026).when(substring('variable', 0, 3)=='P5Y', 2028).when(substring('variable', 0, 3)=='P0Y', 2033))
ds_da_cleaned_2018.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Mapping DA to Postal Codes
# MAGIC Only required for 2018 historical, 2026, 2028, and 2033 projections

# COMMAND ----------

DA_LDU_map = spark.sql(
"""
SELECT EPCCF.PRCDDA, LDU.*, SUM(AREA)/SUM(T2.TOTAL) LDU_PERCENT_OF_DA FROM environics.epccf_unique EPCCF
INNER JOIN (SELECT POSTALCODE, PROV, FIRST(LONGITUDE) LONGITUDE, FIRST(LATITUDE) LATITUDE, SUM(AREA) AREA
    FROM (SELECT * FROM combined_ldu_p1
    UNION
    SELECT * FROM combined_ldu_p2
    ORDER BY POSTALCODE, AREA DESC)
    GROUP BY POSTALCODE, PROV) LDU
ON EPCCF.FSALDU = LDU.POSTALCODE
INNER JOIN (SELECT DISTINCT EPCCF.PRCDDA, SUM(AREA) TOTAL FROM environics.epccf_unique EPCCF
    INNER JOIN (SELECT * FROM combined_ldu_p1
    UNION
    SELECT * FROM combined_ldu_p2) T
    ON EPCCF.FSALDU = T.POSTALCODE
    GROUP BY PRCDDA) T2
ON EPCCF.PRCDDA = T2.PRCDDA
GROUP BY EPCCF.PRCDDA, LDU.POSTALCODE, PROV, LDU.LONGITUDE, LDU.LATITUDE, AREA
"""
)
DA_LDU_map.show()

# COMMAND ----------

# Convert Dissemination Area values to Postal Code (LDU), prorating by postal code area

demostats_LDU_cleaned_2018 = DA_LDU_map.join(ds_da_cleaned_2018, 
               DA_LDU_map.PRCDDA == ds_da_cleaned_2018.CODE, 
               "inner").withColumnRenamed('value', 'DA_VALUE').withColumn('value', round(col('LDU_PERCENT_OF_DA')*col('DA_VALUE'),0)).select('variable', col('POSTALCODE').alias('CODE'), 'PRCDDA', 'CMANAME', 'PRABB', 'value', 'Description', 'Category', 'YEAR')
demostats_LDU_cleaned_2018.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Pull Demostats CY Data at Postal Code level

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from environics.demostats2023_23

# COMMAND ----------

demostats_LDU_2023 = spark.sql(
"""
SELECT EPCCF.PRABB, EPCCF.CMANAME, EPCCF.PRCDDA, DEMOSTATS_2023.* FROM
environics.demostats2023_23 DEMOSTATS_2023
LEFT JOIN (SELECT DISTINCT FSALDU, CASE WHEN CMANAME LIKE '%,%' THEN LEFT(CASE WHEN CMANAME LIKE 'Trois%' THEN REPLACE(CMANAME, '�', 'è') WHEN CMANAME LIKE 'Sept%' THEN REPLACE(CMANAME, '�', 'Î') ELSE REPLACE(CMANAME, '�', 'é') END, CHARINDEX(',', CMANAME)-1) ELSE REPLACE(CMANAME, '�', 'é') END CMANAME, PRABB, PRCDDA FROM environics.epccf_unique) EPCCF
ON DEMOSTATS_2023.CODE = EPCCF.FSALDU
WHERE DEMOSTATS_2023.GEO = 'FSALDU'
"""
)
display(demostats_LDU_2023)

# COMMAND ----------

values = demostats_LDU_2023.columns[5:]
demostats_LDU_2023_cleaned = demostats_LDU_2023.melt(
        ids=['CODE', 'PRCDDA', 'CMANAME', 'PRABB'], values=values, 
        variableColumnName="variable", 
        valueColumnName="value").join(variable_desc, on='variable', how='left').withColumn('YEAR', when(substring('variable', 0, 3)=='EHY', 2018).when(substring('variable', 0, 3)=='ECY', 2023).when(substring('variable', 0, 3)=='P3Y', 2026).when(substring('variable', 0, 3)=='P5Y', 2028).when(substring('variable', 0, 3)=='P0Y', 2033))
demostats_LDU_2023_cleaned.show()

# COMMAND ----------

# demostats_LDU_2023_cleaned.write.mode("overwrite").option("overwriteSchema", "True").format("delta").saveAsTable(
#     "environics.demostats_LDU_2023_cleaned"
# )

# COMMAND ----------

# demostats_LDU_cleaned = demostats_LDU_cleaned_2018.union(demostats_LDU_2023_cleaned).union(demostats_LDU_cleaned_2026).union(demostats_LDU_cleaned_2028).union(demostats_LDU_cleaned_2033)
# demostats_LDU_cleaned.write.mode("overwrite").option("overwriteSchema", "True").format("delta").saveAsTable(
#     "default.demostats_LDU_cleaned"
# )

# COMMAND ----------

# DBTITLE 1,Socialvalues
SV_2023 = spark.sql(
"""
SELECT EPCCF.PRABB, EPCCF.CMANAME, EPCCF.PRCDDA, SV.* FROM
environics.socialvalues SV
LEFT JOIN (SELECT DISTINCT FSALDU, CASE WHEN CMANAME LIKE '%,%' THEN LEFT(CASE WHEN CMANAME LIKE 'Trois%' THEN REPLACE(CMANAME, '�', 'è') WHEN CMANAME LIKE 'Sept%' THEN REPLACE(CMANAME, '�', 'Î') ELSE REPLACE(CMANAME, '�', 'é') END, CHARINDEX(',', CMANAME)-1) ELSE REPLACE(CMANAME, '�', 'é') END CMANAME, PRABB, PRCDDA FROM environics.epccf_unique) EPCCF
ON SV.CODE = EPCCF.FSALDU
WHERE SV.GEO = 'FSALDU'
"""
)

# COMMAND ----------

SV_variable_desc = spark.sql(
    """
SELECT Description, Category, Variable variable
FROM
  social_values_2023_variables_list
"""
)
SV_variable_desc.show()

# COMMAND ----------

values = SV_2023.columns[5:]
SV_2023_cleaned = SV_2023.melt(
        ids=['CODE', 'PRCDDA', 'CMANAME', 'PRABB'], values=values, 
        variableColumnName="variable", 
        valueColumnName="value").join(SV_variable_desc, on='variable', how='left').withColumn('YEAR', lit(2023))
SV_2023_cleaned.show()

# COMMAND ----------

# SV_2023_cleaned.write.mode("overwrite").option("overwriteSchema", "True").format("delta").saveAsTable(
#     "environics.socialvalues_2023_cleaned"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter for relevant features

# COMMAND ----------

# gender/ethnicity/marital status/labour force activity
filter_conditions = [
    "Males",
    "Females",
    "1 Person",
    "2 Persons",
    "3 Persons",
    "4 Persons",
    "5 Or More Persons",
    'Somali',
    'Cantonese',
    'Chinese N.O.S',
    'Mandarin',
    'Czech',
    'Hungarian',
    'Polish',
    'Romanian',
    'Russian',
    'Serbian',
    'Ukrainian',
    'Indigenous Languages',
    'Bengali',
    'Gujarati',
    'Hindi',
    'Panjabi',
    'Tamil',
    'Urdu',
    'Creoles',
    'Japanese',
    'Korean',
    'Vietnamese',
    'Croatian',
    'Greek',
    'Italian',
    'Portuguese',
    'Tagalog',
    'English & French',
    'English & French & Non-Official',
    'English & Non-Official',
    'French & Non-Official',
    'Other Languages',
    'Arabic',
    'Persian',
    'Turkish',
    'Dutch',
    'English',
    'French',
    'German',
    'Spanish',
    ### Visible Minority:
    'Visible Minority Chinese',
    'Visible Minority South Asian',
    'Visible Minority Black',
    'Visible Minority Filipino',
    'Visible Minority Latin American',
    'Visible Minority Southeast Asian',
    'Visible Minority Arab',
    'Visible Minority West Asian',
    'Visible Minority Korean',
    'Visible Minority Japanese',
    'Visible Minority All Other Visible Minorities',
    'Visible Minority Multiple Visible Minorities',
    'Visible Minority Not A Visible Minority',
    ### Country of Immigration:
    "Non-Immigrant",
    'Non-Permanent Resident',
    "Côte d'Ivoire",
    "Russia",
    "Other Western Africa",
    "Congo, The Democratic Republic of The",
    "Guyana",
    "Eritrea",
    "Philippines",
    "Other Central America",
    "Other Southern Europe",
    "Malaysia",
    "Fiji",
    "Turkey",
    "Iraq",
    "Germany",
    "Trinidad And Tobago",
    "Afghanistan",
    "Cambodia",
    "France",
    "Greece",
    "Sri Lanka",
    "Other Southern Africa",
    "Taiwan",
    "Algeria",
    "Ghana",
    "Peru",
    "Other West Central Asia And Middle East",
    "United States",
    "China",
    "India",
    "Other North America",
    "Somalia",
    "Chile",
    "Other Caribbean And Bermuda",
    "Croatia",
    "Nigeria",
    "Italy",
    "Other Western Europe",
    "Cuba",
    "Bangladesh",
    "Iran",
    "Ireland",
    "Morocco",
    "Hong Kong",
    "Venezuela",
    "Ukraine",
    "Israel",
    "Bosnia Herzegovina",
    "South Korea",
    "Mexico",
    "Other Eastern Asia",
    "Other Northern Europe",
    "Tunisia",
    "Syria",
    "Saudi Arabia",
    "Other Eastern Africa",
    "Ethiopia",
    "Jamaica",
    "United Arab Emirates",
    "Other Northern Africa",
    "Czech Republic",
    "Brazil",
    "Kenya",
    "Lebanon",
    "Tanzania",
    "Japan",
    "Other Southern Asia",
    "Other Eastern Europe",
    "Haiti",
    "Other Southeast Asia",
    "Other Oceania And Other",
    "Poland",
    "Portugal",
    "Cameroon",
    "Australia",
    "Romania",
    "Nepal",
    "El Salvador",
    "Egypt",
    "Serbia",
    "South Africa",
    "Colombia",
    "Hungary",
    "Other Central Africa",
    "Pakistan",
    "Moldova",
    "United Kingdom",
    "Vietnam",
    "Netherlands",
    "Other South America",
    ### Totals
    "Total Households",
    "Total Household Population",
    "Total Household Population 15 Years Or Over",
    "Total Population",
    ### Education
    "No Certificate, Diploma Or Degree",
    "High School Certificate Or Equivalent",
    "University Certificate Or Diploma Below Bachelor",
    "College, CEGEP Or Other Non-University Certificate Or Diploma",
    "Apprenticeship Or Trades Certificate Or Diploma",
    "Bachelor's Degree",
    "Above Bachelor's",
    ### Marital Status
    "Married (And Not Separated)",
    "Living Common Law",
    "Single (Never Legally Married)",
    "Separated",
    "Divorced",
    "Widowed",
    ### Employment Status
    "Employed",
    "Unemployed",
    "Not In The Labour Force",
]
# age
reg_condition1 = col("Description").rlike(
    "^\\bTotal\\b\\s\\d+\\s(([Tt][Oo]\\s\\d+)|([Oo][Rr]\\s[Oo][Ll][Dd][Ee][Rr]))"
)
# income levels
reg_condition2 = col("Description").rlike(
    "^\\bHousehold Income\\b\\s\\$300[,][0]{3}\\s[oO][rR]\\s[oO][vV][eE][rR].*"
)
reg_condition3 = col("Description").rlike(
    "^\\bHousehold Income\\b\\s((\\$\\d+)|(\\$\\d+[,]\\d+))\\s(([Tt][Oo]\\s\\$\\d+[,]\\d+.*)).*"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC     SELECT DISTINCT Category
# MAGIC     FROM environics.dim_master_demostats WHERE ADD_TO_PROFILE = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from environics.demostats_LDU_2023_cleaned WHERE CODE='N0R1A0'

# COMMAND ----------

df = spark.sql("""select
  *
from
  environics.demostats_LDU_2023_cleaned
where
   Category in (
     'Basics',
    "Total Population by Age",
    "Households by Size of Household",
    "Households by Income (Current Year)",
    "Household Population by Total Immigrants and Place of Birth",
    "Household Population by Visible Minority Status",
    "Male Population by Age",
    "Female Population by Age",
    "Household Population 15 Years or Over by Educational Attainment",
    "Household Population 15 Years or Over by Marital Status",
    "Household Population 15 Years or Over by Labour Force Activity",
    'Household Population by Mother Tongue'
  ) AND PRABB in ('ON', 'MB','QC')
""").filter(col('Description').isin(filter_conditions)|reg_condition1|reg_condition2|reg_condition3)
display(df)

# COMMAND ----------

# remove all the special characters in Description column
cleaned_df = (
    df.withColumn(
        "Description", regexp_replace("Description", " \\(Current Year \\$\\)", "")
    )
    .withColumn(
        "Description", regexp_replace("Description", "Côte d'Ivoire", "Cote d Ivoire")
    )
    .withColumn(
        "Description", regexp_replace("Description", r"[$,.\-" + "'" + r"()]", "")
    )
    )

# COMMAND ----------

# cleaned_df_2018 = cleaned_df.filter(col('YEAR')=='2018')
# pivot_df_2018 = (
#     cleaned_df_2018.groupBy("CODE")
#     .pivot("Description")
#     .agg(first("value"))
#     .withColumnRenamed("CODE","POSTALCODE")
# )
cleaned_df_2023 = cleaned_df.filter(col('YEAR')=='2023')
pivot_df_2023 = (
    cleaned_df_2023.filter(col('YEAR')=='2023').groupBy("CODE")
    .pivot("Description")
    .agg(first("value"))
    .withColumnRenamed("CODE","POSTALCODE")
)
# cleaned_df_2026 = cleaned_df.filter(col('YEAR')=='2026')
# pivot_df_2026 = (
#     cleaned_df_2026.filter(col('YEAR')=='2026').groupBy("CODE")
#     .pivot("Description")
#     .agg(first("value"))
#     .withColumnRenamed("CODE","POSTALCODE")
# )
# cleaned_df_2028 = cleaned_df.filter(col('YEAR')=='2028')
# pivot_df_2028 = (
#     cleaned_df_2028.filter(col('YEAR')=='2028').groupBy("CODE")
#     .pivot("Description")
#     .agg(first("value"))
#     .withColumnRenamed("CODE","POSTALCODE")
# )
# cleaned_df_2033 = cleaned_df.filter(col('YEAR')=='2033')
# pivot_df_2033 = (
#     cleaned_df_2033.filter(col('YEAR')=='2033').groupBy("CODE")
#     .pivot("Description")
#     .agg(first("value"))
#     .withColumnRenamed("CODE","POSTALCODE")
# )
# display(pivot_df)

# COMMAND ----------

# collist2018 = list(set(pivot_df_2018.columns) - set(['POSTALCODE', 'Total Household Population', 'Total Household Population 15 Years Or Over', 'Total Households', 'Total Population']))
collist2023 = list(set(pivot_df_2023.columns) - set(['POSTALCODE', 'Total Household Population', 'Total Household Population 15 Years Or Over', 'Total Households', 'Total Population']))
# collist2026 = list(set(pivot_df_2026.columns) - set(['POSTALCODE', 'Total Household Population', 'Total Household Population 15 Years Or Over', 'Total Households', 'Total Population']))
# collist2028 = list(set(pivot_df_2028.columns) - set(['POSTALCODE', 'Total Household Population', 'Total Household Population 15 Years Or Over', 'Total Households', 'Total Population']))
# collist2033 = list(set(pivot_df_2033.columns) - set(['POSTALCODE', 'Total Household Population', 'Total Household Population 15 Years Or Over', 'Total Households', 'Total Population']))

# COMMAND ----------

# Melt the feature columns to rows, leaving the totals as their own columns
# melted_df_2018 = pivot_df_2018.melt(
#     ids=['POSTALCODE'], values=collist2018,
#     variableColumnName="Description", valueColumnName="val"
# ).join(cleaned_df_2018.select('Description', 'Category').distinct(), on='Description', how='left').join(pivot_df_2018.select('POSTALCODE', col('Total Households').alias('TOTAL_HOUSEHOLDS'), col('Total Household Population 15 Years Or Over').alias('TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER'), col('Total Household Population').alias('TOTAL_HOUSEHOLD_POPULATION'), col('Total Population').alias('TOTAL_POPULATION')), on='POSTALCODE', how='inner').select('POSTALCODE', 'Description', 'Category', 'val', 'TOTAL_HOUSEHOLDS', 'TOTAL_HOUSEHOLD_POPULATION', 'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER', 'TOTAL_POPULATION').withColumn('YEAR', lit(2018))
melted_df_2023 = pivot_df_2023.melt(
    ids=['POSTALCODE'], values=collist2023,
    variableColumnName="Description", valueColumnName="val"
).join(cleaned_df_2023.select('Description', 'Category').distinct(), on='Description', how='left').join(pivot_df_2023.select('POSTALCODE', col('Total Households').alias('TOTAL_HOUSEHOLDS'), col('Total Household Population 15 Years Or Over').alias('TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER'), col('Total Household Population').alias('TOTAL_HOUSEHOLD_POPULATION'), col('Total Population').alias('TOTAL_POPULATION')), on='POSTALCODE', how='inner').select('POSTALCODE', 'Description', 'Category', 'val', 'TOTAL_HOUSEHOLDS', 'TOTAL_HOUSEHOLD_POPULATION', 'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER', 'TOTAL_POPULATION').withColumn('YEAR', lit(2023))
# melted_df_2026 = pivot_df_2026.melt(
#     ids=['POSTALCODE'], values=collist2026,
#     variableColumnName="Description", valueColumnName="val"
# ).join(cleaned_df_2026.select('Description', 'Category').distinct(), on='Description', how='left').join(pivot_df_2026.select('POSTALCODE', col('Total Households').alias('TOTAL_HOUSEHOLDS'), col('Total Household Population 15 Years Or Over').alias('TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER'), col('Total Household Population').alias('TOTAL_HOUSEHOLD_POPULATION'), col('Total Population').alias('TOTAL_POPULATION')), on='POSTALCODE', how='inner').select('POSTALCODE', 'Description', 'Category', 'val', 'TOTAL_HOUSEHOLDS', 'TOTAL_HOUSEHOLD_POPULATION', 'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER', 'TOTAL_POPULATION').withColumn('YEAR', lit(2026))
# melted_df_2028 = pivot_df_2028.melt(
#     ids=['POSTALCODE'], values=collist2028,
#     variableColumnName="Description", valueColumnName="val"
# ).join(cleaned_df_2028.select('Description', 'Category').distinct(), on='Description', how='left').join(pivot_df_2028.select('POSTALCODE', col('Total Households').alias('TOTAL_HOUSEHOLDS'), col('Total Household Population 15 Years Or Over').alias('TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER'), col('Total Household Population').alias('TOTAL_HOUSEHOLD_POPULATION'), col('Total Population').alias('TOTAL_POPULATION')), on='POSTALCODE', how='inner').select('POSTALCODE', 'Description', 'Category', 'val', 'TOTAL_HOUSEHOLDS', 'TOTAL_HOUSEHOLD_POPULATION', 'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER', 'TOTAL_POPULATION').withColumn('YEAR', lit(2028))
# melted_df_2033 = pivot_df_2033.melt(
#     ids=['POSTALCODE'], values=collist2033,
#     variableColumnName="Description", valueColumnName="val"
# ).join(cleaned_df_2033.select('Description', 'Category').distinct(), on='Description', how='left').join(pivot_df_2033.select('POSTALCODE', col('Total Households').alias('TOTAL_HOUSEHOLDS'), col('Total Household Population 15 Years Or Over').alias('TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER'), col('Total Household Population').alias('TOTAL_HOUSEHOLD_POPULATION'), col('Total Population').alias('TOTAL_POPULATION')), on='POSTALCODE', how='inner').select('POSTALCODE', 'Description', 'Category', 'val', 'TOTAL_HOUSEHOLDS', 'TOTAL_HOUSEHOLD_POPULATION', 'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER', 'TOTAL_POPULATION').withColumn('YEAR', lit(2033))
# melted_df = melted_df_2018.union(melted_df_2023)
# melted_df = melted_df_2018.union(melted_df_2023).union(melted_df_2026).union(melted_df_2028).union(melted_df_2033)
# melted_df.show()

# COMMAND ----------

# DBTITLE 1,Socialvalues extract the total as column
SV_totals = SV_2023_cleaned.filter(col('Description')=='Household Population 15+').select('Variable', 'CODE', col('value').alias('TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER'))
w2 = Window.partitionBy("CODE").orderBy('Variable')
SV_totals = SV_totals.withColumn("row",row_number().over(w2)) \
  .filter(col("row") == 1).drop("row").select('Code', 'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER')
SV_2023_melted = SV_2023_cleaned.filter(col('Description')!='Household Population 15+')
SV_2023_melted = SV_2023_melted.join(SV_totals, on=['CODE'], how='left')
SV_2023_melted = SV_2023_melted.withColumn('TOTAL_HOUSEHOLDS', lit(np.nan)).withColumn('TOTAL_HOUSEHOLD_POPULATION', lit(np.nan)).withColumn('TOTAL_POPULATION', lit(np.nan)).select(col('CODE').alias('POSTALCODE'), 'Description', 'Category', col('value').alias('val'), 'TOTAL_HOUSEHOLDS', 'TOTAL_HOUSEHOLD_POPULATION', 'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER', 'TOTAL_POPULATION', 'YEAR')
display(SV_2023_melted)

# COMMAND ----------

melted_df_ds_sv = melted_df.union(SV_2023_melted)
# display(melted_df_ds_sv)

# COMMAND ----------

melted_df_ds_sv.write.mode("overwrite").option("overwriteSchema", "True").format("delta").saveAsTable(
    "environics.ldu_feature_vals_melted_df_2023"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hyper.ldu_feature_vals_melted_df_master_variable_0912

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Crossjoin Stores to Postal Codes

# COMMAND ----------



stores = spark.sql(
    '''
SELECT STORES.STORE_NUM STORE_NUM, STORES.`STORE_NAME` STORE_NAME,STORES.`BANNER` BANNER, STORES.`STREET` STORE_STREET, STORES.POSTAL_CODE, STORES.PROVINCE STORE_PROVINCE, STORES.`STORE_CHAIN` CUSTOMER_GROUP, CAST(STORES.LONGITUDE AS DOUBLE) STORE_LONGITUDE, CAST(STORES.LATITUDE AS DOUBLE) STORE_LATITUDE, REVENUE_CY, STORES.`CHANNEL`, STORES.`SALES_GROUP`
FROM hyper.unioned_stores_52weeks_2024_0927  STORES
WHERE STORES.`STORE_CHAIN` IS NOT NULL AND REVENUE_CY IS NOT NULL 
''')
display(stores)

# COMMAND ----------

# DBTITLE 1,List of postal codes and their representative points
LDU = spark.sql(
"""
SELECT POSTALCODE, PROV, FIRST(LONGITUDE) LONGITUDE, FIRST(LATITUDE) LATITUDE
    FROM (SELECT * FROM combined_ldu_p1 /* This file from local Jupyter processing of shapefiles */
    UNION
    SELECT * FROM combined_ldu_p2
    ORDER BY POSTALCODE, AREA DESC)
    GROUP BY POSTALCODE, PROV
"""
)
LDU.show()

# COMMAND ----------

# DBTITLE 1,Adjacent Province Check function
def adjacent_province_check(prov, store_prov):
    """
    Takes two provinces as input, returns True if provinces are identical or geographically adjacent. Otherwise returns False.
    """
    if (store_prov == 'AB'):
        if (prov == 'AB') | (prov == 'BC') | (prov == 'SK') | (prov == 'NT'):
            return True
    elif store_prov == 'BC':
        if (prov == 'AB') | (prov == 'BC') | (prov == 'YT'):
            return True
    elif store_prov == 'SK':
        if (prov == 'AB') | (prov == 'MB') | (prov == 'SK') | (prov == 'NT'):
            return True
    elif store_prov == 'MB':
        if (prov == 'ON') | (prov == 'MB') | (prov == 'SK') | (prov == 'NU'):
            return True
    elif store_prov == 'ON':
        if (prov == 'ON') | (prov == 'MB') | (prov == 'QC'):
            return True
    elif store_prov == 'QC':
        if (prov == 'ON') | (prov == 'NL') | (prov == 'QC') | (prov == 'NB'):
            return True
    elif store_prov == 'NB':
        if (prov == 'QC') | (prov == 'NS') | (prov == 'NB') | (prov == 'PE'):
            return True
    elif store_prov == 'NL':
        if (prov == 'NL') | (prov == 'QC') | (prov == 'PE'):
            return True
    elif store_prov == 'YT':
        if (prov == 'YT') | (prov == 'NT') | (prov == 'BC'):
            return True
    elif store_prov == 'NS':
        if (prov == 'NS') | (prov == 'NB') | (prov == 'PE'):
            return True
    elif store_prov == 'NT':
        if (prov == 'NT') | (prov == 'YT') | (prov == 'NU') | (prov == 'AB') | (prov == 'SK'):
            return True
    elif store_prov == 'NU':
        if (prov == 'NT') | (prov == 'NU') | (prov == 'MB'):
            return True
    elif store_prov == 'PE':
        if (prov == 'PE') | (prov == 'NS') | (prov == 'NB'):
            return True
    else:
        return False
adj_prov_check_udf = udf(adjacent_province_check, BooleanType())

# COMMAND ----------

# Check the adjacent province function is working properly
# stores.crossJoin(LDU).filter(adj_prov_check_udf(col('PROV'), col('STORE_PROVINCE'))==True).select('STORE_PROVINCE', 'PROV').distinct().orderBy('STORE_PROVINCE', 'PROV').collect()

# COMMAND ----------

# DBTITLE 1,Calculate Distance function
def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Takes latitude and longitude in degrees for two points, returns the Haversine distance (distance between two points on a sphere).
    """
    # Distance between latitudes and longitudes
    dLat = (lat2 - lat1) * np.pi / 180.0
    dLon = (lon2 - lon1) * np.pi / 180.0
    # Convert to radians
    lat1 = (lat1) * np.pi / 180.0
    lat2 = (lat2) * np.pi / 180.0
    r = 6371.009 # mean radius of the Earth
    return float(2*r*np.arcsin(np.sqrt(math.pow(np.sin((dLat)/2), 2)+ (math.pow(np.sin((dLon)/2), 2))*np.cos(lat1)*np.cos(lat2))))
distance_udf = udf(haversine_distance, DoubleType())

# COMMAND ----------

# Cross join the stores to postal codes (Roughly 7k stores * 900k LDUs = 6 bil records)
# Prefilter to reduce number of records before crossjoin
joined_df = stores.crossJoin(LDU).filter(adj_prov_check_udf(col('PROV'), col('STORE_PROVINCE'))==True)
# Calculate the haversine distance between each store and LDU coordinates
joined_df = joined_df.withColumn('HAVERSINE_DISTANCE', distance_udf(col('STORE_LATITUDE'), col('STORE_LONGITUDE'), col('LATITUDE'), col('LONGITUDE')))
# Filter to reduce number of records after crossjoin
joined_df = joined_df.withColumn('Attraction', col('REVENUE_CY')/col('HAVERSINE_DISTANCE')**2)
#### joined_df = joined_df.withColumn('Attraction', col('SALES')/col('HAVERSINE_DISTANCE')**2) 
# Calculate Total Attraction for each POSTALCODE
windowPartition = Window.partitionBy(['POSTALCODE'])#['STORE_NUM', 'CUSTOMER_GROUP'])
joined_df = joined_df.withColumn("TOTAL_POSTALCODE_ATTRACTION", sum('Attraction').over(windowPartition)) 
# Calculate running total of store-LDU attraction per store
joined_df = joined_df.withColumn('CUMULATIVE_PERCENT_ATTRACTION', sum(col('Attraction')/col('TOTAL_POSTALCODE_ATTRACTION')).over(windowPartition.orderBy((col('Attraction')/col('TOTAL_POSTALCODE_ATTRACTION')).asc()).rowsBetween(Window.unboundedPreceding, 0))).select('STORE_NUM', 'CUSTOMER_GROUP', 'STORE_NAME','CHANNEL','SALES_GROUP','BANNER', 'STORE_STREET', 'STORE_PROVINCE', 'POSTALCODE', 'STORE_LATITUDE','STORE_LONGITUDE','LATITUDE', 'LONGITUDE', 'PROV', 
              'Attraction', 'TOTAL_POSTALCODE_ATTRACTION', 'CUMULATIVE_PERCENT_ATTRACTION', 'REVENUE_CY', 'HAVERSINE_DISTANCE')
# Filter to exclude lower 1% running total of LDUs, PER STORE to reduce output size to ~500mil
# PERCENT_ATTRACTION for each store adds up to 100%
# joined_df_filtered = joined_df.filter(joined_df.CUMULATIVE_PERCENT_ATTRACTION > 0.01).withColumn('PERCENT_ATTRACTION', col('Attraction')/sum('Attraction').over(windowPartition)).select('STORE_NUM', 'CUSTOMER_GROUP', 'STORE_NAME', 'STORE_STREET', 'STORE_CITY', 'STORE_PROVINCE', 'POSTALCODE', 'LATITUDE', 'LONGITUDE', 'PROV', 
#               'Attraction', 'TOTAL_STORE_ATTRACTION', 'CUMULATIVE_PERCENT_ATTRACTION', 'PERCENT_ATTRACTION', 'DNNSI_IMPUTED', 'HAVERSINE_DISTANCE')
display(joined_df)

# COMMAND ----------

joined_df.write.mode("overwrite").option("overwriteSchema", "True").format("delta").saveAsTable(
    "hyper.store_LDU_attraction_0927"
)

# COMMAND ----------

joined_df_1 = stores.crossJoin(LDU).filter(adj_prov_check_udf(col('PROV'), col('STORE_PROVINCE'))==True)
# Calculate the haversine distance between each store and LDU coordinates
joined_df_1 = joined_df_1.withColumn('HAVERSINE_DISTANCE', distance_udf(col('STORE_LATITUDE'), col('STORE_LONGITUDE'), col('LATITUDE'), col('LONGITUDE')))
# Filter to reduce number of records after crossjoin
joined_df_1 = joined_df_1.withColumn('Attraction', col('DNNSI_IMPUTED')/col('HAVERSINE_DISTANCE')**2)
windowPartition = Window.partitionBy(['POSTALCODE'])#['STORE_NUM', 'CUSTOMER_GROUP'])
joined_df_filtered = joined_df.withColumn("TOTAL_POSTALCODE_ATTRACTION", sum('Attraction').over(windowPartition) - max('Attraction').over(windowPartition)) #excluding the max value (since this would be the postal code in which the store resides)
# Calculate running total of store-LDU attraction per store
joined_df_filtered = joined_df_filtered.withColumn('CUMULATIVE_PERCENT_ATTRACTION', sum(col('Attraction')/col('TOTAL_POSTALCODE_ATTRACTION')).over(windowPartition.orderBy((col('Attraction')/col('TOTAL_POSTALCODE_ATTRACTION')).asc()).rowsBetween(Window.unboundedPreceding, 0))).select('STORE_NUM', 'CUSTOMER_GROUP', 'STORE_NAME', 'STORE_STREET', 'STORE_CITY', 'STORE_PROVINCE', 'POSTALCODE', 'LATITUDE', 'LONGITUDE', 'PROV', 
              'Attraction', 'TOTAL_POSTALCODE_ATTRACTION', 'CUMULATIVE_PERCENT_ATTRACTION', 'DNNSI_IMPUTED', 'HAVERSINE_DISTANCE')

# COMMAND ----------

filtered_df = joined_df_filtered.filter(joined_df_filtered['STORE_CITY'] == 'TORONTO')
# display(filtered_df)

# COMMAND ----------

filtered_df_costco = filtered_df.filter(joined_df_filtered['CUSTOMER_GROUP'] == 'CO_COSTCO CENTRAL')
display(filtered_df_costco)

# COMMAND ----------

num_rows = joined_df_filtered.count()
num_columns = len(joined_df_filtered.columns)
(num_rows, num_columns)

# COMMAND ----------


filtered_df_costco = filtered_df.filter(joined_df_filtered['CUSTOMER_GROUP'] == 'CO_COSTCO CENTRAL')
filtered_df_costco_one = filtered_df_costco.filter(filtered_df_costco['STORE_NAME'] == 'COSTCO WHOLESALE #0524')
display(filtered_df_costco_one)

# COMMAND ----------

from pyspark.sql.functions import col

filtered_df_shoppers = joined_df_filtered.filter(
    (col('STORE_NAME') == 'NO FRILLS TORONTO #3643') 
)
display(filtered_df_shoppers)

# COMMAND ----------


not_filtered_df_shoppers = joined_df_not_filtered.filter(
    (col('STORE_NAME') == 'NO FRILLS TORONTO #3643') 
)
display(not_filtered_df_shoppers)

# COMMAND ----------

joined_df = stores.crossJoin(LDU).filter(adj_prov_check_udf(col('PROV'), col('STORE_PROVINCE'))==True)
# Calculate the haversine distance between each store and LDU coordinates
joined_df = joined_df.withColumn('HAVERSINE_DISTANCE', distance_udf(col('STORE_LATITUDE'), col('STORE_LONGITUDE'), col('LATITUDE'), col('LONGITUDE')))
# Filter to reduce number of records after crossjoin
joined_df = joined_df.withColumn('Attraction', col('DNNSI_IMPUTED')/col('HAVERSINE_DISTANCE')**2)
windowPartition = Window.partitionBy(['POSTALCODE'])
joined_df = joined_df.withColumn('Max_Attraction', max('Attraction').over(windowPartition))

display(joined_df)

# COMMAND ----------

# Cross join the stores to postal codes (Roughly 7k stores * 900k LDUs = 6 bil records)
# Prefilter to reduce number of records before crossjoin
joined_df_with_max = stores.crossJoin(LDU).filter(adj_prov_check_udf(col('PROV'), col('STORE_PROVINCE'))==True)
# Calculate the haversine distance between each store and LDU coordinates
joined_df_with_max = joined_df_with_max.withColumn('HAVERSINE_DISTANCE', distance_udf(col('STORE_LATITUDE'), col('STORE_LONGITUDE'), col('LATITUDE'), col('LONGITUDE')))
# Filter to reduce number of records after crossjoin
joined_df_with_max = joined_df_with_max.withColumn('Attraction', col('DNNSI_IMPUTED')/col('HAVERSINE_DISTANCE')**2)
#### joined_df = joined_df.withColumn('Attraction', col('SALES')/col('HAVERSINE_DISTANCE')**2) 
# Calculate Total Attraction for each POSTALCODE
windowPartition = Window.partitionBy(['POSTALCODE'])#['STORE_NUM', 'CUSTOMER_GROUP'])
joined_df_with_max = joined_df.withColumn("TOTAL_POSTALCODE_ATTRACTION", sum('Attraction').over(windowPartition)) # - max('Attraction').over(windowPartition)) #excluding the max value (since this would be the postal code in which the store resides)
# Calculate running total of store-LDU attraction per store
joined_df_with_max = joined_df_with_max.withColumn('CUMULATIVE_PERCENT_ATTRACTION', sum(col('Attraction')/col('TOTAL_POSTALCODE_ATTRACTION')).over(windowPartition.orderBy((col('Attraction')/col('TOTAL_POSTALCODE_ATTRACTION')).asc()).rowsBetween(Window.unboundedPreceding, 0))).select('STORE_NUM', 'CUSTOMER_GROUP', 'STORE_NAME', 'STORE_STREET', 'STORE_CITY', 'STORE_PROVINCE', 'POSTALCODE', 'LATITUDE', 'LONGITUDE', 'PROV', 
              'Attraction', 'TOTAL_POSTALCODE_ATTRACTION', 'CUMULATIVE_PERCENT_ATTRACTION', 'DNNSI_IMPUTED', 'HAVERSINE_DISTANCE')
# Filter to exclude lower 1% running total of LDUs, PER STORE to reduce output size to ~500mil
# PERCENT_ATTRACTION for each store adds up to 100%
# joined_df_filtered = joined_df.filter(joined_df.CUMULATIVE_PERCENT_ATTRACTION > 0.01).withColumn('PERCENT_ATTRACTION', col('Attraction')/sum('Attraction').over(windowPartition)).select('STORE_NUM', 'CUSTOMER_GROUP', 'STORE_NAME', 'STORE_STREET', 'STORE_CITY', 'STORE_PROVINCE', 'POSTALCODE', 'LATITUDE', 'LONGITUDE', 'PROV', 
#               'Attraction', 'TOTAL_STORE_ATTRACTION', 'CUMULATIVE_PERCENT_ATTRACTION', 'PERCENT_ATTRACTION', 'DNNSI_IMPUTED', 'HAVERSINE_DISTANCE')
display(joined_df_with_max)

# COMMAND ----------

joined_result = stores.crossJoin(LDU).filter(adj_prov_check_udf(col('PROV'), col('STORE_PROVINCE'))==True)
display(joined_result)

# COMMAND ----------

# MAGIC %sql
# MAGIC select STORE_NUM,Attraction,TOTAL_POSTALCODE_ATTRACTION, CUMULATIVE_PERCENT_ATTRACTION from store_LDU_attraction
# MAGIC where POSTALCODE= 'A0G2J0'

# COMMAND ----------

# DBTITLE 1,Filter to exclude lower 1% running total of LDUs per store to reduce output size
joined_df_temp = spark.sql("""
                            SELECT * FROM hyper.store_LDU_attraction_0927
                            WHERE Attraction > 0.005 * TOTAL_POSTALCODE_ATTRACTION
                            """)
windowPartition = Window.partitionBy(['POSTALCODE'])#['STORE_NUM', 'CUSTOMER_GROUP'])
joined_df_filtered = joined_df_temp.withColumn('PERCENT_ATTRACTION', col('Attraction')/sum('Attraction').over(windowPartition)).select('STORE_NUM', 'CUSTOMER_GROUP', 'STORE_NAME','CHANNEL','SALES_GROUP','BANNER', 'STORE_STREET', 'STORE_PROVINCE', 'POSTALCODE', 'STORE_LATITUDE','STORE_LONGITUDE','LATITUDE', 'LONGITUDE', 'PROV', 'Attraction', 'TOTAL_POSTALCODE_ATTRACTION', 'CUMULATIVE_PERCENT_ATTRACTION', 'PERCENT_ATTRACTION', 'REVENUE_CY', 'HAVERSINE_DISTANCE')

# COMMAND ----------

joined_df_filtered.write.mode("overwrite").option("overwriteSchema", "True").format("delta").saveAsTable(
    "hyper.store_LDU_attraction_f_halfp_0927"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from environics.ldu_feature_vals_melted_df_master_variable_0826_2

# COMMAND ----------

joined_df_temp = spark.sql("""
                            SELECT * FROM hyper.store_LDU_attraction_0910
                            WHERE Attraction > 0.01 * TOTAL_POSTALCODE_ATTRACTION
                            """)
windowPartition = Window.partitionBy(['POSTALCODE'])#['STORE_NUM', 'CUSTOMER_GROUP'])
joined_df_filtered = joined_df_temp.withColumn('PERCENT_ATTRACTION', col('Attraction')/sum('Attraction').over(windowPartition)).select('STORE_NUM', 'CUSTOMER_GROUP', 'STORE_NAME','CHANNEL','SALES_GROUP','BANNER', 'STORE_STREET', 'STORE_PROVINCE', 'POSTALCODE', 'STORE_LATITUDE','STORE_LONGITUDE','LATITUDE', 'LONGITUDE', 'PROV', 'Attraction', 'TOTAL_POSTALCODE_ATTRACTION', 'CUMULATIVE_PERCENT_ATTRACTION', 'PERCENT_ATTRACTION', 'REVENUE_CY', 'HAVERSINE_DISTANCE')

joined_df_filtered.write.mode("overwrite").option("overwriteSchema", "True").format("delta").saveAsTable(
    "hyper.store_LDU_attraction_fullp_filtered_0910"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## =================
# MAGIC The rest of code better running on Variable notebook

# COMMAND ----------

# DBTITLE 1,Relevant Postal Codes for one store
# MAGIC %sql
# MAGIC -- This allows us to create a store profile based on the weighted avg. of attraction for surrounding postal codes
# MAGIC SELECT A.*, B.geometry, Attraction/TOTAL_STORE_ATTRACTION PERCENT_ATTRACTION FROM environics.store_ldu_attraction_filtered A
# MAGIC LEFT JOIN (SELECT * FROM combined_ldu_p1
# MAGIC     UNION
# MAGIC     SELECT * FROM combined_ldu_p2) B ON A.POSTALCODE = B.POSTALCODE
# MAGIC WHERE STORE_STREET LIKE "%FORT YORK%" AND CUSTOMER_GROUP = 'SO_SOBEYS ONTARIO'
# MAGIC ORDER BY CUMULATIVE_PERCENT_ATTRACTION DESC

# COMMAND ----------

# DBTITLE 1,Relevant stores for L5M6P4
# %sql
# -- This allows us to see which stores are competing with one another in a given area
# SELECT T1.*, Attraction/TOTAL_LDU_ATTRACTION ATTRACTION, STORES.STORE_LATITUDE, STORES.STORE_LONGITUDE FROM store_LDU_attraction T1
# LEFT JOIN (SELECT DISTINCT Customer STORE_NUM, `Customer Name` STORE_NAME, `Address1` STORE_STREET, City STORE_CITY, State STORE_PROVINCE, `Customer Group` CUSTOMER_GROUP, CAST(Longitude AS DOUBLE) STORE_LONGITUDE, CAST(Latitude AS DOUBLE) STORE_LATITUDE
# FROM default.store_dnnsi_past_52_weeks) STORES ON T1.STORE_NUM = STORES.STORE_NUM AND T1.CUSTOMER_GROUP = STORES.CUSTOMER_GROUP
# LEFT JOIN (SELECT POSTALCODE, SUM(Attraction) TOTAL_LDU_ATTRACTION FROM store_LDU_attraction GROUP BY POSTALCODE) T2 ON T1.POSTALCODE = T2.POSTALCODE
# WHERE T1.POSTALCODE LIKE "L5M6P4" AND Attraction/TOTAL_LDU_ATTRACTION > 0.01
# ORDER BY ATTRACTION DESC
# LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT T1.STORE_NUM, T1.CUSTOMER_GROUP, T1.STORE_NAME, T1.STORE_STREET, T1.STORE_PROVINCE, T1.POSTALCODE, Attraction/TOTAL_LDU_ATTRACTION ATTRACTION, T1.LATITUDE, T1.LONGITUDE, T1.REVENUE_CY,T1.HAVERSINE_DISTANCE FROM environics.store_LDU_attraction_0719 T1
# MAGIC LEFT JOIN (SELECT DISTINCT Customer STORE_NUM, `Customer Name` STORE_NAME, `Address1` STORE_STREET, City STORE_CITY, State STORE_PROVINCE, `Customer Group` CUSTOMER_GROUP, CAST(Longitude AS DOUBLE) STORE_LONGITUDE, CAST(Latitude AS DOUBLE) STORE_LATITUDE
# MAGIC FROM default.store_dnnsi_past_52_weeks) STORES ON T1.STORE_NUM = STORES.STORE_NUM AND T1.CUSTOMER_GROUP = STORES.CUSTOMER_GROUP
# MAGIC LEFT JOIN (SELECT POSTALCODE, SUM(Attraction) TOTAL_LDU_ATTRACTION FROM environics.store_LDU_attraction_0719 GROUP BY POSTALCODE) T2 ON T1.POSTALCODE = T2.POSTALCODE
# MAGIC WHERE T1.POSTALCODE LIKE "M5A1L1" 
# MAGIC ORDER BY ATTRACTION DESC
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,Calculate Store Profile for one store
# MAGIC %sql
# MAGIC -- Join the store_ldu_attraction table with the ldu_feature_vals_melted_df table to get the store profile values for Sobeys
# MAGIC SELECT STORE.CUSTOMER_GROUP, STORE.STORE_NAME, STORE.STORE_NUM, STORE.STORE_STREET, STORE.STORE_CITY, STORE.STORE_PROVINCE, MELTED.Description, LMAP.Language_Grouping, MELTED.Category, MELTED.YEAR, SUM(MELTED.val) val, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLDS) TOTAL_HOUSEHOLDS, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION) TOTAL_HOUSEHOLD_POPULATION, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER) TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_POPULATION) TOTAL_POPULATION, 
# MAGIC CASE WHEN MELTED.Category LIKE "Households by%" THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLDS)
# MAGIC     WHEN MELTED.Category LIKE "Household Population 15 Years or Over%" OR MELTED.Category IN ('Trends', 'Attitudes') THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC     WHEN MELTED.Category LIKE "Household Population%" AND MELTED.Category NOT LIKE "%15 Years or Over%" THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION)
# MAGIC     ELSE SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_POPULATION) END STORE_VALUE
# MAGIC FROM default.store_ldu_attraction_filtered_0626 STORE
# MAGIC LEFT JOIN default.ldu_feature_vals_melted_df MELTED ON STORE.POSTALCODE = MELTED.POSTALCODE
# MAGIC LEFT JOIN default.language_mapping LMAP ON MELTED.Description = LMAP.Description_cleaned
# MAGIC WHERE CUSTOMER_GROUP LIKE "%SOBEYS%" AND STORE_STREET LIKE "%FORT YORK%" AND MELTED.val IS NOT NULL
# MAGIC GROUP BY STORE.CUSTOMER_GROUP, STORE.STORE_NAME, STORE.STORE_STREET, STORE.STORE_CITY, STORE.STORE_PROVINCE, MELTED.Description, LMAP.Language_Grouping, MELTED.Category, YEAR

# COMMAND ----------

# DBTITLE 1,Calculate Store Profiles

store_profiles = spark.sql("""
SELECT STORE.CUSTOMER_GROUP, MELTED.Description, LMAP.Language_Grouping, MELTED.Category, MELTED.YEAR, 
SUM(STORE.PERCENT_ATTRACTION*MELTED.val) val, 
SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLDS) TOTAL_HOUSEHOLDS, 
SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION) TOTAL_HOUSEHOLD_POPULATION, 
SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER) TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER, 
SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_POPULATION) TOTAL_POPULATION,

CASE WHEN MELTED.Category LIKE "Households by%" THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLDS)
    WHEN MELTED.Category LIKE "Household Population 15 Years or Over%"  THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
    WHEN MELTED.Category LIKE "Household Population%" AND MELTED.Category NOT LIKE "%15 Years or Over%" THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION)
    ELSE SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_POPULATION) END BANNER_VALUE
FROM environics.store_LDU_attraction_filtered_0801 STORE 
LEFT JOIN environics.ldu_feature_vals_melted_df_2023 MELTED ON STORE.POSTALCODE = MELTED.POSTALCODE
LEFT JOIN default.language_mapping LMAP ON MELTED.Description = LMAP.Description_cleaned
WHERE MELTED.Category NOT IN ('Trends', 'Attitudes') and CUSTOMER_GROUP LIKE "%SOBEYS%" AND STORE_STREET LIKE "%FORT YORK%" AND MELTED.val IS NOT NULL
GROUP BY STORE.CUSTOMER_GROUP, MELTED.Description,LMAP.Language_Grouping,  MELTED.Category, YEAR
""")

# COMMAND ----------

store_profiles_df = spark.sql("""
SELECT STORE.CUSTOMER_GROUP, STORE.STORE_NAME, STORE.STORE_NUM, STORE.CHANNEL, STORE.SALES_GROUP,STORE.BANNER,STORE.STORE_STREET, STORE.STORE_PROVINCE,STORE.STORE_LATITUDE,STORE.STORE_LONGITUDE, MELTED.variable, MELTED.Description, LMAP.Language_Grouping, MELTED.Category, MELTED.YEAR,SUM(STORE.PERCENT_ATTRACTION*MELTED.val)  val, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLDS) TOTAL_HOUSEHOLDS, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION) TOTAL_HOUSEHOLD_POPULATION, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER) TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_POPULATION) TOTAL_POPULATION, first(MELTED.Hierarchy_Description) as Hierarchy_Description, first(MELTED.MEDIAN_AVERAGE) as MEDIAN_AVERAGE, first(MELTED.ADD_TO_PROFILE) as ADD_TO_PROFILE, first(MELTED.LEAF_NODE_FLAG) as LEAF_NODE_FLAG, first(MELTED.TOTAL_FLAG) as TOTAL_FLAG,

CASE WHEN MELTED.Category LIKE "Households by%" THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLDS)
    WHEN MELTED.Category LIKE "Household Population 15 Years or Over%"  THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
    WHEN MELTED.Category LIKE "Household Population%" AND MELTED.Category NOT LIKE "%15 Years or Over%" THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION)
    ELSE SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_POPULATION) END STORE_VALUE
FROM hyper.store_LDU_attraction_f_halfp_0927 STORE 
LEFT JOIN hyper.ldu_feature_vals_melted_df_master_variable_0920 MELTED ON STORE.POSTALCODE = MELTED.POSTALCODE
LEFT JOIN default.language_mapping LMAP ON MELTED.variable = LMAP.Variable
WHERE MELTED.Category NOT IN ('Trends', 'Attitudes') AND MELTED.val IS NOT NULL
GROUP BY STORE.CUSTOMER_GROUP, STORE.STORE_NAME,STORE.STORE_NUM,  STORE.CHANNEL, STORE.SALES_GROUP, STORE.BANNER,STORE.STORE_STREET,STORE.STORE_LONGITUDE,STORE.STORE_LATITUDE,  STORE.STORE_PROVINCE, MELTED.variable,MELTED.Description,LMAP.Language_Grouping,  MELTED.Category, YEAR
"""
)


# COMMAND ----------

display(store_profiles_df)

# COMMAND ----------


store_profiles_df = store_profiles_df.withColumn(
    "STORE_CHAIN",
    when((col("CUSTOMER_GROUP") == 'AOL_AO LOCAL') | (col("CUSTOMER_GROUP") == 'AON_AO NATL RETAIL'), regexp_replace(col("STORE_NAME"), r"#\d+\s*[A-Z]*\s*[A-Z]*\s*", ""))
    .otherwise(col("CUSTOMER_GROUP"))
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE environics.demostats_store_profiles_aggregated AS
# MAGIC SELECT STORE.CUSTOMER_GROUP, STORE.STORE_NAME, STORE.STORE_NUM, STORE.CHANNEL, STORE.SALES_GROUP,STORE.STORE_STREET, STORE.STORE_PROVINCE, MELTED.Description, LMAP.Language_Grouping, MELTED.Category, MELTED.YEAR,SUM(STORE.PERCENT_ATTRACTION*MELTED.val)  val, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLDS) TOTAL_HOUSEHOLDS, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION) TOTAL_HOUSEHOLD_POPULATION, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER) TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_POPULATION) TOTAL_POPULATION,
# MAGIC CASE WHEN MELTED.Category LIKE "Households by%" THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLDS)
# MAGIC     WHEN MELTED.Category LIKE "Household Population 15 Years or Over%"  THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC     WHEN MELTED.Category LIKE "Household Population%" AND MELTED.Category NOT LIKE "%15 Years or Over%" THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION)
# MAGIC     ELSE SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_POPULATION) END STORE_VALUE
# MAGIC FROM environics.store_LDU_attraction_filtered_0801 STORE 
# MAGIC LEFT JOIN environics.ldu_feature_vals_melted_df_2023 MELTED ON STORE.POSTALCODE = MELTED.POSTALCODE
# MAGIC LEFT JOIN default.language_mapping LMAP ON MELTED.Description = LMAP.Description_cleaned
# MAGIC WHERE MELTED.Category NOT IN ('Trends', 'Attitudes') AND MELTED.val IS NOT NULL
# MAGIC GROUP BY STORE.CUSTOMER_GROUP, STORE.STORE_NAME,STORE.STORE_NUM,  STORE.CHANNEL, STORE.SALES_GROUP, STORE.STORE_STREET,  STORE.STORE_PROVINCE, MELTED.Description,LMAP.Language_Grouping,  MELTED.Category, YEAR

# COMMAND ----------



store_profiles_df = store_profiles_df.withColumn(
    'Region',
    when(col("STORE_PROVINCE") == 'ON', 'ONTARIO')
    .when(col("STORE_PROVINCE") == 'QC', 'QUEBEC')
    .when(col("STORE_PROVINCE").isin(['AB', 'BC', 'MB', 'SK', 'YT', 'NT']), 'WEST')
    .when(col("STORE_PROVINCE").isin(['NS', 'NB', 'NL', 'PE']), 'EAST')
    .otherwise(col("STORE_PROVINCE"))
)

# COMMAND ----------

windowPartition = Window.partitionBy(['CUSTOMER_GROUP_MODIFIED','Description'])
store_profiles_df = store_profiles_df.withColumn("BANNER_VALUE", sum('val').over(windowPartition)/sum('TOTAL_POPULATION').over(windowPartition))

windowPartition = Window.partitionBy(['Category','CUSTOMER_GROUP_MODIFIED','STORE_NAME'])
store_profiles_df = store_profiles_df.withColumn('BANNER_VALUE_PERCENT', col('BANNER_VALUE')/sum('BANNER_VALUE').over(windowPartition)).select('CUSTOMER_GROUP_MODIFIED',	'STORE_NAME',	'STORE_NUM',	'CHANNEL',	'SALES_GROUP',	'STORE_STREET',	'STORE_PROVINCE',	'Description',	'Language_Grouping',	'Category',	'YEAR',	'val',	'TOTAL_HOUSEHOLDS',	'TOTAL_HOUSEHOLD_POPULATION',	'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER',	'TOTAL_POPULATION',	'STORE_VALUE',	'BANNER_VALUE_PERCENT')


# COMMAND ----------

windowPartition = Window.partitionBy(['STORE_PROVINCE', 'CHANNEL','Description'])
windowPartition2 = Window.partitionBy(['STORE_PROVINCE', 'CHANNEL','STORE_NUM','Description'])
store_profiles_df = store_profiles_df.withColumn("CHANNEL_PROV_VALUE", sum('val').over(windowPartition)/sum('TOTAL_POPULATION').over(windowPartition))
# display(store_profiles_df)

windowPartition = Window.partitionBy(['STORE_PROVINCE', 'CHANNEL','STORE_NAME','Category'])
store_profiles_df = store_profiles_df.withColumn('CHANNEL_PROV_VALUE_PERCENT', col('CHANNEL_PROV_VALUE')/sum('CHANNEL_PROV_VALUE').over(windowPartition)).select('CUSTOMER_GROUP_MODIFIED',	'STORE_NAME',	'STORE_NUM',	'CHANNEL',	'SALES_GROUP',	'STORE_STREET',	'STORE_PROVINCE',	'Description',	'Language_Grouping',	'Category',	'YEAR',	'val',	'TOTAL_HOUSEHOLDS',	'TOTAL_HOUSEHOLD_POPULATION',	'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER',	'TOTAL_POPULATION',	'STORE_VALUE',	'BANNER_VALUE_PERCENT','CHANNEL_PROV_VALUE_PERCENT')


# COMMAND ----------

windowPartition = Window.partitionBy(['STORE_PROVINCE','Description'])
windowPartition2 = Window.partitionBy(['STORE_PROVINCE','STORE_NUM','Description'])
store_profiles_df = store_profiles_df.withColumn("PROVINCE_VALUE", sum('val').over(windowPartition)/sum('TOTAL_POPULATION').over(windowPartition))

windowPartition = Window.partitionBy(['STORE_PROVINCE','STORE_NAME','Category'])
store_profiles_df = store_profiles_df.withColumn('PROV_VALUE_PERCENT', col('PROVINCE_VALUE')/sum('PROVINCE_VALUE').over(windowPartition)).select('CUSTOMER_GROUP_MODIFIED',	'STORE_NAME',	'STORE_NUM',	'CHANNEL',	'SALES_GROUP',	'STORE_STREET',	'STORE_PROVINCE',	'Description',	'Language_Grouping',	'Category',	'YEAR',	'val',	'TOTAL_HOUSEHOLDS',	'TOTAL_HOUSEHOLD_POPULATION',	'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER',	'TOTAL_POPULATION',	'STORE_VALUE',	'BANNER_VALUE_PERCENT','CHANNEL_PROV_VALUE_PERCENT','PROV_VALUE_PERCENT')
# display(store_profiles_df)

# COMMAND ----------

windowPartition = Window.partitionBy(['CHANNEL','Description'])
windowPartition2 = Window.partitionBy(['STORE_NUM', 'CHANNEL','Description'])#['STORE_NUM', 'CUSTOMER_GROUP'])
store_profiles_df = store_profiles_df.withColumn("CHANNEL_VALUE", sum('val').over(windowPartition)/sum('TOTAL_POPULATION').over(windowPartition))

windowPartition = Window.partitionBy(['CHANNEL','STORE_NAME','Category'])
store_profiles_df = store_profiles_df.withColumn('CHANNEL_VALUE_PERCENT', col('CHANNEL_VALUE')/sum('CHANNEL_VALUE').over(windowPartition)).select('CUSTOMER_GROUP_MODIFIED',	'STORE_NAME',	'STORE_NUM',	'CHANNEL',	'SALES_GROUP',	'STORE_STREET',	'STORE_PROVINCE',	'Description',	'Language_Grouping',	'Category',	'YEAR',	'val',	'TOTAL_HOUSEHOLDS',	'TOTAL_HOUSEHOLD_POPULATION',	'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER',	'TOTAL_POPULATION',	'STORE_VALUE',	'BANNER_VALUE_PERCENT','CHANNEL_PROV_VALUE_PERCENT','PROV_VALUE_PERCENT','CHANNEL_VALUE_PERCENT')
# display(store_profiles_df)

# COMMAND ----------

store_profiles_df = store_profiles_df.withColumn("LONGITUDE", lit(None).cast('double'))
store_profiles_df = store_profiles_df.withColumn("LATITUDE", lit(None).cast('double'))

windowPartition = Window.partitionBy(['BANNER','variable','YEAR'])
store_profiles_df = store_profiles_df.withColumn("BANNER_VALUE", sum('val').over(windowPartition)/sum('TOTAL_POPULATION').over(windowPartition))

windowPartition = Window.partitionBy(['Category','BANNER','STORE_NAME','YEAR'])
store_profiles_df = store_profiles_df.withColumn('BANNER_VALUE_PERCENT', col('BANNER_VALUE') / sum(when(col('LEAF_NODE_FLAG') == 1, col('BANNER_VALUE')).otherwise(0)).over(windowPartition)).select('STORE_CHAIN','BANNER',	'STORE_NAME',	'STORE_NUM',	'CHANNEL',	'SALES_GROUP',	'STORE_STREET','STORE_LONGITUDE','STORE_LATITUDE'	,'STORE_PROVINCE','Region',	'variable','Description',	'Language_Grouping',	'Category',	'YEAR',	'val',	'TOTAL_HOUSEHOLDS',	'TOTAL_HOUSEHOLD_POPULATION',	'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER',	'TOTAL_POPULATION',	'STORE_VALUE',	'BANNER_VALUE_PERCENT','Hierarchy_Description','MEDIAN_AVERAGE','ADD_TO_PROFILE','LEAF_NODE_FLAG','TOTAL_FLAG')

windowPartition = Window.partitionBy(['Region', 'CHANNEL','variable','YEAR'])
windowPartition2 = Window.partitionBy(['Region', 'CHANNEL','STORE_NUM','variable','YEAR'])
store_profiles_df = store_profiles_df.withColumn("CHANNEL_RE_VALUE", sum('val').over(windowPartition)/sum('TOTAL_POPULATION').over(windowPartition))
# display(store_profiles_df)

windowPartition = Window.partitionBy(['Region', 'CHANNEL','STORE_NAME','Category','YEAR'])
store_profiles_df = store_profiles_df.withColumn('CHANNEL_RE_VALUE_PERCENT', col('CHANNEL_RE_VALUE')/sum(when(col('LEAF_NODE_FLAG') == 1, col('CHANNEL_RE_VALUE')).otherwise(0)).over(windowPartition)).select('STORE_CHAIN',	'BANNER','STORE_NAME',	'STORE_NUM',	'CHANNEL',	'SALES_GROUP',	'STORE_STREET',	'STORE_LONGITUDE','STORE_LATITUDE','STORE_PROVINCE','Region','variable',	'Description',	'Language_Grouping',	'Category',	'YEAR',	'val',	'TOTAL_HOUSEHOLDS',	'TOTAL_HOUSEHOLD_POPULATION',	'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER',	'TOTAL_POPULATION',	'STORE_VALUE',	'BANNER_VALUE_PERCENT','CHANNEL_RE_VALUE_PERCENT','Hierarchy_Description','MEDIAN_AVERAGE','ADD_TO_PROFILE','LEAF_NODE_FLAG','TOTAL_FLAG')


windowPartition = Window.partitionBy(['Region','variable','YEAR'])
windowPartition2 = Window.partitionBy(['Region','STORE_NUM','variable','YEAR'])
store_profiles_df = store_profiles_df.withColumn("REGION_VALUE", sum('val').over(windowPartition)/sum('TOTAL_POPULATION').over(windowPartition))

windowPartition = Window.partitionBy(['Region','STORE_NAME','Category'])
store_profiles_df = store_profiles_df.withColumn('REGION_VALUE_PERCENT', col('REGION_VALUE')/sum(when(col('LEAF_NODE_FLAG') == 1, col('REGION_VALUE')).otherwise(0)).over(windowPartition)).select('STORE_CHAIN','BANNER',	'STORE_NAME',	'STORE_NUM',	'CHANNEL',	'SALES_GROUP',	'STORE_STREET',	'STORE_LONGITUDE','STORE_LATITUDE','STORE_PROVINCE','Region','variable',	'Description',	'Language_Grouping',	'Category',	'YEAR',	'val',	'TOTAL_HOUSEHOLDS',	'TOTAL_HOUSEHOLD_POPULATION',	'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER',	'TOTAL_POPULATION',	'STORE_VALUE',	'BANNER_VALUE_PERCENT','CHANNEL_RE_VALUE_PERCENT','REGION_VALUE_PERCENT','Hierarchy_Description','MEDIAN_AVERAGE','ADD_TO_PROFILE','LEAF_NODE_FLAG','TOTAL_FLAG')
# display(store_profiles_df)

windowPartition = Window.partitionBy(['CHANNEL','variable','YEAR'])
windowPartition2 = Window.partitionBy(['STORE_NUM', 'CHANNEL','variable','YEAR'])#['STORE_NUM', 'CUSTOMER_GROUP'])
store_profiles_df = store_profiles_df.withColumn("CHANNEL_VALUE", sum('val').over(windowPartition)/sum('TOTAL_POPULATION').over(windowPartition))

windowPartition = Window.partitionBy(['CHANNEL','STORE_NAME','Category','YEAR'])
store_profiles_df = store_profiles_df.withColumn('CHANNEL_VALUE_PERCENT', col('CHANNEL_VALUE')/sum(when(col('LEAF_NODE_FLAG') == 1, col('CHANNEL_VALUE')).otherwise(0)).over(windowPartition)).select('STORE_CHAIN','BANNER',	'STORE_NAME',	'STORE_NUM',	'CHANNEL',	'SALES_GROUP',	'STORE_STREET',	'STORE_LONGITUDE','STORE_LATITUDE','STORE_PROVINCE','Region','variable',	'Description',	'Language_Grouping',	'Category',	'YEAR',	'val',	'TOTAL_HOUSEHOLDS',	'TOTAL_HOUSEHOLD_POPULATION',	'TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER',	'TOTAL_POPULATION',	'STORE_VALUE',	'BANNER_VALUE_PERCENT','CHANNEL_RE_VALUE_PERCENT','REGION_VALUE_PERCENT','CHANNEL_VALUE_PERCENT','Hierarchy_Description','MEDIAN_AVERAGE','LEAF_NODE_FLAG','TOTAL_FLAG')
# display(store_profiles_df)


# COMMAND ----------

display(store_profiles_df)

# COMMAND ----------

store_profiles_df.write.mode("overwrite").option("overwriteSchema", "True").format("delta").saveAsTable(
    "hyper.demostats_store_profiles_aggregated_0910"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hyper.demostats_store_profiles_aggregated_0910 where BANNER='NF ON'and STORE_PROVINCE!='ON'

# COMMAND ----------

# MAGIC %md
# MAGIC # Below not used for now

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT STORE.CUSTOMER_GROUP, STORE.STORE_NAME, STORE.STORE_NUM, STORE.CHANNEL, STORE.SALES_GROUP,STORE.STORE_STREET, STORE.STORE_PROVINCE, MELTED.Description, LMAP.Language_Grouping, MELTED.Category, MELTED.YEAR,SUM(STORE.PERCENT_ATTRACTION*MELTED.val)  val, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLDS) TOTAL_HOUSEHOLDS, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION) TOTAL_HOUSEHOLD_POPULATION, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER) TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER, SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_POPULATION) TOTAL_POPULATION,
# MAGIC CASE WHEN MELTED.Category LIKE "Households by%" THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLDS)
# MAGIC     WHEN MELTED.Category LIKE "Household Population 15 Years or Over%"  THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC     WHEN MELTED.Category LIKE "Household Population%" AND MELTED.Category NOT LIKE "%15 Years or Over%" THEN SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_HOUSEHOLD_POPULATION)
# MAGIC     ELSE SUM(STORE.PERCENT_ATTRACTION*MELTED.val)/SUM(STORE.PERCENT_ATTRACTION*MELTED.TOTAL_POPULATION) END STORE_VALUE
# MAGIC FROM environics.store_LDU_attraction_filtered_0801 STORE 
# MAGIC LEFT JOIN environics.ldu_feature_vals_melted_df_2023 MELTED ON STORE.POSTALCODE = MELTED.POSTALCODE
# MAGIC LEFT JOIN default.language_mapping LMAP ON MELTED.Description = LMAP.Description_cleaned
# MAGIC WHERE MELTED.Category NOT IN ('Trends', 'Attitudes') and CUSTOMER_GROUP LIKE "%SOBEYS%" AND STORE_STREET LIKE "%FORT YORK%" AND MELTED.val IS NOT NULL
# MAGIC GROUP BY STORE.CUSTOMER_GROUP, STORE.STORE_NAME,STORE.STORE_NUM,  STORE.CHANNEL, STORE.SALES_GROUP, STORE.STORE_STREET,  STORE.STORE_PROVINCE, MELTED.Description,LMAP.Language_Grouping,  MELTED.Category, YEAR

# COMMAND ----------

store_profiles.write.mode("overwrite").option("overwriteSchema", "True").format("delta").saveAsTable(
    "environics.demostats_store_profiles_0801_SOBEYS_2"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from environics.demostats_store_profiles_0801_SOBEYS_2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM environics.demostats_store_profiles_2023_0726
# MAGIC WHERE CUSTOMER_GROUP LIKE "%SOBEYS%" AND STORE_STREET LIKE "%FORT YORK%"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.store_ldu_attraction_filtered A
# MAGIC LEFT JOIN (SELECT DISTINCT POSTALCODE, TOTAL_POPULATION FROM default.ldu_feature_vals_melted_df WHERE YEAR = 2023 AND POSTALCODE IS NOT NULL AND Category NOT IN ('Trends', 'Attitudes')) B ON A.POSTALCODE = B.POSTALCODE
# MAGIC WHERE A.STORE_NAME = 'NO FRILLS #1335'
# MAGIC ORDER BY A.PERCENT_ATTRACTION DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.demostats_store_profiles
# MAGIC WHERE STORE_NAME = 'NO FRILLS #1335'

# COMMAND ----------

# DBTITLE 1,Calculate Customer Group Profile for Sobeys
# MAGIC %sql
# MAGIC SELECT CUSTOMER_GROUP, STORE_PROVINCE,CHANNEL,
# MAGIC Description, Category, 
# MAGIC CASE WHEN Category LIKE "Households by%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLDS)
# MAGIC     WHEN Category LIKE "Household Population 15 Years or Over%" OR Category IN ('Trends', 'Attitudes') THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC     WHEN Category LIKE "Household Population%" AND Category NOT LIKE "%15 Years or Over%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION)
# MAGIC     ELSE SUM(val)/SUM(TOTAL_POPULATION) END STORE_VALUE
# MAGIC --  SUM(val)/SUM(TOTAL_POPULATION)  STORE_VALUE
# MAGIC FROM environics.demostats_store_profiles_0801_SOBEYS_2
# MAGIC GROUP BY 1,2,3,4,5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT STORE.CUSTOMER_GROUP, STORE.STORE_PROVINCE, Store.Description, Store.Category, STORE.STORE_VALUE, OVERALL.REGION_OVERALL_VALUE, STORE.STORE_VALUE/OVERALL.REGION_OVERALL_VALUE PROFILE_REGION_INDEX FROM (SELECT CUSTOMER_GROUP, STORE_PROVINCE,
# MAGIC Description, Category, 
# MAGIC CASE WHEN Category LIKE "Households by%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLDS)
# MAGIC     WHEN Category LIKE "Household Population 15 Years or Over%" OR Category IN ('Trends', 'Attitudes') THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC     WHEN Category LIKE "Household Population%" AND Category NOT LIKE "%15 Years or Over%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION)
# MAGIC     ELSE SUM(val)/SUM(TOTAL_POPULATION) END STORE_VALUE
# MAGIC --  SUM(val)/SUM(TOTAL_POPULATION)  STORE_VALUE
# MAGIC FROM environics.demostats_store_profiles_0801_SOBEYS_2
# MAGIC GROUP BY 1,2,3,4) STORE
# MAGIC LEFT JOIN (SELECT EPCCF.PROV, MELT.Description, MELT.Category, 
# MAGIC CASE WHEN Category LIKE "Households by%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLDS)
# MAGIC     WHEN Category LIKE "Household Population 15 Years or Over%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC     WHEN Category LIKE "Household Population%" AND Category NOT LIKE "%15 Years or Over%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION)
# MAGIC     ELSE SUM(val)/SUM(TOTAL_POPULATION) END REGION_OVERALL_VALUE
# MAGIC     FROM default.ldu_feature_vals_melted_df MELT
# MAGIC LEFT JOIN (SELECT PRABB PROV, FSALDU FROM environics.epccf_unique) EPCCF
# MAGIC     ON MELT.POSTALCODE = EPCCF.FSALDU
# MAGIC GROUP BY 1,2,3) OVERALL ON STORE.STORE_PROVINCE = OVERALL.PROV AND STORE.Description = OVERALL.Description AND STORE.Category = OVERALL.Category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     A.CUSTOMER_GROUP,
# MAGIC     A.STORE_NAME,
# MAGIC     A.STORE_NUM,
# MAGIC     A.CHANNEL,
# MAGIC     A.SALES_GROUP,
# MAGIC     A.STORE_STREET,
# MAGIC     A.STORE_PROVINCE,
# MAGIC     A.Description,
# MAGIC     A.Language_Grouping,
# MAGIC     A.Category,
# MAGIC     A.YEAR,
# MAGIC     A.val,
# MAGIC     A.TOTAL_HOUSEHOLDS,
# MAGIC     A.TOTAL_HOUSEHOLD_POPULATION,
# MAGIC     A.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER,
# MAGIC     A.TOTAL_POPULATION,
# MAGIC     A.STORE_VALUE,
# MAGIC     B.BANNER_VALUE
# MAGIC FROM
# MAGIC     (SELECT 
# MAGIC         STORE.CUSTOMER_GROUP, 
# MAGIC         STORE.STORE_NAME, 
# MAGIC         STORE.STORE_NUM, 
# MAGIC         STORE.CHANNEL, 
# MAGIC         STORE.SALES_GROUP,
# MAGIC         STORE.STORE_STREET, 
# MAGIC         STORE.STORE_PROVINCE, 
# MAGIC         MELTED.Description, 
# MAGIC         LMAP.Language_Grouping, 
# MAGIC         MELTED.Category, 
# MAGIC         MELTED.YEAR,
# MAGIC         SUM(STORE.PERCENT_ATTRACTION * MELTED.val) as val, 
# MAGIC         SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLDS) as TOTAL_HOUSEHOLDS, 
# MAGIC         SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLD_POPULATION) as TOTAL_HOUSEHOLD_POPULATION, 
# MAGIC         SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER) as TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER, 
# MAGIC         SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_POPULATION) as TOTAL_POPULATION,
# MAGIC         CASE 
# MAGIC             WHEN MELTED.Category LIKE "Households by%" THEN SUM(STORE.PERCENT_ATTRACTION * MELTED.val) / SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLDS)
# MAGIC             WHEN MELTED.Category LIKE "Household Population 15 Years or Over%" THEN SUM(STORE.PERCENT_ATTRACTION * MELTED.val) / SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC             WHEN MELTED.Category LIKE "Household Population%" AND MELTED.Category NOT LIKE "%15 Years or Over%" THEN SUM(STORE.PERCENT_ATTRACTION * MELTED.val) / SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLD_POPULATION)
# MAGIC             ELSE SUM(STORE.PERCENT_ATTRACTION * MELTED.val) / SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_POPULATION) 
# MAGIC         END as STORE_VALUE
# MAGIC     FROM environics.store_LDU_attraction_filtered_0801 STORE 
# MAGIC     LEFT JOIN environics.ldu_feature_vals_melted_df_2023 MELTED ON STORE.POSTALCODE = MELTED.POSTALCODE
# MAGIC     LEFT JOIN default.language_mapping LMAP ON MELTED.Description = LMAP.Description_cleaned
# MAGIC     WHERE MELTED.Category NOT IN ('Trends', 'Attitudes') and CUSTOMER_GROUP LIKE "%SOBEYS%"  AND MELTED.val IS NOT NULL
# MAGIC     GROUP BY 
# MAGIC         STORE.CUSTOMER_GROUP, 
# MAGIC         STORE.STORE_NAME,
# MAGIC         STORE.STORE_NUM, 
# MAGIC         STORE.CHANNEL, 
# MAGIC         STORE.SALES_GROUP, 
# MAGIC         STORE.STORE_STREET,  
# MAGIC         STORE.STORE_PROVINCE, 
# MAGIC         MELTED.Description,
# MAGIC         LMAP.Language_Grouping,  
# MAGIC         MELTED.Category, 
# MAGIC         YEAR) A
# MAGIC JOIN
# MAGIC     (SELECT 
# MAGIC         STORE.CUSTOMER_GROUP, 
# MAGIC         MELTED.Description, 
# MAGIC         LMAP.Language_Grouping, 
# MAGIC         MELTED.Category, 
# MAGIC         MELTED.YEAR, 
# MAGIC         CASE 
# MAGIC             WHEN MELTED.Category LIKE "Households by%" THEN SUM(STORE.PERCENT_ATTRACTION * MELTED.val) / SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLDS)
# MAGIC             WHEN MELTED.Category LIKE "Household Population 15 Years or Over%" THEN SUM(STORE.PERCENT_ATTRACTION * MELTED.val) / SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC             WHEN MELTED.Category LIKE "Household Population%" AND MELTED.Category NOT LIKE "%15 Years or Over%" THEN SUM(STORE.PERCENT_ATTRACTION * MELTED.val) / SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLD_POPULATION)
# MAGIC             ELSE SUM(STORE.PERCENT_ATTRACTION * MELTED.val) / SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_POPULATION) 
# MAGIC         END as BANNER_VALUE
# MAGIC     FROM environics.store_LDU_attraction_filtered_0801 STORE 
# MAGIC     LEFT JOIN environics.ldu_feature_vals_melted_df_2023 MELTED ON STORE.POSTALCODE = MELTED.POSTALCODE
# MAGIC     LEFT JOIN default.language_mapping LMAP ON MELTED.Description = LMAP.Description_cleaned
# MAGIC     WHERE MELTED.Category NOT IN ('Trends', 'Attitudes') and CUSTOMER_GROUP LIKE "%SOBEYS%" AND MELTED.val IS NOT NULL
# MAGIC     GROUP BY 
# MAGIC         STORE.CUSTOMER_GROUP, 
# MAGIC         MELTED.Description,
# MAGIC         LMAP.Language_Grouping,  
# MAGIC         MELTED.Category, 
# MAGIC         YEAR) B
# MAGIC ON A.CUSTOMER_GROUP = B.CUSTOMER_GROUP 
# MAGIC AND A.Description = B.Description
# MAGIC AND A.Language_Grouping = B.Language_Grouping
# MAGIC AND A.Category = B.Category
# MAGIC AND A.YEAR = B.YEAR
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW IntermediateData2 AS
# MAGIC SELECT 
# MAGIC     STORE.CUSTOMER_GROUP,
# MAGIC     STORE.STORE_NAME,
# MAGIC     STORE.STORE_NUM,
# MAGIC     STORE.CHANNEL,
# MAGIC     STORE.SALES_GROUP,
# MAGIC     STORE.STORE_STREET,
# MAGIC     STORE.STORE_PROVINCE,
# MAGIC     STORE.POSTALCODE,
# MAGIC     MELTED.Description,
# MAGIC     LMAP.Language_Grouping,
# MAGIC     MELTED.Category,
# MAGIC     MELTED.YEAR,
# MAGIC     SUM(STORE.PERCENT_ATTRACTION * MELTED.val) AS val,
# MAGIC     SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLDS) AS TOTAL_HOUSEHOLDS,
# MAGIC     SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLD_POPULATION) AS TOTAL_HOUSEHOLD_POPULATION,
# MAGIC     SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER) AS TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER,
# MAGIC     SUM(STORE.PERCENT_ATTRACTION * MELTED.TOTAL_POPULATION) AS TOTAL_POPULATION
# MAGIC FROM environics.store_LDU_attraction_filtered_0801 STORE 
# MAGIC LEFT JOIN environics.ldu_feature_vals_melted_df_2023 MELTED ON STORE.POSTALCODE = MELTED.POSTALCODE
# MAGIC LEFT JOIN default.language_mapping LMAP ON MELTED.Description = LMAP.Description_cleaned
# MAGIC WHERE MELTED.Category NOT IN ('Trends', 'Attitudes')
# MAGIC     AND MELTED.val IS NOT NULL
# MAGIC GROUP BY STORE.CUSTOMER_GROUP, STORE.STORE_NAME, STORE.STORE_NUM, STORE.CHANNEL, STORE.SALES_GROUP, STORE.STORE_STREET, STORE.STORE_PROVINCE, STORE.POSTALCODE, MELTED.Description, LMAP.Language_Grouping, MELTED.Category, MELTED.YEAR;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     A.CUSTOMER_GROUP,
# MAGIC     A.STORE_NAME,
# MAGIC     A.STORE_NUM,
# MAGIC     A.CHANNEL,
# MAGIC     A.SALES_GROUP,
# MAGIC     A.STORE_STREET,
# MAGIC     A.STORE_PROVINCE,
# MAGIC     A.Description,
# MAGIC     A.Language_Grouping,
# MAGIC     A.Category,
# MAGIC     A.YEAR,
# MAGIC     A.STORE_VALUE,
# MAGIC     B.BANNER_VALUE,
# MAGIC     C.CHANNEL_VALUE
# MAGIC     D.PROVINCE_VALUE
# MAGIC FROM 
# MAGIC     (SELECT 
# MAGIC         CUSTOMER_GROUP,
# MAGIC         STORE_NAME,
# MAGIC         STORE_NUM,
# MAGIC         CHANNEL,
# MAGIC         SALES_GROUP,
# MAGIC         STORE_STREET,
# MAGIC         STORE_PROVINCE,
# MAGIC         Description,
# MAGIC         Language_Grouping,
# MAGIC         Category,
# MAGIC         YEAR,
# MAGIC         CASE 
# MAGIC             WHEN Category LIKE "Households by%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLDS)
# MAGIC             WHEN Category LIKE "Household Population 15 Years or Over%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC             WHEN Category LIKE "Household Population%" AND Category NOT LIKE "%15 Years or Over%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLD_POPULATION)
# MAGIC             ELSE SUM(val) / SUM(TOTAL_POPULATION)
# MAGIC         END AS STORE_VALUE
# MAGIC     FROM IntermediateData2
# MAGIC     GROUP BY CUSTOMER_GROUP, STORE_NAME, STORE_NUM, CHANNEL, SALES_GROUP, STORE_STREET, STORE_PROVINCE, Description, Language_Grouping, Category, YEAR) A
# MAGIC JOIN 
# MAGIC     (SELECT 
# MAGIC         CUSTOMER_GROUP,
# MAGIC         Description,
# MAGIC         Language_Grouping,
# MAGIC         Category,
# MAGIC         YEAR,
# MAGIC         CASE 
# MAGIC             WHEN Category LIKE "Households by%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLDS)
# MAGIC             WHEN Category LIKE "Household Population 15 Years or Over%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC             WHEN Category LIKE "Household Population%" AND Category NOT LIKE "%15 Years or Over%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLD_POPULATION)
# MAGIC             ELSE SUM(val) / SUM(TOTAL_POPULATION)
# MAGIC         END AS BANNER_VALUE
# MAGIC     FROM IntermediateData2
# MAGIC     GROUP BY CUSTOMER_GROUP, Description, Language_Grouping, Category, YEAR) B
# MAGIC ON A.CUSTOMER_GROUP = B.CUSTOMER_GROUP
# MAGIC    AND A.Description = B.Description
# MAGIC    AND A.Language_Grouping = B.Language_Grouping
# MAGIC    AND A.Category = B.Category
# MAGIC    AND A.YEAR = B.YEAR
# MAGIC
# MAGIC
# MAGIC JOIN 
# MAGIC     (SELECT 
# MAGIC         CUSTOMER_GROUP,
# MAGIC         Description,
# MAGIC         Language_Grouping,
# MAGIC         Category,
# MAGIC         YEAR,
# MAGIC         CASE 
# MAGIC             WHEN Category LIKE "Households by%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLDS)
# MAGIC             WHEN Category LIKE "Household Population 15 Years or Over%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC             WHEN Category LIKE "Household Population%" AND Category NOT LIKE "%15 Years or Over%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLD_POPULATION)
# MAGIC             ELSE SUM(val) / SUM(TOTAL_POPULATION)
# MAGIC         END AS BANNER_VALUE
# MAGIC     FROM IntermediateData2
# MAGIC     GROUP BY CUSTOMER_GROUP, Description, Language_Grouping, Category, YEAR) B
# MAGIC ON A.CUSTOMER_GROUP = B.CUSTOMER_GROUP
# MAGIC    AND A.Description = B.Description
# MAGIC    AND A.Language_Grouping = B.Language_Grouping
# MAGIC    AND A.Category = B.Category
# MAGIC    AND A.YEAR = B.YEAR
# MAGIC
# MAGIC
# MAGIC JOIN 
# MAGIC     (SELECT 
# MAGIC         CUSTOMER_GROUP,
# MAGIC         Description,
# MAGIC         Language_Grouping,
# MAGIC         Category,
# MAGIC         YEAR,
# MAGIC         CASE 
# MAGIC             WHEN Category LIKE "Households by%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLDS)
# MAGIC             WHEN Category LIKE "Household Population 15 Years or Over%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC             WHEN Category LIKE "Household Population%" AND Category NOT LIKE "%15 Years or Over%" THEN SUM(val) / SUM(TOTAL_HOUSEHOLD_POPULATION)
# MAGIC             ELSE SUM(val) / SUM(TOTAL_POPULATION)
# MAGIC         END AS BANNER_VALUE
# MAGIC     FROM IntermediateData2
# MAGIC     GROUP BY CUSTOMER_GROUP, Description, Language_Grouping, Category, YEAR) B
# MAGIC ON A.CUSTOMER_GROUP = B.CUSTOMER_GROUP
# MAGIC    AND A.Description = B.Description
# MAGIC    AND A.Language_Grouping = B.Language_Grouping
# MAGIC    AND A.Category = B.Category
# MAGIC    AND A.YEAR = B.YEAR

# COMMAND ----------

# MAGIC %md
# MAGIC no attraction involved below

# COMMAND ----------

# DBTITLE 1,Calculate Customer Profiles including Overall Region Profile
customer_profiles = spark.sql("""
SELECT STORE.CUSTOMER_GROUP, STORE.STORE_PROFILE_REGION, Store.Description, Store.Category, STORE.CUSTOMER_VALUE, OVERALL.REGION_OVERALL_VALUE, STORE.CUSTOMER_VALUE/OVERALL.REGION_OVERALL_VALUE PROFILE_REGION_INDEX FROM (SELECT CUSTOMER_GROUP, 
CASE WHEN STORE_PROVINCE IN ('AB', 'SK', 'MB') THEN 'PRAIRIES'
    WHEN STORE_PROVINCE IN ('NL', 'NS', 'PE', 'NB') THEN 'EAST'
    WHEN STORE_PROVINCE IN ('YT', 'NT', 'NU') THEN 'NORTH'
    ELSE STORE_PROVINCE END STORE_PROFILE_REGION,
Description, Category, 
CASE WHEN Category LIKE "Households by%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLDS)
    WHEN Category LIKE "Household Population 15 Years or Over%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
    WHEN Category LIKE "Household Population%" AND Category NOT LIKE "%15 Years or Over%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION)
    ELSE SUM(val)/SUM(TOTAL_POPULATION) END CUSTOMER_VALUE
FROM demostats_store_profiles
WHERE CUSTOMER_GROUP LIKE "%SOBEYS%" AND NOT (CUSTOMER_GROUP = 'SO_SOBEYS WEST' AND STORE_PROVINCE = 'ON') AND NOT (CUSTOMER_GROUP LIKE '%SOBEYS%' AND STORE_PROVINCE = 'QC')
GROUP BY 1,2,3,4) STORE
LEFT JOIN (SELECT CASE WHEN EPCCF.PROV IN ('AB', 'SK', 'MB') THEN 'PRAIRIES'
    WHEN EPCCF.PROV IN ('NL', 'NS', 'PE', 'NB') THEN 'EAST'
    WHEN EPCCF.PROV IN ('YT', 'NT', 'NU') THEN 'NORTH'
    ELSE EPCCF.PROV END PROV, MELT.Description, MELT.Category, 
CASE WHEN Category LIKE "Households by%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLDS)
    WHEN Category LIKE "Household Population 15 Years or Over%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
    WHEN Category LIKE "Household Population%" AND Category NOT LIKE "%15 Years or Over%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION)
    ELSE SUM(val)/SUM(TOTAL_POPULATION) END REGION_OVERALL_VALUE
    FROM default.ldu_feature_vals_melted_df MELT
LEFT JOIN (SELECT PRABB PROV, FSALDU FROM environics.epccf_unique) EPCCF
    ON MELT.POSTALCODE = EPCCF.FSALDU
GROUP BY 1,2,3) OVERALL ON STORE.STORE_PROFILE_REGION = OVERALL.PROV AND STORE.Description = OVERALL.Description AND STORE.Category = OVERALL.Category
""")

# COMMAND ----------

# DBTITLE 1,Overall Region values
# MAGIC %sql
# MAGIC SELECT CASE WHEN EPCCF.PROV IN ('AB', 'SK', 'MB') THEN 'PRAIRIES'
# MAGIC     WHEN EPCCF.PROV IN ('NL', 'NS', 'PE', 'NB') THEN 'EAST'
# MAGIC     WHEN EPCCF.PROV IN ('YT', 'NT', 'NU') THEN 'NORTH'
# MAGIC     ELSE EPCCF.PROV END PROV, MELT.Description, MELT.Category, 
# MAGIC CASE WHEN Category LIKE "Households by%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLDS)
# MAGIC     WHEN Category LIKE "Household Population 15 Years or Over%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION_15_YEARS_OR_OVER)
# MAGIC     WHEN Category LIKE "Household Population%" AND Category NOT LIKE "%15 Years or Over%" THEN SUM(val)/SUM(TOTAL_HOUSEHOLD_POPULATION)
# MAGIC     ELSE SUM(val)/SUM(TOTAL_POPULATION) END REGION_OVERALL_VALUE
# MAGIC     FROM default.ldu_feature_vals_melted_df MELT
# MAGIC LEFT JOIN (SELECT PRABB PROV, FSALDU FROM environics.epccf_unique) EPCCF
# MAGIC     ON MELT.POSTALCODE = EPCCF.FSALDU
# MAGIC GROUP BY 1,2,3

# COMMAND ----------

# customer_profiles.write.mode("overwrite").option("overwriteSchema", "True").format("delta").saveAsTable(
#     "default.demostats_customer_profiles"
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demostats_customer_profiles
# MAGIC WHERE CUSTOMER_GROUP LIKE '%SOBEYS%'
