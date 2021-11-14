import pandas as pd
import numpy as np
import snowflake_connection2 as sf
from config import database,warehouse
import datetime

current_time = datetime.datetime.now()

df = pd.read_csv('data/survey_results_public.csv',index_col='Respondent')
schema_df = pd.read_csv('data/survey_results_schema.csv',index_col='Column')

print("total = ",df.shape[0])
df['ConvertedComp'] = df['ConvertedComp'].fillna(0)
df.rename(columns={'ConvertedComp':'SalaryUSD'},inplace=True)
df['YearsCode'].replace('Less than 1 year', 0, inplace=True)
df['YearsCode'].replace('More than 50 years', 51, inplace=True)
df['YearsCode'] = df['YearsCode'].astype(float)

df['Current_Time']=current_time
df['ID']="snowflake_user"


North_American_data=['United States','Canada','Mexico']
filt1= df['Country'].isin(North_American_data)
filt2= ~df['Country'].isin(North_American_data)
NorthAmerica=df.loc[filt1,['Country','Ethnicity','Age','Gender','EdLevel','LanguageWorkedWith','LanguageDesireNextYear','YearsCode','WorkWeekHrs','SalaryUSD','Current_Time','ID']]
RestOfWorld =df.loc[filt2,['Country','Ethnicity','Age','Gender','EdLevel','LanguageWorkedWith','LanguageDesireNextYear','YearsCode','WorkWeekHrs','SalaryUSD','Current_Time','ID']]


NorthAmerica.to_csv("NorthAmerica.csv", sep='|', encoding="utf-8")
RestOfWorld.to_csv("RestOfWorld.csv", sep='|', encoding="utf-8")

print("tot = ", NorthAmerica.shape[0]+RestOfWorld.shape[0])

SQL1='CREATE OR REPLACE TABLE "TEST_DB"."PUBLIC"."NORTHAMERICAN_DATA" ("RESPONDENT" STRING, "COUNTRY" STRING, "ETHNICITY" STRING, "AGE" STRING, "GENDER" STRING, "EDLEVEL" STRING, "LANGUAGEWORKEDWITH" STRING, "LANGUAGEDESIRENEXTYEAR" STRING, "YEARSCODE" STRING, "WORKWEEKHRS" STRING, "SALARYUSD" STRING,"current_time" STRING, "ID" STRING) COMMENT = "NorthAmerican Survey data";'
result=sf.execute_query1(SQL1,database,warehouse)
for c in result:print(c)
SQL2='CREATE OR REPLACE TABLE "TEST_DB"."PUBLIC"."RESTOFWORLD_DATA" ("RESPONDENT" STRING, "COUNTRY" STRING, "ETHNICITY" STRING, "AGE" STRING, "GENDER" STRING, "EDLEVEL" STRING, "LANGUAGEWORKEDWITH" STRING, "LANGUAGEDESIRENEXTYEAR" STRING, "YEARSCODE" STRING, "WORKWEEKHRS" STRING, "SALARYUSD" STRING,"current_time" STRING, "ID" STRING) COMMENT = "Rest of World Survey data";'
result=sf.execute_query1(SQL2,database,warehouse)
for c in result:print(c)


SQL='select current_database(),current_schema(),current_warehouse();'
result=sf.execute_query1(SQL,database,warehouse)
for c in result:print(c)


SQL="create or replace stage my_stage  file_format = (type = 'CSV' field_delimiter = '|' skip_header = 1);"
result=sf.execute_query1(SQL,database,warehouse)

SQL='list @TEST_DB.PUBLIC.my_stage'
result=sf.execute_query1(SQL,database,warehouse)
for c in result:
    print(c)
print("\n")


SQL1="PUT file://C:\\Users\\Administrator\\Desktop\\snowflakepy\\NorthAmerica.csv @TEST_DB.PUBLIC.MY_STAGE"
result=sf.execute_query1(SQL1,database,warehouse)
for c in result:
    print(c)

SQL2="PUT file://C:\\Users\\Administrator\\Desktop\\snowflakepy\\RestOfWorld.csv @TEST_DB.PUBLIC.MY_STAGE"
result=sf.execute_query1(SQL2,database,warehouse)
for c in result:
    print(c)

print("************ My_stage ************\n")
SQL='list @TEST_DB.PUBLIC.my_stage'
result=sf.execute_query1(SQL,database,warehouse)
for c in result:
    print(c)
print("\n")

SQL1="copy into TEST_DB.PUBLIC.NORTHAMERICAN_DATA from @TEST_DB.PUBLIC.my_stage FILE_FORMAT = MY_CSV ,pattern='.*NorthAmerica.csv.gz'"
result=sf.execute_query1(SQL1,database,warehouse)
for c in result:
    print(c)

SQL2="copy into TEST_DB.PUBLIC.RESTOFWORLD_DATA from @TEST_DB.PUBLIC.my_stage FILE_FORMAT = MY_CSV ,pattern='.*RestOfWorld.csv.gz'"
result=sf.execute_query1(SQL2,database,warehouse)
for c in result:
    print(c)