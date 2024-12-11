## Installation instructions
- Use `devtools::install_github("aasiyahrashan/SeverityScoresOMOP", auth_token = "your_PAT")`
- The personal access token is necessary, since this is currently a private repository

#### This package reads in data from OMOP to calculate the APACHE II and SOFA scores. 
#### It also provides a convenient method of reading in other variables summarised into time windows.
- Works on databases in the OMOP CDM format version 5.4. 
- Depends on the OHDSI SqlRender package, https://github.com/OHDSI/SqlRender, which can have quite limited translations between SQL dialects.
- I've only used this package with SQLServer and PostgreSQL. Most of the code was tested on Postgres only.
- Copy the `inst/example_concepts.csv` file. 
- Save it under the name `your_dataset_name_concepts.csv`. 
- The file needs to contain OMOP concept IDs which match the `short_names`. Don't edit the short names, since they're used in the code.
````
    library(SeverityScoresOMOP)
    # Connect to the database.
    omop_conn <- omop_connect(driver = "PostgreSQL",
                              host = "localhost",
                              dbname = "dbname",
                              port = 5432,
                              user = "user",
                              password = "password")

    # Getting dataset of physiology variables
    # The first and last window arguments describe how many timepoints (depending on cadence) after ICU admisison to get data for.
    # The filters are inclusive on both sides, so 0 for both values will get data at ICU admission
    # Each time window is a separate row in the output dataset. 
    # Age method is used to calculate age. It decides whether to use date of birth, or just year of birth. Options are 'dob' (default) or 'year_only'
    # Cadence is measured in hours. 24 represents a day, 1 represents an hour, 0.5 is 30 minutes.
    # Window start can either be `calendar_date` or `icu_admission_time`. See function documentation.

    data <- get_score_variables(omop_conn, "your_sql_dialect", "your_schema_name", 
                                start_date = "2022-07-01", end_date = "2022-07-31",
                                first_window = 0, last_window = 0, 
                                concepts_file_path = "path_to_your_concepts_file", 
                                severity_score = c("APACHE II", "SOFA"),
                                age_method = 'dob',
                                cadence = 24
                                window_start_point = "calendar_date")
    dbDisconnect(omop_conn)
    
    # Standardise units of measure and calculate the APACHE II score.
    data <- fix_apache_ii_units(data)
    data <- fix_implausible_values_apache_ii(data)
    # Can choose complete case calculation or normal imputation. Default is normal imputation.
    data <- calculate_apache_ii_score(data, imputation = "none")
    print(data$apache_ii_score_no_imputation)
    data <- calculate_apache_ii_score(data, imputation = "normal")
    print(data$apache_ii_score)
    
    # SOFA score
    data <- fix_sofa_units(data)
    data <- fix_implausible_values_sofa(data)
    data <- calculate_sofa_score(data, imputation = "none")
    data <- calculate_sofa_score(data)
    print(data$score_score)

    # Inspect the distribution and availability of variables.
    availability_dataframe <- get_physiology_variable_availability(data)
    View(availability_dataframe)
    get_physiology_variable_distributions(data)
````

#### Format for the csv file.
The file specifies the OMOP concept codes which correspond to each variable. Entries here determine how the data is read in	
I suggest adding the file to your project version control.
	
	
Description of variables:
- score: Currently only takes values "APACHE II", "SOFA". This variable is used as a filter, based on the value of the `score` argument in the `get_score_variables` function
- short_name:	This becomes part of the column name in the dataset extracted from SQL. If the same name is repeated on multiple rows, all the data is stored in one variable. Don't change the names of variables used in the severity scores, because they're used in the code.
- table: Name of the OMOP table the data is stored in. Currently only allows `Measurement`, `Observation`, `Condition`, `Procedure`, `Device`, `Drug`. `Visit detail` is allowed if short_name is `emergency_admission`.
- concept_id: 	OMOP concept ID for the variable. If a single short_name corresponds to multiple concept_ids, put the IDs on separate rows. Also allows a string for string searching the OMOP concept names. The query is `where concept.concept_name LIKE'%search_term%'` and is case insensitive.
- omop_variable:	Either value_as_number, value_as_concept_id, concept_name (for string searches) or nothing. If it's value as number, the output dataframe will contain 3 variables per short_name: min_short_name, max_short_name, unit_short_name. Note, unit_short_name relies on units being the same per patient and variable. Need to fix. If value_as_concept_id, concept_name, or blank is entered, a count_short_name is returned per short_name. It represents the number of times that concept_id was recorded for that patient and time period.
- concept_id_value:	If value_as_concept_id is selected for the omop_variable, this is the OMOP concept ID to filter the field by. If more than one is required, create a separate row.
- name_of_value:	The name corresponding to the concept_id_value variable. Only used to make the file more readable.
- additional_filter_variable_name:	Option to add an extra filter (usually by source name). This specifies the variable to filter by. Only allows one variable per short_name.
- additional_filter_variable_nameP	Option to add an extra filter (usually by source name). This specifies the values to filter by. If there are multiple values, use more than one row.
- Note:	For readability only, not used in the code.



#### TODO
- Adapt the SQL queries to work for OMOP 5.3.1
- Match pao2, paco2 and fio2s instead of just getting min and max.
- Same for MAP. Currently getting max and min SBP and DBP. 
- Write mortality prediction calculation
- Fix unit conversion and implausible values functions to prevent code being duplicated between scores.
- Include vasopressors in SOFA calculation.
- Handle case where there is more than one unit of measure per person, variable and time.


