## Installation instructions
- Use `devtools::install_github("aasiyahrashan/SeverityScoresOMOP", auth_token = "your_PAT")`
- The personal access token is necessary, since this is currently a private repository

#### This package reads in data from OMOP to calculate the APACHE II and SOFA scores. 
#### It also provides a convenient method of reading in other variables summarised into time windows.
- Works on databases in the OMOP CDM format version 5.4. 
- Copy the `inst/example_concepts.csv` file. 
- Save it under the name `your_dataset_name_concepts.csv`. 
- The file needs to be contain OMOP concept IDs which match the `short_names`. Don't edit the short names, since they're used in the code.
````
    library(SeverityScoresOMOP)
    #### Connect to the database.
    omop_conn <- omop_connect(driver = "PostgreSQL",
                              host = "localhost",
                              dbname = "dbname",
                              port = 5432,
                              user = "user",
                              password = "password")

    ### Getting dataset of physiology variables
    ### The min and max date arguments describe how many days after ICU admisison to get data for.
    ### Setting min to 0 and max to 1 gives the first day of ICU admission.
    ### Each day of data per visit is a separate row in the output dataset. 
    ### Age method is used to calculate age. It decides whether to use date of birth, or just year of birth. Options are 'dob' (default) or 'year_only'
    ### Cadence is measured in hours. 24 represents a day, 1 represents an hour, 0.5 is 30 minutes.
    ### Window start can either be `calendar_date` or `icu_admission_time`. See function documentation.

    data <- get_score_variables(omop_conn, "your_sql_dialect", "your_schema_name", 
                                start_date = "2022-07-01", end_date = "2022-07-31",
                                first_window = 0, last_window = 1, 
                                concepts_file_path = "path_to_your_concepts_file", 
                                severity_score = c("APACHE II", "SOFA"),
                                age_method = 'dob',
                                cadence = 24
                                window_start_point = "calendar_date")
    dbDisconnect(omop_conn)
    
    ### Standardise units of measure and calculate the APACHE II score.
    data <- fix_apache_ii_units(data)
    data <- fix_implausible_values_apache_ii(data)
    ### Can choose complete case calculation or normal imputation. Default is normal imputation.
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

    #### Inspect the distribution and availability of variables.
    availability_dataframe <- get_physiology_variable_availability(data)
    View(availability_dataframe)
    get_physiology_variable_distributions(data)
````

#### TODO
- Adapt the SQL queries to work for OMOP 5.3.1
- Match pao2, paco2 and fio2s instead of just getting min and max.
- Same for MAP. Currently getting max and min SBP and DBP. 
- Write mortality prediction calculation
- Fix unit conversion and implausible values functions to prevent code being duplicated between scores.
- Include vasopressors in SOFA calculation.
