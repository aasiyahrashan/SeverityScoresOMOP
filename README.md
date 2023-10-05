## Installation instructions
- Use `devtools::install_github("aasiyahrashan/SeverityScoresOMOP", auth_token = "your_PAT")`
- The personal access token is necessary, since this is currently a private repository

#### This package calculates the APACHE II ICU risk prediction score and probability of mortality.
- Works on databases in the OMOP CDM format version 5.4. 
- The file `example_concepts.csv` needs to be renamed to `your_dataset_name_concepts.csv`. The file needs to be filled in with the OMOP concept IDs that match the `short_names`. Don't edit the short names, since they're used in the code.
```` 
    #### Connect to the database.
    postgres_conn <- postgres_connect(host = "localhost",
                                  dbname = "dbname",
                                  port = 5432,
                                  user = "user",
                                  password = "password")

    ### Getting dataset of physiology variables
    ### The min and max date arguments describe how many days after ICU admisison to get data for.
    ### Setting min to 0 and max to 1 gives the first 24 hours of ICU admission.
    ### Each day of data per visit is a separate row in the output dataset. 
    data <- get_score_variables(postgres_conn, "schema_name", 
                                start_date = "2022-07-01", end_date = "2022-07-31",
                                min_day = 0, max_day = 1, dataset_name = "your_dataset_name", 
                                severity_score = "APACHE II")
    dbDisconnect(postgres_conn)
    
    ### Standardise units of measure and calculate the APACHE II score.
    data <- fix_apache_ii_units(data)
    data <- fix_implausible_values_apache_ii(data)
    ### Can choose complete case calculation or normal imputation. Default is normal imputation.
    data <- calculate_apache_ii_score(data, imputation = "none")
    print(data$apache_ii_score_no_imputation)
    data <- calculate_apache_ii_score(data, imputation = "normal")
    print(data$apache_ii_score)
````

#### TODO
- Adapt the SQL queries to work for OMOP 5.3.1
- Adapt the queries and connections to work with non-postgres databases. 
- Match pao2, paco2 and fio2s instead of just getting min and max.
- Same for MAP. Currently getting max and min SBP and DBP. 
- Write mortality prediction calculation
