##### Install using
`devtools::install_github("aasiyahrashan/SeverityScoresOMOP", auth_token = "your_PAT")`

##### Calculates the APACHE II ICU risk prediction score and probability of mortality.
- Works on databases in the OMOP CDM format version 5.3.1.
- Currently only works for the CC-HIC data set.
- The OMOP concept IDs need to be specified in the '*_concepts.csv' files. The short names need to be consistent. I plan to make a template of the file.



#### TODO
- Import comorbidities for CCHIC
- Import admission type for CCHIC
- Match pao2, paco2 and fio2s instead of just getting min and max.
- Same for MAP. Currently getting max and min SBP and DBP. 
- Find out how blood pressure is stored in CCHIC and get concepts to match it.
- Write mortality prediction calculation
- Write instructions on how to use the package.
- Write a template for the concept files.
