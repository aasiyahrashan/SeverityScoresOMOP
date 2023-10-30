#' Get physiology variable availability
#'
#' @param data Dataframe containing physiology variables and (optionally) APACHE II score.
#' @return A dataframe listing variable names and % availability per patient day.
#' @export
#' @import dplyr
#' @import tidyr
#' @import stringr
#' @examples
#' availability_dataframe <- get_physiology_variable_availability(data)
get_physiology_variable_availability <- function(data){
  availability <-
    data %>%
    select(starts_with(c("max_", "apache_ii_score"))) %>%
    rename_all(~stringr::str_replace(.,"^max_","")) %>%
    rename_all(~stringr::str_replace(.,"^apache_ii_score_no_imputation", "APACHE-II-no-imputation")) %>%
    rename_all(~stringr::str_replace(.,"^apache_ii_score", "APACHE-II")) %>%
    summarise_all(list(availability = ~round(100*sum(!is.na(.))/nrow(data), 2),
                       min = ~if (all(is.na(.))) NA_real_ else round(min(., na.rm = TRUE), 2),
                       max = ~if (all(is.na(.))) NA_real_ else round(max(., na.rm = TRUE), 2)
    )) %>%
    pivot_longer(
      cols = names(.),
      names_to = c("variable", "summary"),
      names_sep = "_") %>%
    pivot_wider(names_from = "summary", values_from = "value") %>%
    arrange(desc(availability))

  availability
}

#' Create histogram of distributions of APACHE II physiology variables.
#'
#' @param data Dataframe containing physiology variables. Should have had the units of measure function run as well.
#' @return A dataframe listing variable names and % availability per patient day.
#' @export
#' @import dplyr
#' @import tidyr
#' @import stringr
#' @import ggplot2
#' @examples
#' get_physiology_variable_distributions(data)
get_physiology_variable_distributions <- function(data){
  hist <-
    data %>%
    select(visit_detail_id, starts_with("min")) %>%
    pivot_longer(cols = !visit_detail_id) %>%
    filter(name != "min_paco2",
           name != "min_sbp",
           name != "min_dbp") %>%
    ### Should really join this to the units in the dataset instead of hardcoding.
    mutate(name = case_when(
      name == "min_hr" ~ "Heart rate",
      name == "min_bicarbonate" ~ "Bicarbonate mmol/L",
      name == "min_creatinine" ~ "Creatinine mg/dL",
      name == "min_map" ~ "Mean arterial pressure",
      name == "min_fio2" ~ "FiO2",
      name == "min_gcs" ~ "GCS",
      name == "min_hematocrit" ~ "Hematocrit %",
      name == "min_hr" ~ "Heart rate",
      #### This one is specific to a dataset where the paco2 varialbe is always imputed as normal.
      #### Will ignore otherwise.
      name == "min_paco2orig" ~ "PaCO2 original mmHg",
      name == "min_paco2" ~ "PaCO2 mmHg",
      name == "min_pao2" ~ "PaO2 mmHg",
      name == "min_ph" ~ "pH",
      name == "min_potassium" ~ "Potassium mmol/L",
      name == "min_rr" ~ "Respiratory rate",
      name == "min_sodium" ~ "Sodium mmol/L",
      name == "min_temp" ~ "Temperature C",
      name == "min_wcc" ~ "White cell count 10^9/L")) %>%
    ggplot(aes(value)) +
    geom_histogram() +
    facet_wrap(~name, scales = "free") +
    theme_classic()

  hist
}
