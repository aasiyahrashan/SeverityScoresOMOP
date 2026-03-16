#' Apply unit conversions from a lookup table.
#'
#' Reads the unit conversion CSV and applies conversions to min_ and max_ columns.
#' Handles two types of conversion:
#' - Unit-based: converts when unit_X matches unit_from
#' - Value-based: converts when value exceeds a threshold (e.g. temp > 50 = Fahrenheit)
#'   These use special unit_from values: "value_gt_X" and "value_lte_X"
#'
#' @param data A data.table with physiology variables.
#' @param variables Character vector of variable short_names to convert.
#'   If NULL, converts all variables found in the lookup table.
#' @param conversions_path Path to the unit conversions CSV. Defaults to the
#'   package-installed file.
#'
#' @return The data.table with converted values and updated unit columns.
#' @import data.table
#' @importFrom readr read_csv
#' @export
fix_units <- function(data,
                      variables = NULL,
                      conversions_path = system.file("unit_conversions.csv",
                                                     package = "SeverityScoresOMOP")) {
  data <- as.data.table(data)
  conversions <- read_csv(conversions_path, show_col_types = FALSE)

  if (!is.null(variables)) {
    conversions <- conversions[conversions$variable %in% variables, ]
  }

  # Process each variable
  for (var in unique(conversions$variable)) {
    min_col <- paste0("min_", var)
    max_col <- paste0("max_", var)
    unit_col <- paste0("unit_", var)

    var_conversions <- conversions[conversions$variable == var, ]

    # Get metadata from the first row (known_units and default_unit are the same for all rows of a variable)
    known_units <- var_conversions$known_units[1]
    default_unit <- var_conversions$default_unit[1]

    # Apply each conversion rule
    for (i in seq_len(nrow(var_conversions))) {
      rule <- var_conversions[i, ]
      unit_from <- rule$unit_from
      factor <- rule$conversion_factor

      if (is.na(unit_from) || unit_from == "") {
        # No conversion needed (e.g. bicarbonate) — just check units
        next
      }

      if (grepl("^value_gt_", unit_from)) {
        # Value-based threshold conversion (e.g. temp > 50 means Fahrenheit)
        threshold <- as.numeric(sub("^value_gt_", "", unit_from))
        if (factor == "F_to_C") {
          # Special Fahrenheit to Celsius formula
          if (max_col %in% names(data))
            data[get(max_col) > threshold, (max_col) := (get(max_col) - 32) * 5 / 9]
          if (min_col %in% names(data))
            data[get(min_col) > threshold, (min_col) := (get(min_col) - 32) * 5 / 9]
        } else {
          factor <- as.numeric(factor)
          if (max_col %in% names(data))
            data[get(max_col) > threshold, (max_col) := get(max_col) * factor]
          if (min_col %in% names(data))
            data[get(min_col) > threshold, (min_col) := get(min_col) * factor]
        }
      } else if (grepl("^value_lte_", unit_from)) {
        # Value-based threshold (e.g. hematocrit <= 1 means ratio)
        threshold <- as.numeric(sub("^value_lte_", "", unit_from))
        factor <- as.numeric(factor)
        if (max_col %in% names(data))
          data[get(max_col) <= threshold, (max_col) := get(max_col) * factor]
        if (min_col %in% names(data))
          data[get(min_col) <= threshold, (min_col) := get(min_col) * factor]
      } else {
        # Standard unit-based conversion
        factor <- as.numeric(factor)
        if (unit_col %in% names(data)) {
          if (max_col %in% names(data))
            data[get(unit_col) == unit_from, (max_col) := get(max_col) * factor]
          if (min_col %in% names(data))
            data[get(unit_col) == unit_from, (min_col) := get(min_col) * factor]
        }
      }
    }

    # Warn about unknown units
    if (!is.na(known_units) && known_units != "" && unit_col %in% names(data)) {
      known <- trimws(unlist(strsplit(known_units, ",")))
      actual <- unique(data[[unit_col]])
      unknown <- setdiff(actual[!is.na(actual)], known)
      if (length(unknown) > 0) {
        warning(var, " contains unknown unit(s) of measure: ",
                paste(unknown, collapse = ", "),
                ". Assuming values are in ", default_unit)
      }
    }

    # Set unit to default
    if (!is.na(default_unit) && unit_col %in% names(data)) {
      data[, (unit_col) := default_unit]
    }
  }

  data
}


#' Remove implausible values using a lookup table.
#'
#' Reads the implausible values CSV and sets values outside plausible ranges to NA.
#'
#' @param data A data.table with physiology variables.
#' @param variables Character vector of variable short_names to check.
#'   If NULL, checks all variables found in the lookup table.
#' @param thresholds_path Path to the implausible values CSV.
#'
#' @return The data.table with implausible values set to NA.
#' @import data.table
#' @importFrom readr read_csv
#' @export
fix_implausible_values <- function(data,
                                   variables = NULL,
                                   thresholds_path = system.file("implausible_values.csv",
                                                                 package = "SeverityScoresOMOP")) {
  data <- as.data.table(data)
  thresholds <- read_csv(thresholds_path, show_col_types = FALSE)

  if (!is.null(variables)) {
    thresholds <- thresholds[thresholds$variable %in% variables, ]
  }

  for (i in seq_len(nrow(thresholds))) {
    rule <- thresholds[i, ]
    var <- rule$variable
    min_col <- paste0("min_", var)
    max_col <- paste0("max_", var)
    unit_col <- paste0("unit_", var)
    expected_unit <- rule$expected_unit
    lo <- rule$min_plausible
    hi <- rule$max_plausible

    for (col in c(min_col, max_col)) {
      if (!col %in% names(data)) next

      if (!is.na(expected_unit) && expected_unit != "" && unit_col %in% names(data)) {
        # Only nullify when unit matches expected (so we don't nullify unconverted data)
        data[get(unit_col) == expected_unit & (get(col) < lo | get(col) > hi), (col) := NA]

        # Warn if unexpected units remain
        actual <- unique(data[[unit_col]])
        unexpected <- setdiff(actual[!is.na(actual)], expected_unit)
        if (length(unexpected) > 0) {
          warning(var, " has values not in expected unit '", expected_unit,
                  "'. Please fix units before removing implausible values.")
        }
      } else {
        # No unit column — apply unconditionally
        data[get(col) < lo | get(col) > hi, (col) := NA]
      }
    }
  }

  data
}


#' Apply scoring ranges from a lookup table to physiology variables.
#'
#' Reads a scoring CSV and assigns points based on value ranges. Handles
#' both min and max columns, and the "worst score" logic (pmax of min/max).
#' Does NOT handle special cases (AaDO2, GCS, creatinine doubling, chronic health,
#' bicarbonate/pH fallback, mechanical ventilation) — those stay in the
#' score-specific functions.
#'
#' @param data A data.table with physiology variables.
#' @param scoring_path Path to the scoring CSV (e.g. apache_ii_scoring.csv).
#' @param imputation Either "normal" (missing = 0 points) or "none" (missing = NA).
#'
#' @return The data.table with subscore columns added (e.g. min_temp_score, max_temp_score).
#' @import data.table
#' @importFrom readr read_csv
apply_scoring_table <- function(data, scoring_path, imputation = "normal") {
  data <- as.data.table(data)
  scoring <- read_csv(scoring_path, show_col_types = FALSE)

  # Get unique variables and their score column names
  score_variables <- scoring[, c("variable", "use_min_or_max")] |> unique()

  # Build list of subscore columns to initialise
  subscore_cols <- c()
  for (i in seq_len(nrow(score_variables))) {
    var <- score_variables$variable[i]
    use <- score_variables$use_min_or_max[i]
    if (use == "both") {
      subscore_cols <- c(subscore_cols, paste0("min_", var, "_score"), paste0("max_", var, "_score"))
    } else if (use == "min") {
      subscore_cols <- c(subscore_cols, paste0("min_", var, "_score"))
    } else if (use == "max") {
      subscore_cols <- c(subscore_cols, paste0("max_", var, "_score"))
    } else if (use == "age") {
      subscore_cols <- c(subscore_cols, paste0(var, "_score"))
    }
  }

  # Initialise subscores
  if (imputation == "normal") {
    data[, (subscore_cols) := as.integer(0)]
  } else {
    data[, (subscore_cols) := as.integer(NA)]
  }

  # Apply each range
  for (i in seq_len(nrow(scoring))) {
    rule <- scoring[i, ]
    var <- rule$variable
    lo <- rule$min_value
    hi <- rule$max_value
    pts <- rule$points
    use <- rule$use_min_or_max
    condition <- rule$special_condition

    # Determine which columns to score
    if (use == "age") {
      cols_to_score <- var  # just "age"
      score_cols <- paste0(var, "_score")
    } else if (use == "both") {
      cols_to_score <- c(paste0("min_", var), paste0("max_", var))
      score_cols <- c(paste0("min_", var, "_score"), paste0("max_", var, "_score"))
    } else if (use == "min") {
      cols_to_score <- paste0("min_", var)
      score_cols <- paste0("min_", var, "_score")
    } else {
      cols_to_score <- paste0("max_", var)
      score_cols <- paste0("max_", var, "_score")
    }

    for (j in seq_along(cols_to_score)) {
      src_col <- cols_to_score[j]
      dst_col <- score_cols[j]
      if (!src_col %in% names(data)) next

      # Build the range condition
      if (is.na(lo) && !is.na(hi)) {
        range_expr <- substitute(val <= hi, list(val = as.name(src_col), hi = hi))
      } else if (!is.na(lo) && is.na(hi)) {
        range_expr <- substitute(val >= lo, list(val = as.name(src_col), lo = lo))
      } else {
        range_expr <- substitute(val >= lo & val <= hi,
                                 list(val = as.name(src_col), lo = lo, hi = hi))
      }

      # Add special condition if needed
      if (!is.na(condition) && condition == "ph_missing") {
        ph_col <- if (grepl("^min_", src_col)) "min_ph" else "max_ph"
        if (ph_col %in% names(data)) {
          full_expr <- substitute(is.na(ph) & range,
                                  list(ph = as.name(ph_col), range = range_expr))
          data[eval(full_expr), (dst_col) := pts]
        }
      } else {
        data[eval(range_expr), (dst_col) := pts]
      }
    }
  }

  data
}


#' Calculate the APACHE II score using lookup tables.
#'
#' Replaces the old hardcoded calculate_apache_ii_score function.
#' Uses CSV lookup tables for scoring ranges, but retains code for
#' special cases: AaDO2/PaO2 oxygenation, GCS (15 - score), creatinine
#' doubling for renal failure, and chronic health points.
#'
#' @param data Dataframe containing physiology variables.
#'   Should be the output of get_score_variables, after fix_units and
#'   fix_implausible_values have been run.
#' @param imputation Either "normal" (missing = 0 points) or "none" (missing = NA).
#' @param scoring_path Path to the APACHE II scoring CSV. Defaults to the
#'   package-installed file.
#'
#' @return A data.table with the APACHE II score column added.
#' @import data.table
#' @export
calculate_apache_ii_score <- function(data, imputation = "normal",
                                      scoring_path = system.file("apache_ii_scoring.csv",
                                                                 package = "SeverityScoresOMOP")) {
  data <- as.data.table(data)

  if (!imputation %in% c("normal", "none")) {
    stop("The imputation type must be either 'normal' or 'none', not '", imputation, "'")
  }

  # Check required columns
  apache <- c(
    "max_temp", "min_temp", "min_wcc", "max_wcc",
    "max_fio2", "min_paco2", "min_pao2", "min_hematocrit", "max_hematocrit",
    "min_hr", "max_hr", "min_rr", "max_rr", "min_ph", "max_ph",
    "min_bicarbonate", "max_bicarbonate", "min_sodium", "max_sodium",
    "min_potassium", "max_potassium", "min_creatinine",
    "max_creatinine", "age"
  )
  check_required_columns(data, apache, "calculate_apache_ii_score")

  total_variable <- if (imputation == "normal") "apache_ii_score" else "apache_ii_score_no_imputation"

  # --- Table-driven scoring for standard variables ---
  data <- mean_arterial_pressure(data)
  data <- apply_scoring_table(data, scoring_path, imputation)

  # --- Special case: AaDO2 / PaO2 oxygenation ---
  default_pts <- if (imputation == "normal") 0L else NA_integer_
  data[, aado2_score := default_pts]
  data[, aado2 := (max_fio2 * 710) - (min_paco2 * 1.25) - min_pao2]

  data[max_fio2 >= 0.5 & aado2 >= 500, aado2_score := 4L]
  data[max_fio2 < 0.5 & min_pao2 < 55, aado2_score := 4L]
  data[max_fio2 >= 0.5 & aado2 >= 350 & aado2 < 500, aado2_score := 3L]
  data[max_fio2 < 0.5 & min_pao2 >= 55 & min_pao2 <= 60, aado2_score := 3L]
  data[max_fio2 >= 0.5 & aado2 >= 200 & aado2 < 350, aado2_score := 2L]
  data[max_fio2 < 0.5 & min_pao2 > 60 & min_pao2 <= 70, aado2_score := 1L]
  data[max_fio2 >= 0.5 & aado2 < 200, aado2_score := 0L]
  data[max_fio2 < 0.5 & min_pao2 > 70, aado2_score := 0L]

  # --- Special case: GCS ---
  data <- total_gcs(data)
  data[, gcs_score := 15L - as.integer(min_gcs)]
  if (imputation == "normal") {
    data[is.na(gcs_score), gcs_score := 0L]
  }

  # --- Special case: creatinine doubling for renal failure ---
  data <- renal_failure(data)
  data[renal_failure == 1 & !is.na(min_creatinine_score), min_creatinine_score := min_creatinine_score * 2L]
  data[renal_failure == 1 & !is.na(max_creatinine_score), max_creatinine_score := max_creatinine_score * 2L]

  # --- Special case: chronic health ---
  data <- comorbidities(data)
  data <- emergency_admission(data)
  data[, chronic_score := default_pts]
  data[comorbidity == 0, chronic_score := 0L]
  data[comorbidity > 0 & emergency_admission == 0, chronic_score := 2L]
  data[comorbidity > 0 & emergency_admission == 1, chronic_score := 5L]

  # --- Total score: worst of min/max for each component ---
  data[, (total_variable) :=
         pmax(min_temp_score, max_temp_score, na.rm = TRUE) +
         pmax(min_wcc_score, max_wcc_score, na.rm = TRUE) +
         pmax(min_map_score, max_map_score, na.rm = TRUE) +
         aado2_score +
         pmax(min_hematocrit_score, max_hematocrit_score, na.rm = TRUE) +
         pmax(min_hr_score, max_hr_score, na.rm = TRUE) +
         pmax(min_rr_score, max_rr_score, na.rm = TRUE) +
         pmax(min_ph_score, max_ph_score,
              min_bicarbonate_score, max_bicarbonate_score, na.rm = TRUE) +
         pmax(min_sodium_score, max_sodium_score, na.rm = TRUE) +
         pmax(min_potassium_score, max_potassium_score, na.rm = TRUE) +
         gcs_score +
         pmax(min_creatinine_score, max_creatinine_score, na.rm = TRUE) +
         age_score +
         chronic_score]

  # Clean up intermediate columns
  score_cols <- grep("_score$", names(data), value = TRUE)
  internal_cols <- setdiff(score_cols, total_variable)
  data[, (c(internal_cols, "aado2")) := NULL]

  data
}


#' Calculate the SOFA score using lookup tables.
#'
#' Replaces the old hardcoded calculate_sofa_score function.
#' Uses CSV lookup tables for scoring ranges, but retains code for
#' special cases: PF ratio (depends on mechanical ventilation status).
#'
#' @param data Dataframe containing physiology variables.
#' @param imputation Either "normal" (missing = 0 points) or "none" (missing = NA).
#' @param scoring_path Path to the SOFA scoring CSV.
#'
#' @return A data.table with the SOFA score column added.
#' @import data.table
#' @export
calculate_sofa_score <- function(data, imputation = "normal",
                                 scoring_path = system.file("sofa_scoring.csv",
                                                            package = "SeverityScoresOMOP")) {
  data <- as.data.table(data)

  if (!imputation %in% c("normal", "none")) {
    stop("The imputation type must be either 'normal' or 'none', not '", imputation, "'")
  }

  sofa <- c("min_platelet", "max_fio2", "min_pao2", "max_creatinine", "max_bilirubin")
  check_required_columns(data, sofa, "calculate_sofa_score")

  total_variable <- if (imputation == "normal") "sofa_score" else "sofa_score_no_imputation"

  # --- Table-driven scoring for standard variables ---
  data <- mean_arterial_pressure(data)
  data <- total_gcs(data)
  data <- apply_scoring_table(data, scoring_path, imputation)

  # --- Special case: PF ratio (depends on mechanical ventilation) ---
  default_pts <- if (imputation == "normal") 0L else NA_integer_
  data <- mechanical_ventilation(data)
  data[, pf_ratio := min_pao2 / max_fio2]
  data[, pf_ratio_score := default_pts]

  data[pf_ratio >= 400, pf_ratio_score := 0L]
  data[pf_ratio >= 300 & pf_ratio < 400, pf_ratio_score := 1L]
  data[pf_ratio >= 200 & pf_ratio < 300, pf_ratio_score := 2L]
  data[pf_ratio >= 100 & pf_ratio < 200 & mechanical_ventilation == 1, pf_ratio_score := 3L]
  data[pf_ratio < 100 & mechanical_ventilation == 1, pf_ratio_score := 4L]

  # --- Total score ---
  data[, (total_variable) :=
         pf_ratio_score +
         min_platelet_score +
         max_bilirubin_score +
         min_map_score +
         max_creatinine_score +
         min_gcs_score]

  # Clean up
  score_cols <- grep("_score$", names(data), value = TRUE)
  internal_cols <- setdiff(score_cols, total_variable)
  data[, (c(internal_cols, "pf_ratio")) := NULL]

  data
}
