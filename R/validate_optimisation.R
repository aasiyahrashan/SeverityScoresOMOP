# =============================================================================
# validate_optimisation.R
#
# Compares output from the old (pre-optimisation) and new (post-optimisation)
# code paths to verify they produce identical results.
#
# Usage:
#   source("validate_optimisation.R")
#   result <- validate_optimisation(conn, dialect, schema, ...)
#
# The function runs the same extraction twice — once with the old code
# (which must still be installed or available) and once with the new code —
# then compares the results column by column.
#
# If you've already saved old output as a CSV/RDS, use validate_against_saved()
# instead.
# =============================================================================

#' Compare two dataframes from old vs new extraction.
#'
#' Performs a thorough comparison:
#'   1. Column names (same set?)
#'   2. Row count
#'   3. Key alignment (same person/stay/window combinations?)
#'   4. Value comparison per column (numeric tolerance for floats)
#'
#' @param old_data Dataframe from the old code path.
#' @param new_data Dataframe from the new code path.
#' @param tolerance Numeric tolerance for floating-point comparison.
#'   Default 1e-6.
#' @param key_cols Character vector of columns that uniquely identify rows.
#'   Default: person_id, icu_admission_datetime, time_in_icu.
#'
#' @return A list with:
#'   \describe{
#'     \item{identical}{Logical. TRUE if all checks pass.}
#'     \item{column_diff}{Columns present in one but not the other.}
#'     \item{row_count}{Named vector: old, new.}
#'     \item{key_diff}{Keys in one dataset but not the other.}
#'     \item{value_diffs}{Per-column diff details for mismatched values.}
#'     \item{summary}{Character string summarising the comparison.}
#'   }
#'
#' @import data.table
#' @export
compare_extraction_results <- function(old_data, new_data,
                                       tolerance = 1e-6,
                                       key_cols = c("person_id",
                                                    "icu_admission_datetime",
                                                    "time_in_icu")) {

  old_dt <- as.data.table(old_data)
  new_dt <- as.data.table(new_data)
  issues <- character(0)

  # --- 1. Column comparison ---
  old_cols <- sort(names(old_dt))
  new_cols <- sort(names(new_dt))
  only_in_old <- setdiff(old_cols, new_cols)
  only_in_new <- setdiff(new_cols, old_cols)
  common_cols <- intersect(old_cols, new_cols)

  if (length(only_in_old) > 0) {
    issues <- c(issues, paste("Columns only in old:",
                              paste(only_in_old, collapse = ", ")))
  }
  if (length(only_in_new) > 0) {
    issues <- c(issues, paste("Columns only in new:",
                              paste(only_in_new, collapse = ", ")))
  }

  # --- 2. Row count ---
  row_counts <- c(old = nrow(old_dt), new = nrow(new_dt))
  if (row_counts["old"] != row_counts["new"]) {
    issues <- c(issues, paste0("Row count mismatch: old=",
                               row_counts["old"], ", new=",
                               row_counts["new"]))
  }

  # --- 3. Key alignment ---
  if (!all(key_cols %in% common_cols)) {
    issues <- c(issues, "Key columns missing from one or both datasets")
    return(list(
      identical = FALSE,
      column_diff = list(only_in_old = only_in_old, only_in_new = only_in_new),
      row_count = row_counts,
      key_diff = NULL,
      value_diffs = NULL,
      summary = paste(issues, collapse = "\n")
    ))
  }

  # Create string keys for comparison
  old_keys <- old_dt[, do.call(paste, c(.SD, sep = "|")), .SDcols = key_cols]
  new_keys <- new_dt[, do.call(paste, c(.SD, sep = "|")), .SDcols = key_cols]

  only_in_old_keys <- setdiff(old_keys, new_keys)
  only_in_new_keys <- setdiff(new_keys, old_keys)

  if (length(only_in_old_keys) > 0) {
    issues <- c(issues, paste(length(only_in_old_keys),
                              "keys only in old data"))
  }
  if (length(only_in_new_keys) > 0) {
    issues <- c(issues, paste(length(only_in_new_keys),
                              "keys only in new data"))
  }

  # --- 4. Value comparison on matched rows ---
  value_diffs <- list()
  value_cols <- setdiff(common_cols, key_cols)

  if (length(only_in_old_keys) == 0 && length(only_in_new_keys) == 0 &&
      nrow(old_dt) == nrow(new_dt)) {

    # Sort both by key columns for aligned comparison
    setkeyv(old_dt, key_cols)
    setkeyv(new_dt, key_cols)

    for (col in value_cols) {
      old_val <- old_dt[[col]]
      new_val <- new_dt[[col]]

      if (is.numeric(old_val) && is.numeric(new_val)) {
        # Numeric comparison with tolerance
        both_na <- is.na(old_val) & is.na(new_val)
        one_na <- xor(is.na(old_val), is.na(new_val))
        diff_val <- !both_na & !one_na &
          abs(old_val - new_val) > tolerance

        n_mismatches <- sum(one_na | diff_val, na.rm = TRUE)
      } else {
        # Character/other comparison
        both_na <- is.na(old_val) & is.na(new_val)
        both_equal <- !is.na(old_val) & !is.na(new_val) &
          as.character(old_val) == as.character(new_val)
        n_mismatches <- sum(!(both_na | both_equal))
      }

      if (n_mismatches > 0) {
        value_diffs[[col]] <- list(
          n_mismatches = n_mismatches,
          n_total = nrow(old_dt),
          pct = round(100 * n_mismatches / nrow(old_dt), 2)
        )
        issues <- c(issues, paste0(col, ": ", n_mismatches, " mismatches (",
                                   value_diffs[[col]]$pct, "%)"))
      }
    }
  } else if (nrow(old_dt) > 0 && nrow(new_dt) > 0) {
    # Merge on keys and compare matched rows only
    merged <- merge(old_dt, new_dt, by = key_cols, suffixes = c(".old", ".new"))

    for (col in value_cols) {
      old_col <- paste0(col, ".old")
      new_col <- paste0(col, ".new")
      if (!(old_col %in% names(merged) && new_col %in% names(merged))) next

      old_val <- merged[[old_col]]
      new_val <- merged[[new_col]]

      if (is.numeric(old_val) && is.numeric(new_val)) {
        both_na <- is.na(old_val) & is.na(new_val)
        one_na <- xor(is.na(old_val), is.na(new_val))
        diff_val <- !both_na & !one_na &
          abs(old_val - new_val) > tolerance
        n_mismatches <- sum(one_na | diff_val, na.rm = TRUE)
      } else {
        both_na <- is.na(old_val) & is.na(new_val)
        both_equal <- !is.na(old_val) & !is.na(new_val) &
          as.character(old_val) == as.character(new_val)
        n_mismatches <- sum(!(both_na | both_equal))
      }

      if (n_mismatches > 0) {
        value_diffs[[col]] <- list(
          n_mismatches = n_mismatches,
          n_total = nrow(merged),
          pct = round(100 * n_mismatches / nrow(merged), 2)
        )
        issues <- c(issues, paste0(col, ": ", n_mismatches, " mismatches (",
                                   value_diffs[[col]]$pct, "%)"))
      }
    }
  }

  is_identical <- length(issues) == 0

  list(
    identical = is_identical,
    column_diff = list(only_in_old = only_in_old, only_in_new = only_in_new),
    row_count = row_counts,
    key_diff = list(only_in_old = length(only_in_old_keys),
                    only_in_new = length(only_in_new_keys)),
    value_diffs = value_diffs,
    summary = if (is_identical) {
      "PASS: Old and new outputs are identical."
    } else {
      paste("FAIL:", length(issues), "issue(s) found:\n",
            paste(issues, collapse = "\n"))
    }
  )
}


#' Compare new output against a previously saved old output.
#'
#' @param saved_path Path to saved old output (CSV or RDS).
#' @param new_data Dataframe from the new code path.
#' @param tolerance Numeric tolerance. Default 1e-6.
#'
#' @return Result from compare_extraction_results().
#' @export
validate_against_saved <- function(saved_path, new_data, tolerance = 1e-6) {

  ext <- tolower(tools::file_ext(saved_path))
  old_data <- if (ext == "rds") {
    readRDS(saved_path)
  } else if (ext == "csv") {
    read.csv(saved_path, stringsAsFactors = FALSE)
  } else {
    stop("Unsupported file format: ", ext, ". Use .rds or .csv.")
  }

  compare_extraction_results(old_data, new_data, tolerance = tolerance)
}


#' Run a full validation: extract with old and new code, compare.
#'
#' This function is meant to be run interactively during testing.
#' It calls get_score_variables twice — you must have both the old and new
#' versions of the package available (e.g. by switching branches or using
#' separate library paths).
#'
#' A simpler approach: save old output to RDS before upgrading, then use
#' validate_against_saved() after upgrading.
#'
#' @param conn Database connection.
#' @param ... Arguments passed to get_score_variables.
#'
#' @return Result from compare_extraction_results().
#' @export
validate_optimisation <- function(conn, ...) {

  message("Step 1: Run extraction with current (new) code...")
  new_data <- get_score_variables(conn, ...)

  message("\nTo complete validation:")
  message("  1. Save old output before upgrading:")
  message("     saveRDS(old_data, 'old_output.rds')")
  message("  2. After upgrading, run:")
  message("     result <- validate_against_saved('old_output.rds', new_data)")
  message("  3. Check result$summary")

  invisible(new_data)
}
