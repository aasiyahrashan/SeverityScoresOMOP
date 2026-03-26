#' Compute composite comorbidity conditions from count columns.
#'
#' Derives three composite binary flags that each combine multiple source
#' count columns. These cannot be handled by \code{\link{binarise_counts}}
#' because they aggregate across multiple source variables.
#'
#' The function operates per \code{person_id}, summing counts across all rows
#' for each patient before thresholding. This is appropriate when the input has
#' multiple rows per patient (e.g. daily assessments filtered to before the
#' index event).
#'
#' **Composite condition logic:**
#' \itemize{
#'   \item \code{cirrhosis}: \code{count_moderate_severe_liver_disease} +
#'         \code{count_mild_liver_disease} > 0
#'   \item \code{diabetes}: \code{count_diabetes_with_complications} +
#'         \code{count_diabetes_without_complications} > 0
#'   \item \code{cancer}: \code{count_malignancy} +
#'         \code{count_metastatic_solid_tumor} > 0
#' }
#'
#' Warns if any source columns are missing (treated as zero for that composite).
#' Skips a composite entirely only if ALL source columns are absent.
#'
#' @param data A data.table with \code{person_id} and \code{count_*} columns.
#' @return The data.table with \code{cirrhosis}, \code{diabetes}, and
#'   \code{cancer} columns added in-place (where source data is present).
#' @import data.table
#' @export
compute_composite_comorbidities <- function(data) {
  data <- as.data.table(data)

  composites <- list(
    cirrhosis = c("count_moderate_severe_liver_disease", "count_mild_liver_disease"),
    diabetes  = c("count_diabetes_with_complications",  "count_diabetes_without_complications"),
    cancer    = c("count_malignancy",                   "count_metastatic_solid_tumor")
  )

  for (flag in names(composites)) {
    expected <- composites[[flag]]
    present  <- intersect(expected, names(data))
    missing  <- setdiff(expected, names(data))
    if (length(missing) > 0) {
      warning("compute_composite_comorbidities: '", flag, "' is missing source column(s): ",
              paste(missing, collapse = ", "),
              ". These will be treated as zero.")
    }
    if (length(present) == 0) next
    data[, (flag) := fifelse(
      Reduce("+", lapply(.SD, function(x) sum(x, na.rm = TRUE))) > 0, 1L, 0L
    ), .SDcols = present, by = person_id]
  }

  data
}


#' Calculate Charlson Comorbidity Index from count columns.
#'
#' Computes the Charlson Comorbidity Index (CCI) score per patient from
#' \code{count_*} comorbidity columns, then categorises it into four levels.
#'
#' The function operates per \code{person_id}, summing counts across all rows
#' before scoring. Missing source columns are treated as zero with a warning.
#'
#' @param data A data.table with \code{person_id} and \code{count_*} comorbidity
#'   columns. Typically filtered to rows before the index event.
#' @param output_col Name of the output column. Default \code{"charlson_score"}.
#' @param categorise If TRUE (default), returns an ordered factor:
#'   None / Mild (1-2) / Moderate (3-4) / Severe (>=5).
#'   If FALSE, returns the raw integer score.
#'
#' @return The data.table with \code{charlson_score} (or \code{output_col}) added.
#'
#' @details
#' **Scoring weights (Charlson 1987):**
#' \itemize{
#'   \item 1 point: myocardial infarction, congestive heart failure,
#'         peripheral vascular disease, cerebrovascular disease, dementia,
#'         chronic pulmonary disease/pneumonia, rheumatic disease,
#'         peptic ulcer disease, mild liver disease,
#'         diabetes without complications
#'   \item 2 points: diabetes with complications, hemiplegia/paraplegia,
#'         renal failure, malignancy
#'   \item 3 points: moderate/severe liver disease
#'   \item 6 points: metastatic solid tumour, AIDS/HIV
#' }
#'
#' @references
#' Charlson ME, et al. A new method of classifying prognostic comorbidity in
#' longitudinal studies. J Chronic Dis. 1987;40(5):373-383.
#'
#' @import data.table
#' @export
calculate_charlson_score <- function(data,
                                     output_col = "charlson_score",
                                     categorise = TRUE) {
  data <- as.data.table(data)

  weights <- c(
    count_myocardial_infarction               = 1,
    count_congestive_heart_failure            = 1,
    count_peripheral_vascular_disease         = 1,
    count_cerebrovascular_disease             = 1,
    count_dementia                            = 1,
    count_chronic_pulmonary_disease_pneumonia = 1,
    count_rheumatic_disease                   = 1,
    count_peptic_ulcer_disease                = 1,
    count_mild_liver_disease                  = 1,
    count_diabetes_without_complications      = 1,
    count_diabetes_with_complications         = 2,
    count_hemiplegia_paraplegia               = 2,
    count_renal_failure                       = 2,
    count_malignancy                          = 2,
    count_moderate_severe_liver_disease       = 3,
    count_metastatic_solid_tumor              = 6,
    count_aids_hiv                            = 6
  )

  present <- intersect(names(weights), names(data))
  missing <- setdiff(names(weights), names(data))
  if (length(missing) > 0) {
    warning("calculate_charlson_score: missing column(s), treated as 0: ",
            paste(missing, collapse = ", "))
  }

  data[, (output_col) := {
    score <- 0L
    for (col in present) {
      score <- score + as.integer(sum(get(col), na.rm = TRUE) > 0) * weights[[col]]
    }
    score
  }, by = person_id]

  if (categorise) {
    data[, (output_col) := factor(
      fcase(
        get(output_col) == 0L,            "None",
        get(output_col) %in% c(1L, 2L),  "Mild",
        get(output_col) %in% c(3L, 4L),  "Moderate",
        get(output_col) >= 5L,            "Severe"
      ),
      levels = c("None", "Mild", "Moderate", "Severe")
    )]
  }

  data
}
