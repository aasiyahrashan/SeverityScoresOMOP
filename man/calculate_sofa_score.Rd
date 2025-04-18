% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sofa_functions.R
\name{calculate_sofa_score}
\alias{calculate_sofa_score}
\title{Calculates the SOFA score.
Assumes the units of measure have been fixed using the fix_sofa_units function.
Missing data is handled using normal imputation by default. If 'none', there is no imputation.}
\usage{
calculate_sofa_score(data, imputation = "normal")
}
\arguments{
\item{data}{Dataframe containing physiology variables and units of measure.}

\item{imputation}{Should be the output of the get_score_variables function with the 'severity score parameter set to SOFA}
}
\value{
A data frame with a variable containing the apache II score calculated.
}
\description{
Calculates the SOFA score.
Assumes the units of measure have been fixed using the fix_sofa_units function.
Missing data is handled using normal imputation by default. If 'none', there is no imputation.
}
