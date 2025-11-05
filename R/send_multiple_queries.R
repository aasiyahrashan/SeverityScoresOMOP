#' Runs an SQL script containing multiple create , drop statements
#' There can only be one select statement, and this returns results from that.
#'
#' @param conn A connection object to a database
#' @param sql_script The script to be executed
#' @importFrom DBI dbGetQuery dbExecute
run_multiple_queries <- function(con, sql_script) {
  # Split SQL on semicolons (ignoring those inside quotes)
  statements <- unlist(strsplit(sql_script, ";", fixed = TRUE))
  for (stmt in statements) {
    stmt <- str_trim(stmt)
    if (nchar(stmt) == 0) next  # skip empty statements
    # Decide whether to execute or query
    if (grepl("^SELECT", toupper(stmt))) {
      message("Running SELECT ...")
      result <- dbGetQuery(con, stmt)
    } else {
      message("Running statement: ", substr(stmt, 1, 60), "...")
      dbExecute(con, stmt)
      result <- NULL
    }
  }
  return(result)  # returns result from the last SELECT if present
}
