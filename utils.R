ConvertCharToVector <- function(c) {
  return(base::unlist(base::strsplit(c, split = " ")))
}

# convert_csv_to_parquet converts csv data to parquet format.
ConvertCSVToParquet <- function(file_path = "") {
  # load csv file
  rd_order <- data.table::fread(file_path, header = T, colClasses = "character")

  ext <- tools::file_ext(file_path)
  parquet_path <- base::gsub(stringr::str_c(".", ext), ".parquet", file_path)

  arrow_table <- arrow::as_arrow_table(rd_order)
  arrow::write_parquet(arrow_table, parquet_path, compression = "ZSTD")
}

# https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
InitialSparkR <- function(sp_home) {
  h <- base::Sys.getenv("SPARK_HOME")
  if (base::nchar(h) < 1) {
    base::Sys.setenv(SPARK_HOME = sp_home)
    base::.libPaths(base::c(base::file.path(sp_home, "R", "lib"), base::.libPaths()))
  }
  h <- base::Sys.getenv("SPARK_HOME")
  library(SparkR, lib.loc = base::c(base::file.path(h, "R", "lib")))
  sess <- SparkR::sparkR.session(master = "local[*]", sparkConfig = base::list(spark.driver.memory = "2g"), sparkHome = h)
  return(sess)
}

ReadCSVData <- function(file_path = "") {
  df <- data.table::fread(file_path, header = T, colClasses = "character", encoding = "UTF-8")
  return(df)
}

ReadParquet <- function(file_path) {
  data <- SparkR::read.parquet(file_path)
  return(data)
}

WriteParquet <- function(df, schema, file_path) {
  table <- arrow::Table$create(base::as.data.frame(df), schema = schema)
  arrow::write_parquet(table, file_path, compression = "ZSTD")
}

