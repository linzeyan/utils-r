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

ReadCSVData <- function(file_path = "") {
  df <- data.table::fread(file_path, header = T, colClasses = "character", encoding = "UTF-8")
  return(df)
}



