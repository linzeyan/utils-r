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


# https://stackoverflow.com/questions/68589841/r-implementation-of-ecb-aes-128-not-compatible
pad <- function(plaintext) {
  block_length <- 16
  bytes_to_pad <- block_length - (nchar(plaintext) %% block_length)
  padded_raw <- c(charToRaw(plaintext), rep(as.raw(bytes_to_pad), times = bytes_to_pad))
  padded_raw
}

unpad <- function(plaintext) {
  padded_raw <- charToRaw(plaintext)
  last_byte <- tail(padded_raw, 1)
  bytes_to_remove <- as.integer(last_byte)
  if (bytes_to_remove > 0 && bytes_to_remove <= length(padded_raw)) {
    unpadded_raw <- head(padded_raw, -bytes_to_remove)
    return(rawToChar(unpadded_raw))
  } else {
    stop("Invalid padding")
  }
}

# AES ECB encrypt 
AesEcbEncrypt <- function(bytea_key, data) {
  aes_ecb <- digest::AES(key = bytea_key, mode = "ECB")
  enc_string <- aes_ecb$encrypt(pad(data))
  return(openssl::base64_encode(enc_string))
}

AesEcbDecrypt <- function(bytea_key, encode_data) {
  aes_ecb <- digest::AES(key = bytea_key, mode = "ECB")
  s <- aes_ecb$decrypt(openssl::base64_decode(encode_data))
  return(unpad(s))
}
