data.table::fwrite(data.table::rbindlist(rep(list(iris), 100000)), "out.csv")
for (i in 1:10) {
  data.table::fwrite(data.table::rbindlist(rep(list(iris), 100000)), "out.csv", append = TRUE)
}
data.table::fread("out.csv") |> 
  arrow::write_parquet("out.parquet")

library(tictoc)
library(duckdb)
library(dplyr)
library(arrow)


# duckdb csv --------------------------------------------------------------
tic()
con <- dbConnect(duckdb())
tbl(con, "out.csv") |> 
  group_by(Species) |> 
  summarise(
    mean = mean(Sepal.Length),
    n = n()
  ) |> 
  collect()
dbDisconnect(con)
duckdb_csv_time <- toc()
# duckdb parquet ----------------------------------------------------------
tic()
con <- dbConnect(duckdb())
tbl(con, "out.parquet") |> 
  group_by(Species) |> 
  summarise(
    mean = mean(Sepal.Length),
    n = n()
  ) |> 
  collect()
dbDisconnect(con)
duckdb_parquet_time <- toc()
# arrow csv ---------------------------------------------------------------
tic()
open_csv_dataset("out.csv") |> 
  group_by(Species) |> 
  summarise(
    mean = mean(Sepal.Length),
    n = n()
  ) |> 
  collect()
arrow_csv_time <- toc()
# Arrow on parquet --------------------------------------------------------
tic()
open_dataset("out.parquet") |> 
  group_by(Species) |> 
  summarise(
    mean = mean(Sepal.Length),
    n = n()
  ) |> 
  collect()
arrow_parquet_time <- toc()

duckdb_csv_time$callback_msg
duckdb_parquet_time$callback_msg
arrow_csv_time$callback_msg
arrow_parquet_time$callback_msg










