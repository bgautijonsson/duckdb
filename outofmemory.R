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
duckdb_time <- toc()



# Reading in CSV and dplyr ------------------------------------------------
tic()
d <- data.table::fread("out.csv")

d |> 
  dplyr::group_by(Species) |> 
  dplyr::summarise(
    mean = mean(Sepal.Length),
    n = n()
  )
inmemory_time <- toc()


# Arrow on parquet --------------------------------------------------------
tic()
open_dataset("out.parquet") |> 
  group_by(Species) |> 
  summarise(
    mean = mean(Sepal.Length),
    n = n()
  ) |> 
  collect()
parquet_time <- toc()

duckdb_time$callback_msg
inmemory_time$callback_msg
parquet_time$callback_msg
lobstr::obj_size(d)










