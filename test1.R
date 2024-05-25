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

tic()
d <- data.table::fread("out.csv")

d |> 
  dplyr::group_by(Species) |> 
  dplyr::summarise(
    mean = mean(Sepal.Length),
    n = n()
  )
inmemory_time <- toc()

tic()
read_parquet("out.parquet", as_data_frame = FALSE) |> 
  group_by(Species) |> 
  summarise(
    mean = mean(Sepal.Length),
    n = n()
  ) |> 
  collect()
arrow_inmemory_time <- toc()

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
arrow_inmemory_time$callback_msg
lobstr::obj_size(d)










