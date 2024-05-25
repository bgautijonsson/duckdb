data.table::fwrite(data.table::rbindlist(rep(list(iris), 100000)), "out.csv")
for (i in 1:10) {
  data.table::fwrite(data.table::rbindlist(rep(list(iris), 100000)), "out.csv", append = TRUE)
}

library(tictoc)
library(duckdb)
library(dplyr)
library(arrow)
library(data.table)

arrow::open_csv_dataset("out.csv") |> 
  arrow::write_parquet("out.parquet")

d <- arrow::read_parquet("out.parquet")

tic()
d |> 
  dplyr::group_by(Species) |> 
  dplyr::summarise(
    mean = mean(Sepal.Length),
    n = n()
  )
dplyr_time <- toc()
rm(d)

d <- arrow::read_parquet("out.parquet", as_data_frame = FALSE)

tic()
d |> 
  group_by(Species) |> 
  summarise(
    mean = mean(Sepal.Length),
    n = n()
  ) |> 
  collect()
arrow_time <- toc()
rm(d)

d <- fread("out.csv")

tic()
d[, .(mean=mean(Sepal.Length),n=length(Sepal.Length)), .(Species)]
datatable_time <- toc()
rm(d)

dplyr_time$callback_msg
arrow_time$callback_msg
datatable_time$callback_msg