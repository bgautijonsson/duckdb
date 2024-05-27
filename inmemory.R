data.table::fwrite(data.table::rbindlist(rep(list(iris), 100000)), "out.csv")
for (i in 1:2) {
  data.table::fwrite(data.table::rbindlist(rep(list(iris), 100000)), "out.csv", append = TRUE)
}

library(tictoc)
library(dplyr)
library(arrow)
library(data.table)
library(duckplyr)

arrow::open_csv_dataset("out.csv") |> 
  arrow::write_parquet("out.parquet")



# dplyr -------------------------------------------------------------------
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


# arrow -------------------------------------------------------------------
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


# duckplyr ----------------------------------------------------------------
duckplyr::methods_overwrite()
d <- arrow::read_parquet("out.parquet") |> 
  as_duckplyr_df()

tic()
d |> 
  summarise(
    mean = mean(Sepal.Length),
    n = n(),
    .by = Species
  ) 
duckplyr_time <- toc()
rm(d)
duckplyr::methods_restore()


# data.table --------------------------------------------------------------
d <- fread("out.csv")

tic()
d[, .(mean = mean(Sepal.Length),n = length(Sepal.Length)), .(Species)]
datatable_time <- toc()
rm(d)



# Results -----------------------------------------------------------------
dplyr_time$callback_msg
arrow_time$callback_msg
duckplyr_time$callback_msg
datatable_time$callback_msg