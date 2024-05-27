data.table::fwrite(data.table::rbindlist(rep(list(iris), 100000)), "small_out.csv")

library(tictoc)
library(duckdb)
library(dplyr)
library(data.table)
library(arrow)
library(bench)

d_datatable <- fread("small_out.csv")
d_dplyr <- readr::read_csv("small_out.csv")
con <- dbConnect(duckdb())
d_duckdb <- tbl(con, "small_out.csv")


bench::mark(
  "duckdb" = d_duckdb |> 
    group_by(Species) |> 
    summarise(
      mean = mean(Sepal.Length),
      n = n()
    ) |> 
    collect() |> 
    pull(mean) |> 
    min(),
  "data.table" = d_datatable[, .(mean = mean(Sepal.Length),n = length(Sepal.Length)), .(Species)]$mean |> min(),
  "dplyr" = d_dplyr |> 
    group_by(Species) |> 
    summarise(
      mean = mean(Sepal.Length),
      n = n()
    ) |> 
    pull(mean) |> 
    min()
)
