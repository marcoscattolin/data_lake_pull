library(tidyverse)
library(httr)
library(jsonlite)
library(curl)


# GET TOKEN ---------------------------------------------------------------
source("k:/dept/DIGITAL E-COMMERCE/E-COMMERCE/Report E-Commerce/data_lake/token/azure_token.r")



# LIST FILES --------------------------------------------------------------
path <- "sales/ecommerce"
r <- httr::GET(paste0("https://pradadigitaldatalake.azuredatalakestore.net/webhdfs/v1/",path,"?op=LISTSTATUS"),add_headers(Authorization = paste0("Bearer ",res$access_token)))
toJSON(jsonlite::fromJSON(content(r,"text")), pretty = TRUE) %>% fromJSON(simplifyDataFrame = T)



# READ DATA ---------------------------------------------------------------
data_lake_file <- "sales/ecommerce/ecommerce_2017.csv"
r <- httr::GET(paste0("https://pradadigitaldatalake.azuredatalakestore.net/webhdfs/v1/",data_lake_file,"?op=OPEN&read=true"),
               add_headers(Authorization = paste0("Bearer ",res$access_token)))

writeBin(content(r), "data/ecommerce_2017.csv")




