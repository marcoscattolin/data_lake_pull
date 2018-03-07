library(tidyverse)
library(httr)
library(jsonlite)
library(curl)
library(lubridate)


# GET TOKEN ---------------------------------------------------------------
source("k:/dept/DIGITAL E-COMMERCE/E-COMMERCE/Report E-Commerce/data_lake/token/azure_token.r")



# LIST FILES --------------------------------------------------------------
path <- "sales/retail"
r <- httr::GET(paste0("https://pradadigitaldatalake.azuredatalakestore.net/webhdfs/v1/",path,"?op=LISTSTATUS"),add_headers(Authorization = paste0("Bearer ",res$access_token)))
files <- toJSON(jsonlite::fromJSON(content(r,"text")), pretty = TRUE) %>% fromJSON(simplifyDataFrame = T)
files <- files$FileStatuses$FileStatus



# READ DATA ---------------------------------------------------------------
fetch_file <- function(f){
        data_lake_file <- paste0(path,"/",f)
        r <- httr::GET(paste0("https://pradadigitaldatalake.azuredatalakestore.net/webhdfs/v1/",data_lake_file,"?op=OPEN&read=true"),
                       add_headers(Authorization = paste0("Bearer ",res$access_token)))
        
        writeBin(content(r), "data/temp.csv")
        res <- read_csv2("data/temp.csv", col_types = cols(.default = col_character()))
        file.remove("data/temp.csv")
        message(paste0("fetched file ",f))
        res
        
}


df <- map_df(files$pathSuffix, fetch_file)

