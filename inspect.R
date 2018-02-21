temp <- read_csv2("data/temp.csv", col_types = cols(.default = col_character()))


temp %>% 
        filter(brand == "PS" & commercial_class == "50", collection == "D") %>% View()
