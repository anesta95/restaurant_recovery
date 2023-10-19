library(readr)
library(httr)
library(dplyr)
library(magrittr)
library(purrr)
library(stringr)
library(lubridate)
library(tidyr)

### FUNCTIONS ####
# Function to grab BLS data programmatically
get_bls_data <- function(url, email) {
  bls_res <- GET(url = url, user_agent(email))
  stop_for_status(bls_res)
  
  bls_content <- content(bls_res, 
                         as = "parsed",
                         type = "text/tab-separated-values",
                         encoding = "UTF-8",
                         col_names = T,
                         col_types = cols(.default = col_character()),
                         trim_ws = T
  )
  return(bls_content)
  
}

# Custom function to calculate YoY, Mo2M annualized, and Mo2M changes in 
# CPI food away from home.
date_lag <- function(x, y, period_chg) {
  if (period_chg == "yoy") {
    if (n_distinct(month(x)) == 6) {
      res <- round(((y / lead(y, n = 6)) - 1) * 100, 2)
    } else {
      res <- round(((y / lead(y, n = 12)) - 1) * 100, 2)
    }
  } else if (period_chg == "mo2m_ann") {
    if (n_distinct(month(x)) == 6) {
      res <- round((((y / lead(y)) ^ 6) - 1) * 100, 2)
    } else {
      res <- round((((y / lead(y, n = 2)) ^ 6) - 1) * 100, 2)
    }
  } else if (period_chg == "mo2m") {
    if (n_distinct(month(x)) == 6) {
      res <- round(((y / lead(y)) - 1) * 100, 2)
    } else {
      res <- round(((y / lead(y, n = 2)) - 1) * 100, 2)
    }
  }
  return(res)
}

### REFERENCE DATA ###
# MSA FIPS join file

msa_fips_join <- read_csv("./reference_data/msa_id_join_table.csv",
                          col_names = T, col_types = "cccccc")

### HISTORICAL DATA ###
# Reading in historical data file


### DATA GATHERING ###

## SPENDING OPPORTUNITY INSIGHTS ##
oi_affinity_raw <- read_csv("https://raw.githubusercontent.com/OpportunityInsights/EconomicTracker/main/data/Affinity%20-%20City%20-%20Daily.csv",
         col_names = T, col_types = cols(.default = col_character()))

oi_affinity_raw_natl <- read_csv("https://raw.githubusercontent.com/OpportunityInsights/EconomicTracker/main/data/Affinity%20-%20National%20-%20Daily.csv",
         col_names = T, col_types = cols(.default = col_character()))

oi_affinity_present <- oi_affinity_raw %>% 
  filter(spend_acf != ".", cityid %in% msa_fips_join$oi_id) %>% 
  mutate(date = base::as.Date(paste(year, month, day, sep = "-")),
         spend_acf = as.numeric(spend_acf) * 100) %>% 
  select(date, cityid, spend_acf)

oi_affinity_present_natl <- oi_affinity_raw_natl %>% 
  filter(spend_acf != ".") %>% 
  mutate(date = base::as.Date(paste(year, month, day, sep = "-")),
         spend_acf = as.numeric(spend_acf) * 100,
         cityid = "0") %>% 
  select(date, cityid, spend_acf)

oi_affinity_present_combo <- bind_rows(oi_affinity_present, oi_affinity_present_natl)

oi_affinity_joined <- left_join(msa_fips_join, oi_affinity_present_combo, by = c("oi_id" = "cityid"))

write_csv(oi_affinity_joined, "./restaurant_industry_data/accomodation_food_service_spending_oi.csv")

## PRICES BLS CPI ##
bls_cpi_raw <- list_rbind(
  map(c("https://download.bls.gov/pub/time.series/cu/cu.data.10.OtherWest",
      "https://download.bls.gov/pub/time.series/cu/cu.data.7.OtherNorthEast",
      "https://download.bls.gov/pub/time.series/cu/cu.data.8.OtherNorthCentral",
      "https://download.bls.gov/pub/time.series/cu/cu.data.9.OtherSouth",
      "https://download.bls.gov/pub/time.series/cu/cu.data.11.USFoodBeverage"), 
      function(x) {
        
        reg_cpi <- get_bls_data(x, "adriannesta@gmail.com")
        
        message(paste("Done with", 
                       str_extract(x, "(?<=\\d{1,2}\\.)\\w+")))
        Sys.sleep(5)
        return(reg_cpi)
      })
  )

bls_cpi_present <- bls_cpi_raw %>% 
  filter(period != "M13") %>% 
  mutate(seas_adj = str_sub(series_id, start = 3, end = 3),
         period_code = str_sub(series_id, start = 4, end = 4),
         region = str_sub(series_id, start = 5, end = 8),
         item_code = str_sub(series_id, start = 9, end = 16),
         date = base::as.Date(paste0(year, "-", 
                                     str_remove(period, "M"), "-01")),
         value = as.numeric(value)) %>% 
  filter(date >= base::as.Date("2018-01-01"),
         period_code == "R",
         item_code == "SEFV",
         seas_adj == "U",
         region %in% msa_fips_join$cpi_fips) %>% 
  select(series_id, date, region, value) %>% 
  arrange(series_id, desc(date))
  
bls_cpi_present_calc <- bls_cpi_present %>% 
  group_by(series_id, region) %>% 
  mutate(yoy_pct_chg = date_lag(date, value, "yoy"),
         mo2m_annl_chg = date_lag(date, value, "mo2m_ann"),
         mo2m_pct_chg = date_lag(date, value, "mo2m")) %>% 
  ungroup()

bls_cpi_joined <- left_join(msa_fips_join, bls_cpi_present_calc, by = c("cpi_fips" = "region"))

write_csv(bls_cpi_joined, "./restaurant_industry_data/restaurant_prices_bls_cpi.csv")

## EARNINGS, EMPLOYMENT, AND ESTABLISHMENTS BLS QCEW ##
qcew_filenames <- list.files("./reference_data/qcew_raw_data")

bls_qcew_present <- list_rbind(map(
  qcew_filenames, function(x) {
    
    raw <- read_csv(
      paste0("./reference_data/qcew_raw_data/", x),
      col_names = T, col_types = cols(.default = col_character())) 
    
    
    present <- raw %>% 
      filter(own_code == "5", area_fips %in% msa_fips_join$qcew_fips) %>% 
      select(area_fips, industry_code, year, qtr, qtrly_estabs_count, month1_emplvl,
             month2_emplvl, month3_emplvl, avg_wkly_wage) %>% 
      pivot_longer(cols = ends_with("_emplvl"),
                   names_to = "qtr_month",
                   names_prefix = "month",
                   values_to = "employment") %>% 
      mutate(qtr_month = as.numeric(str_extract(qtr_month, "\\d{1}")),
             month = as.character(case_when(qtr == "1" ~ qtr_month,
                               qtr == "2" ~ 3 + qtr_month,
                               qtr == "3" ~ 6 + qtr_month,
                               qtr == "4" ~ 9 + qtr_month,
                               
                               )),
             date = base::as.Date(paste0(year, "-", str_pad(month, 
                                                            width = 2, 
                                                            side = "left",
                                                            pad = "0"), "-01")),
             qtrly_estabs_count = as.numeric(qtrly_estabs_count),
             avg_wkly_wage = as.numeric(avg_wkly_wage),
             employment = as.numeric(employment)) %>% 
      select(-c(year, qtr, qtr_month, month))
    
    message(paste("Done with year", unique(year(present$date))))
  
    
    return(present)
    
  }
))

bls_qcew_joined <- left_join(msa_fips_join, bls_qcew_present,
                             by = c("qcew_fips" = "area_fips")) %>% 
  arrange(area_name, desc(date)) %>% 
  mutate(across(c("avg_wkly_wage", "employment"), ~if_else(.x == 0, NA_real_, .x)))

write_csv(bls_qcew_joined, 
          "./restaurant_industry_data/restaurant_emp_wage_estabs_bls_qcew.csv")
