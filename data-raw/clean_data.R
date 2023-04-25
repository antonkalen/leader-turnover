
# Load packages -----------------------------------------------------------

library(here)
library(readr)
library(janitor)
library(arrow)
library(dplyr)


# Load raw data -----------------------------------------------------------

raw_data <- read_csv2(
  here("data-raw/TillAnton.csv"),
  na = c("", "-999")
)


# Clean data --------------------------------------------------------------

cleaned_data <- raw_data |>
  clean_names() |>
  mutate(
    kon = factor(kon, levels = 1:2, labels = c("Man", "Kvinna")),
    utbildning = factor(
      utbildning,
      levels = 1:5,
      labels = c(
        "Förgymnasial",
        "Gymnasial",
        "Eftergymansial – 2år",
        "Eftergymnasial + 2år",
        "Forskarutbildning"
      )
    ),
    bransch2 = factor(
      bransch2,
      levels = 11:17,
      labels = c(
        "Politiker, VD och högre ämbetsmän",
        "Ekonomi, personal, marknadsföring och fösäljning",
        "It, logistik, FoU, fastighetsbolag, bygg och ingengörsverksamhet",
        "Utbildning",
        "Hälso och sjukvård",
        "Bank och finans",
        "Övrig service"
      )
    ),
    across(where(is.numeric), \(x) if_else(x < 0, NA, x)),
    across(
      c(justice, perceived_org_support),
      \(x) (x - 1) / 6
    ),
    across(
      c(
        quant_dem,
        emo_dem,
        role_con,
        possibilities_for_development,
        social_support_from_collegues,
        role_clarity,
        turnouver
      ),
      \(x) (x - 1) / 4
    )
  )

final_data <- cleaned_data |>
  select(
    id,
    alder = alderslut,
    utbildning,
    bransch = bransch2,
    quant_dem,
    emo_dem,
    role_con,
    justice,
    possibilities_for_development,
    influence,
    perceived_org_support,
    social_support_from_collegues,
    role_clarity,
    turnover = turnouver
  )


# Write data --------------------------------------------------------------

final_data |> write_parquet(here("data/turnover.parquet"))
