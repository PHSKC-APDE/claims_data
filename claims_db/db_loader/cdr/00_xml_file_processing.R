library(xml2)
library(tidyverse)
library(apde.etl)

# 1. Read the XML file
xml_data <- read_xml("//dphcifs/APDE-CDIP/Mcaid-Mcare/cdr_raw/20260729/extract/2021/1.3.6.1.4.1.38630.2.1.1.13.999.5.2.5.211124174156590268332759252.xml")

