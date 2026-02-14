###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department,
# University of Florida.

# clear all
cat("\014"); rm(list=ls());

# set seed to my phone number
set.seed(2028349824)

# smallest number in this version of R
e =.Machine$double.xmin

# maximum size of datasets in this script
N=Inf

# set directory
mydir = "C:/Users/Fer/UF Dropbox/Fernando Jose Brito Gonzalez/Fernando_Brito_Dissertation/h2a_labor"

setwd(mydir)

library(jsonlite)

# load functions and packages for this script
source(paste0(mydir,"/scripts/R/myfuns_h2ajson.R"))

# ##############################################################################





# Directory containing zip files
target_dir <- paste0(mydir,"/data/jo")

# List all zip files
zip_files <- list.files(
 path = target_dir,
 pattern = "\\.zip$",
 full.names = TRUE
)

# Loop and unzip each file into the same directory
for (zf in zip_files) {
 unzip(zipfile = zf, exdir = target_dir)
}








################################################################################
################################################################################
# Text description of job postings are contained in DOL's ETA-790A forms
#	which can be gathered either by:

# (1) downloading structured JSON files from DOL's seasonal job data feed, which
# are also available from 10/02.2019, the begging of fiscal year 2020, until
# current date but with an irrecoverable gap between 12/2023 and 02/2024. Fast.

# update.h2a.datafeed("2024-12-01") # last update: 2025-02-11


# h2a.json[[4938]]=NULL # this file is weird
var4 = c("addmcSectionName","addmcSectionNumber","addmcSectionDetails")

# append all decompressed files
df.text = data.frame()
t0=Sys.time()
path = paste0(mydir,"\\data\\jo\\");files = dir(path, pattern = "_jo.json");
for(i in 1:length(files)){ #length(files):(length(files)-1)
 
 
 h2a.json=fromJSON(paste0(file.path(path),files)[i],simplifyDataFrame=F)
 
 # extract addendum C of "Terms and Conditions"
 
 h2a.json=lapply(h2a.json,json.tr)
 
 df.text00 = do.call(bind_rows, lapply(h2a.json, json.df,"termsAndConditions"))
 df.text01 = do.call(bind_rows, lapply(h2a.json, json.df,"jobFrontTextFields"))
 df.text02 = do.call(bind_rows, lapply(h2a.json, json.df,"jobBeginDate"))
 df.text0  = bind_rows(df.text00,df.text01) %>% 
  filter(!duplicated(.)) %>% 
  filter(addmcSectionNumber=="A.8a") %>% 
  group_by(caseNumber) %>%
  dplyr::summarize(addmcSectionDetails=paste0(unique(addmcSectionDetails), collapse = "; ")) %>% 
  ungroup() %>% 
  merge(.,df.text02,by=c("caseNumber")) %>% 
  mutate(dateSubmitted=as.Date(V1, format = "%d-%b-%Y")) %>% 
  mutate(source="DOL https://seasonaljobs.dol.gov/")
 
 df.text=bind_rows(df.text,df.text0)
 cat("\n\n",i,"\n",Sys.time()-t0,"\n",format(object.size(df.text), units = "auto"),"\n\n")
 
 df_stats <- df.text %>%
  mutate(
   n_chars  = str_length(addmcSectionDetails),
   n_words  = str_count(addmcSectionDetails, "\\S+")
  )
 
 summary_stats <- df_stats %>%
  summarise(
   n_docs            = n(),
   total_words       = sum(n_words),
   unique_words      = length(unique(unlist(str_split(addmcSectionDetails, "\\s+")))),
   mean_words        = mean(n_words),
   median_words      = median(n_words),
   min_words         = min(n_words),
   max_words         = max(n_words),
   total_characters  = sum(n_chars),
   mean_characters   = mean(n_chars)
  )
 
 print(summary_stats)
 
 
 
}