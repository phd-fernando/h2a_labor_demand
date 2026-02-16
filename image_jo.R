library(dplyr)
library(corporaexplorer)

# job_orders: your data frame
job_orders2 <- df.text %>%
 mutate(
  Date = as.Date(dateSubmitted),              # must be Date class if date_based_corpus=TRUE
  Text = as.character(addmcSectionDetails)
 )

cx <- prepare_data(
 dataset = job_orders2,
 text_column = "Text",
 corpus_name = "Job Orders",
 columns_doc_info = c("Date", "Employer", "State", "Occupation", "Wage", "URL"),
 columns_for_ui_checkboxes = c("State", "Occupation", "H2A"),
 use_matrix = TRUE,
 matrix_without_punctuation = TRUE
)

explore(cx)   # launches the dashboard
