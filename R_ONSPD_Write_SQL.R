
library(tidyverse)
library(odbc)
library(here)
library(glue)

# get SQL connection ----------------------------------------------------------

# testing this by loading into the KCOM SQL database
# ultimately - replace this to write to Michael Jeffrey's Postgres database
DBI_Connection <- dbConnect(odbc(),
                            driver = "SQL Server",
                            server = Sys.getenv("KCOM-SVR"),
                            database = 'Sandbox',
                            UID = Sys.getenv("KCOM-USR"),
                            Trusted_Connection = 'Yes')

# -------------------------------------------------------------------------
LOAD_SQL <- TRUE

# get the csv files to load
df_files <- tibble(filename = list.files(here('data-for-sql'))) %>% 
  mutate(tablename = str_remove(filename, '.csv')) %>% 
  filter(tablename == 'tbl_region_type')

for (i in seq_len(nrow(df_files))) {
  filetoload <- df_files[i, c('filename')] %>% pull()
  tablename <- df_files[i, c('tablename')] %>% pull()
  
  print(paste0(rep('-', 80), collapse=''))
  print(glue("Loading {filetoload} into table {tablename}"))
  
  # drop table if it exists
  sql_text <- glue("IF EXISTS (SELECT * 
                               FROM sys.tables
                               WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND NAME = '{tablename}')
                    DELETE FROM [dbo].[{tablename}] ")
  
  execute <- dbGetQuery(DBI_Connection, sql_text)
  
  # get the table and load it row by row (for now)
  table <- read_csv(here('data-for-sql',filetoload), col_types = cols(.default = 'c'))

  if (LOAD_SQL) {
    for (j in seq_len(nrow(table))) {
      
      if (j == 1) sql_cols <- paste0(names(table), collapse = ", ")
      sql_data <- paste0(str_replace_all(unlist(table[j,]), "'","''"), 
                         collapse = "', '")
      
      sql_text <- glue("INSERT INTO {tablename} ({sql_cols})
                     VALUES ('{sql_data}')")
      
      # there may be some nulls
      sql_text <- str_replace_all(sql_text, "'NA'", "NULL")
      
      execute <- dbGetQuery(DBI_Connection, sql_text)
      
      if  ((j %% 10000) == 0) print(glue("{Sys.time()} : {j} records loaded"))
    }
  } # LOAD_SQL
}
