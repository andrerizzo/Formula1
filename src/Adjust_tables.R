# Adjust SQLite tables' structure

# Load requires libraries
library(DBI)
library(RSQLite)

# Connect to database in SQLite
db_connection = dbConnect(drv = SQLite(), dbname = 'formula1.db')

# Update primary key
