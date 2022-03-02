# Load csv files to SQLite

# Load requires libraries
library(DBI)
library(RSQLite)
library(stringr)

# Create database in SQLite
db_connection = dbConnect(drv = SQLite(), dbname = 'formula1.db')

# Get CSV files names
csv_files = list.files(path = "./data/raw", pattern = "csv")
size = length(csv_files)
for (aux in csv_files) {
  # Prepare string
  table_name = paste("data/raw/", aux, sep = "")
  # Load csv file to a dataframe 
  df = readr::read_csv(table_name)
  
  # Remove the suffix ".csv" from the table name
  table_name = str_extract(aux, regex("\\w*"))
  
  # Convert primary keys classes
  ## Table circuits
  if (table_name == 'circuits'){
    # Edit dataframe
    df$circuitId = as.integer(df$circuitId)
    }
  
  ## Table constructor_results
  if (table_name == 'constructor_results'){
    df$constructorResultsId = as.integer(df$constructorResultsId)
    df$raceId = as.integer(df$raceId)
    df$constructorId = as.integer(df$constructorId)
  }
  
  ## Table constructor_standings
  if (table_name == 'constructor_standings'){
    df$constructorStandingsId = as.integer(df$constructorStandingsId)
    df$raceId = as.integer(df$raceId)
    df$constructorId = as.integer(df$constructorId)
    df$position = as.integer(df$position)
    df$positionText = as.character(df$positionText)
    df$wins = as.integer(df$wins)
  }
  
  ## Table constructors
  if (table_name == 'constructors'){
    df$constructorId = as.integer(df$constructorId)
  }
  
  ## Table driver_standings
  if (table_name == 'driver_standings'){
    df$driverStandingsId = as.integer(df$driverStandingsId)
    df$raceId = as.integer(df$raceId)
    df$driverId = as.integer(df$driverId)
    df$position = as.integer(df$position)
    df$positionText = as.character(df$positionText)
    df$wins = as.integer(df$wins)
  }
  
  ## Table drivers
  if (table_name == 'drivers'){
    df$driverId = as.integer(df$driverId)
    df$number = as.character(df$number)
  }
  
  ## Table lap_times
  if (table_name == 'lap_times'){
    df$raceId = as.integer(df$raceId)
    df$driverId = as.integer(df$driverId)
    df$lap = as.integer(df$lap)
    df$position = as.integer(df$position)
    df$time = as.character(df$time)
    df$milliseconds = as.integer(df$milliseconds)
  }
  
  ## Table pit_stops
  if (table_name == 'pit_stops'){
    df$raceId = as.integer(df$raceId)
    df$driverId = as.integer(df$driverId)
    df$stop = as.integer(df$stop)
    df$lap = as.integer(df$lap)
    df$time = as.character(df$time)
    df$duration = as.numeric(df$duration)
    df$milliseconds = as.integer(df$milliseconds)
  }
  
  ## Table qualifying
  if (table_name == 'qualifying'){
    df$qualifyId = as.integer(df$qualifyId)
    df$raceId = as.integer(df$raceId)
    df$driverId = as.integer(df$driverId)
    df$constructorId = as.integer(df$constructorId)
    df$number = as.integer(df$number)
    df$position = as.integer(df$position)
    df$q1 = as.character(df$q1)
    df$q2 = as.character(df$q2)
    df$q3 = as.character(df$q3)
  }
  
  ## Table races
  if (table_name == 'races'){
    df$raceId = as.integer(df$raceId)
    df$year = as.integer(df$year)
    df$round = as.integer(df$round)
    df$circuitId = as.integer(df$circuitId)
    df$name = as.character(df$name)
    df$date = as.character(df$date)
    df$time = as.character(df$time)
  }
  
  ## Table results
  if (table_name == 'results'){
    df$resultId = as.integer(df$resultId)
    df$raceId = as.integer(df$raceId)
    df$driverId = as.integer(df$driverId)
    df$constructorId = as.integer(df$constructorId)
    df$number = as.integer(df$number)
    df$grid = as.integer(df$grid)
    df$position = as.integer(df$position)
    df$positionText = as.character(df$positionText)
    df$positionOrder = as.integer(df$positionOrder)
    df$points = as.integer(df$points)
    df$laps = as.integer(df$laps)
    df$time = as.character(df$time)
    df$milliseconds = as.integer(df$milliseconds)
    df$fastestLap = as.character(df$fastestLap)
    df$fastestLapTime = as.character(df$fastestLapTime)
    df$fastestLapSpeed = as.numeric(df$fastestLapSpeed)
    df$statusId = as.integer(df$statusId)
  }
  
  ## Table seasons
  if (table_name == 'seasons'){
    df$year = as.integer(df$year)
  }
  
  ## Table status
  if (table_name == 'status'){
    df$statusId = as.integer(df$statusId)
    df$status = as.character(df$status)
  }
  
  # Write dataframe to a SQLite table
  dbWriteTable(conn = db_connection, name = table_name, value = df, overwrite = TRUE)
}

dbDisconnect(db_connection)











