# Load required libraries
library(tidyverse)
library(lubridate)
library(zoo)
library(nbastatR)
library(DBI)
library(RPostgres)

memory.limit(size = 16000)  # Increase to 16GB


# Connect to the RDS database
conn <- dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("DB_HOST"),       # Use environment variable
  port = as.integer(Sys.getenv("DB_PORT")),  # Use environment variable
  dbname = Sys.getenv("DB_NAME"),    # Use environment variable
  user = Sys.getenv("DB_USER"),      # Use environment variable
  password = Sys.getenv("DB_PASSWORD")  # Use environment variable
)


# Fetch team logs for the desired season
team_logs <- game_logs(seasons = 2025, result_types = "team")

# Filter for games in the past 7 days
# Assuming 'dateGame' is in the returned data and is of Date type,
# you can filter using Sys.Date() - 7 for the last week's games.
team_logs <- team_logs %>%
  filter(dateGame >= (Sys.Date() - 1))

games <- team_logs %>%
  mutate(
    slugTeamHome = ifelse(locationGame == "H", slugTeam, slugOpponent),
    slugTeamAway = ifelse(locationGame == "A", slugTeam, slugOpponent)
  ) %>%
  distinct(idGame, slugTeamHome, slugTeamAway)

#if (future::supportsMulticore()) future::plan(future::multiprocess)  
#if (future::supportsMulticore() == FALSE) future::plan(future::multisession)

# Set up the future plan for parallel processing
if (future::supportsMulticore()) {
  future::plan(future::multisession)  # Use multisession for parallel processing
} else {
  future::plan(future::sequential)    # Fallback to sequential if multicore is not supported
}



# Choose how many games to fetch. Here we fetch all games from the past week.
num_game_ids_to_fetch <- nrow(games)

game_ids_subset <- head(games$idGame, num_game_ids_to_fetch)

# Fetch play-by-play data for the subset
play_logs_all <- play_by_play_v2(game_ids = game_ids_subset)

new_pbp <- play_logs_all %>%
  mutate(numberOriginal = numberEvent) %>%
  distinct(idGame, numberEvent, .keep_all = TRUE) %>%   # remove duplicate events
  mutate(secsLeftQuarter = (minuteRemainingQuarter * 60) + secondsRemainingQuarter) %>%             
  mutate(secsStartQuarter = case_when(                                                                    
    numberPeriod %in% c(1:5) ~ (numberPeriod - 1) * 720,
    TRUE ~ 2880 + (numberPeriod - 5) * 300
  )) %>%
  mutate(secsPassedQuarter = ifelse(numberPeriod %in% c(1:4), 720 - secsLeftQuarter, 300 - secsLeftQuarter),  
         secsPassedGame = secsPassedQuarter + secsStartQuarter) %>%
  arrange(idGame, secsPassedGame) %>%
  filter(numberEventMessageType != 18) %>%     # instant replay
  group_by(idGame) %>%
  mutate(numberEvent = row_number()) %>%  # new numberEvent column with events in the right order
  ungroup() %>%
  select(idGame, numberOriginal, numberEventMessageType, numberEventActionType, namePlayer1, namePlayer2, namePlayer3,                   
         slugTeamPlayer1, slugTeamPlayer2,  slugTeamPlayer3, numberPeriod, timeQuarter, secsPassedGame, 
         descriptionPlayHome, numberEvent, descriptionPlayVisitor) %>%
  mutate(shotPtsHome = case_when(
    numberEventMessageType == 3 & !str_detect(descriptionPlayHome, "MISS") ~ 1,                            
    numberEventMessageType == 1 & str_detect(descriptionPlayHome, "3PT") ~ 3,                              
    numberEventMessageType == 1 & !str_detect(descriptionPlayHome, "3PT") ~ 2,
    TRUE ~ 0
  )) %>%
  mutate(shotPtsAway = case_when(
    numberEventMessageType == 3 & !str_detect(descriptionPlayVisitor, "MISS") ~ 1,
    numberEventMessageType == 1 & str_detect(descriptionPlayVisitor, "3PT") ~ 3,
    numberEventMessageType == 1 & !str_detect(descriptionPlayVisitor, "3PT") ~ 2,
    TRUE ~ 0
  )) %>%
  group_by(idGame) %>%
  mutate(ptsHome = cumsum(shotPtsHome),
         ptsAway = cumsum(shotPtsAway)) %>%
  ungroup() %>%
  left_join(games) %>%
  select(idGame, numberOriginal, numberEventMessageType, numberEventActionType, slugTeamHome, slugTeamAway, slugTeamPlayer1,
         slugTeamPlayer2, slugTeamPlayer3, numberPeriod, timeQuarter, secsPassedGame, numberEvent, namePlayer1, namePlayer2, 
         namePlayer3, descriptionPlayHome, descriptionPlayVisitor, ptsHome, ptsAway, shotPtsHome, shotPtsAway) %>%
  mutate(marginBeforeHome = ptsHome - ptsAway - shotPtsHome + shotPtsAway,
         marginBeforeAway = ptsAway - ptsHome - shotPtsAway + shotPtsHome,
         timeQuarter = str_pad(timeQuarter, width = 5, pad = 0))

subs_made <- new_pbp %>%
  filter(numberEventMessageType == 8) %>%
  mutate(slugTeamLocation = ifelse(slugTeamPlayer1 == slugTeamHome, "Home", "Away")) %>%
  select(idGame, numberPeriod, timeQuarter, secsPassedGame, slugTeamPlayer = slugTeamPlayer1,
         slugTeamLocation, playerOut = namePlayer1, playerIn = namePlayer2) %>%
  pivot_longer(cols = starts_with("player"),
               names_to = "inOut",
               names_prefix = "player",
               values_to = "namePlayer") %>%
  group_by(idGame, numberPeriod, slugTeamPlayer, namePlayer) %>%
  filter(row_number() == 1) %>%
  ungroup()

others_qtr <- new_pbp %>%
  filter(numberEventMessageType != 8) %>%                             
  filter(!(numberEventMessageType == 6 & numberEventActionType %in% c(10, 11, 16, 18, 25))) %>%
  filter(!(numberEventMessageType == 11 & numberEventActionType %in% c(1, 4))) %>%
  pivot_longer(cols = starts_with("namePlayer"),
               names_to = "playerNumber",
               names_prefix = "namePlayer",
               values_to = "namePlayer") %>%
  mutate(slugTeamPlayer = case_when(
    playerNumber == 1 ~ slugTeamPlayer1,
    playerNumber == 2 ~ slugTeamPlayer2,
    playerNumber == 3 ~ slugTeamPlayer3,
    TRUE ~ "None"
  )) %>%
  mutate(slugTeamLocation = ifelse(slugTeamPlayer == slugTeamHome, "Home", "Away")) %>%
  filter(!is.na(namePlayer),
         !is.na(slugTeamPlayer)) %>%
  anti_join(subs_made %>% select(idGame, numberPeriod, slugTeamPlayer, namePlayer)) %>%
  distinct(idGame, numberPeriod, namePlayer, slugTeamPlayer, slugTeamLocation)

lineups_quarters <- subs_made %>%
  filter(inOut == "Out") %>%
  select(idGame, numberPeriod, slugTeamPlayer, namePlayer, slugTeamLocation) %>%
  bind_rows(others_qtr) %>%
  arrange(idGame, numberPeriod, slugTeamPlayer)

# Check for quarters with not exactly 5 players
lineups_quarters %>%
  count(idGame, numberPeriod, slugTeamPlayer) %>%
  filter(n != 5)

missing_players_ot <- tribble(
  ~idGame,   ~slugTeamPlayer,          ~namePlayer,     ~numberPeriod,
  21900023,        "DEN",           "Malik Beasley",          5,
  21900120,        "MIN",          "Treveon Graham",          5,
  21900272,        "ATL",         "De'Andre Hunter",          5,
  21900409,        "WAS",               "Ish Smith",          5,
  21900502,        "GSW",              "Damion Lee",          5,
  21900550,        "OKC",       "Terrance Ferguson",          5,
  21900563,        "DET",              "Tony Snell",          5,
  21900696,        "SAC",         "Harrison Barnes",          5,
  21900787,        "ATL",         "De'Andre Hunter",          5,
  21900892,        "HOU",             "Eric Gordon",          5,
  21901281,        "DEN",            "Monte Morris",          6
) %>%
  left_join(games) %>%
  mutate(slugTeamLocation = ifelse(slugTeamHome == slugTeamPlayer, "Home", "Away")) %>%
  select(-c(slugTeamHome, slugTeamAway))

lineups_quarters <- lineups_quarters %>%
  bind_rows(missing_players_ot) %>%
  arrange(idGame, numberPeriod, slugTeamPlayer)

lineup_subs <- new_pbp %>%
  filter(numberEventMessageType == 8) %>%
  select(idGame, numberPeriod, timeQuarter, secsPassedGame, slugTeamPlayer = slugTeamPlayer1, playerOut = namePlayer1,
         playerIn = namePlayer2, numberEvent) %>%
  arrange(idGame, numberEvent) %>%
  group_by(idGame, numberPeriod, slugTeamPlayer) %>%
  mutate(row1 = row_number()) %>%
  ungroup() %>%
  left_join(lineups_quarters %>%
              group_by(idGame, numberPeriod, slugTeamPlayer) %>%
              summarise(lineupBefore = paste(sort(unique(namePlayer)), collapse = ", ")) %>%
              ungroup() %>%
              mutate(row1 = 1)) %>%
  select(-row1)

lineup_subs <- lineup_subs %>%
  mutate(lineupBefore = str_split(lineupBefore, ", ")) %>%
  arrange(idGame, numberEvent) %>%
  group_by(idGame, numberPeriod, slugTeamPlayer) %>%
  mutate(
    lineupAfter = accumulate2(playerIn, playerOut, ~setdiff(c(..1, ..2), ..3), .init = lineupBefore[[1]])[-1],
    lineupBefore = coalesce(lineupBefore, lag(lineupAfter))
  ) %>%
  ungroup() %>%
  mutate_all(~map_chr(., ~paste(.x, collapse = ", "))) %>%
  mutate_at(vars("numberEvent", "numberPeriod", "idGame"), ~ as.integer(.)) %>%
  mutate(secsPassedGame = as.numeric(secsPassedGame)) %>%
  arrange(idGame, numberEvent) %>%
  left_join(lineups_quarters %>%
              distinct(idGame, slugTeamPlayer, slugTeamLocation)) %>%
  filter(!is.na(slugTeamLocation))

lineup_game <- new_pbp %>%
  group_by(idGame, numberPeriod) %>%
  mutate(row1 = row_number()) %>%
  ungroup() %>%
  left_join(lineups_quarters %>%
              group_by(idGame, numberPeriod, slugTeamLocation) %>%
              summarise(lineupBefore = paste(sort(unique(namePlayer)), collapse = ", ")) %>%
              ungroup() %>%
              pivot_wider(names_from = slugTeamLocation,
                          names_prefix = "lineupInitial",
                          values_from = lineupBefore) %>%
              mutate(row1 = 1)) %>%
  select(-row1) %>%
  left_join(lineup_subs %>%
              mutate(lineupBeforeHome = ifelse(slugTeamLocation == "Home", lineupBefore, NA),
                     lineupAfterHome = ifelse(slugTeamLocation == "Home", lineupAfter, NA),
                     lineupBeforeAway = ifelse(slugTeamLocation == "Away", lineupBefore, NA),
                     lineupAfterAway = ifelse(slugTeamLocation == "Away", lineupAfter, NA)) %>%
              select(idGame, numberPeriod, timeQuarter, secsPassedGame, numberEvent, slugTeamPlayer1 = slugTeamPlayer,
                     contains("Home"), contains("Away"))) %>%
  mutate_at(vars(c(lineupBeforeHome, lineupAfterHome)), ~ ifelse(!is.na(lineupInitialHome), lineupInitialHome, .)) %>%
  mutate_at(vars(c(lineupBeforeAway, lineupAfterAway)), ~ ifelse(!is.na(lineupInitialAway), lineupInitialAway, .)) %>%
  select(-starts_with("lineupInitial"))

lineup_game <- lineup_game %>%
  group_by(idGame, numberPeriod) %>%
  mutate(
    lineupHome = na.locf(lineupAfterHome, na.rm = FALSE),
    lineupAway = na.locf(lineupAfterAway, na.rm = FALSE),
    lineupHome = ifelse(is.na(lineupHome), na.locf(lineupBeforeHome, fromLast = TRUE, na.rm = FALSE), lineupHome),
    lineupAway = ifelse(is.na(lineupAway), na.locf(lineupBeforeAway, fromLast = TRUE, na.rm = FALSE), lineupAway),
    lineupHome = str_split(lineupHome, ", "),
    lineupAway = str_split(lineupAway, ", "),
    lineupHome = map_chr(lineupHome, ~ paste(sort(.), collapse = ", ")),
    lineupAway = map_chr(lineupAway, ~ paste(sort(.), collapse = ", "))
  ) %>%
  ungroup() %>%
  select(-c(starts_with("lineupBefore"), starts_with("lineupAfter")))

# Write to database
# dbWriteTable(conn, "pbp", lineup_game, overwrite = TRUE)
dbWriteTable(conn, "pbp", lineup_game, append = TRUE)

dbDisconnect(conn)
