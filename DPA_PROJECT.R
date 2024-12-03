# Load necessary libraries
library(dplyr)     # For data manipulation
library(knitr)     # For printing output nicely
library(data.table)  # For efficient data manipulation
library(janitor)    # For data cleaning and formatting
library(tidyr)      # For data tidying
library(visdat)     # For visualizing missing data
library(arrow)      # For reading and writing Arrow files
library(lubridate)  # For working with dates and times
library(ggplot2)    # For data visualization

# Read the CSV file
df_raw <- read.csv("Traffic_Crashes_-_Crashes_20240302.csv")

# Print column names
print(colnames(df_raw))

# Print the number of rows and columns
cat("Number of rows:", nrow(df_raw), "\n")
cat("Number of columns:", ncol(df_raw), "\n")

# Display information about the dataset
str(df_raw)

# Rename columns
df_1 <- df_raw %>%
  rename(inj_non_incap = INJURIES_NON_INCAPACITATING,
         inj_report_not_evdnt = INJURIES_REPORTED_NOT_EVIDENT)

# Drop unwanted columns
df_1 <- df_raw %>%
  select(-CRASH_RECORD_ID, -CRASH_DATE_EST_I, -LANE_CNT, -LOCATION, -REPORT_TYPE, -DATE_POLICE_NOTIFIED,
         -PHOTOS_TAKEN_I, -STATEMENTS_TAKEN_I, -DOORING_I, -WORK_ZONE_I, -WORK_ZONE_TYPE, -WORKERS_PRESENT_I)

# Assuming df_1 is your dataframe
df_c <- df_1
colnames(df_c)

# Assuming df_c is your dataframe
df_c <- df_c %>%
  mutate(INTERSECTION_RELATED_I = if_else(is.na(INTERSECTION_RELATED_I), 'N', INTERSECTION_RELATED_I),
         NOT_RIGHT_OF_WAY_I = if_else(is.na(NOT_RIGHT_OF_WAY_I), 'N', NOT_RIGHT_OF_WAY_I),
         HIT_AND_RUN_I = if_else(is.na(HIT_AND_RUN_I), 'N', HIT_AND_RUN_I))

# Set columns to 0 where injuries_total is missing and crash_type is 'NO INJURY / DRIVE AWAY'
df_c <- df_c %>%
  mutate(INJURIES_TOTAL = ifelse(is.na(INJURIES_TOTAL) & CRASH_TYPE == 'NO INJURY / DRIVE AWAY', 0, INJURIES_TOTAL),
         INJURIES_FATAL = ifelse(is.na(INJURIES_TOTAL) & CRASH_TYPE == 'NO INJURY / DRIVE AWAY', 0, INJURIES_FATAL),
         INJURIES_INCAPACITATING = ifelse(is.na(INJURIES_TOTAL) & CRASH_TYPE == 'NO INJURY / DRIVE AWAY', 0, INJURIES_INCAPACITATING),
         INJURIES_NON_INCAPACITATING = ifelse(is.na(INJURIES_TOTAL) & CRASH_TYPE == 'NO INJURY / DRIVE AWAY', 0, INJURIES_NON_INCAPACITATING),
         INJURIES_REPORTED_NOT_EVIDENT = ifelse(is.na(INJURIES_TOTAL) & CRASH_TYPE == 'NO INJURY / DRIVE AWAY', 0, INJURIES_REPORTED_NOT_EVIDENT),
         INJURIES_NO_INDICATION = ifelse(is.na(INJURIES_TOTAL) & CRASH_TYPE == 'NO INJURY / DRIVE AWAY', 0, INJURIES_NO_INDICATION),
         INJURIES_UNKNOWN = ifelse(is.na(INJURIES_TOTAL) & CRASH_TYPE == 'NO INJURY / DRIVE AWAY', 0, INJURIES_UNKNOWN))

# Drop rows where injuries_total is missing
df_c <- df_c %>%
  filter(!is.na(INJURIES_TOTAL))

# wherever most_severe_injury is missing and injuries_total is 0
df_c <- df_c %>%
  mutate(MOST_SEVERE_INJURY = ifelse(is.na(MOST_SEVERE_INJURY) & INJURIES_TOTAL == 0,
                                     'NO INDICATION OF INJURY', MOST_SEVERE_INJURY))

# Print table of MOST_SEVERE_INJURY values
print(as.data.frame(table(df_c$MOST_SEVERE_INJURY, useNA = "ifany")))

ggplot(df_c, aes(x = MOST_SEVERE_INJURY)) +
  geom_bar() +  # Add bars
  labs(x = "MOST_SEVERE_INJURY", y = "Number of Records", title = "Number of Records per Category") +  # Add labels and title
  theme_minimal() +  # Minimal theme
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 

# Convert CRASH_DATE to datetime and create crash_year column
df_c <- df_c %>%
  mutate(CRASH_DATE = mdy_hms(CRASH_DATE),
         crash_year = year(CRASH_DATE))

# Create address column
df_c <- df_c %>%
  mutate(
    STREET_NO = as.character(STREET_NO),
    STREET_DIRECTION = as.character(STREET_DIRECTION),
    STREET_NAME = as.character(STREET_NAME),
    address = paste0(STREET_NO, " ", STREET_DIRECTION, " ", STREET_NAME)
  )

# Assuming df_c is your dataframe
df_c2 <- df_c

# Using base R unique function to drop duplicates
df_c2 <- unique(df_c)

# Using dplyr's distinct function
df_c2 <- distinct(df_c)

# Drop rows that have both latitude and longitude == 0
df_c2 <- df_c2 %>%
  filter(LATITUDE != 0 & LONGITUDE != 0)

# Convert columns to integer type
df_c2 <- df_c2 %>%
  mutate(
    BEAT_OF_OCCURRENCE = as.integer(BEAT_OF_OCCURRENCE),
    INJURIES_TOTAL = as.integer(INJURIES_TOTAL),
    INJURIES_FATAL = as.integer(INJURIES_FATAL),
    INJURIES_INCAPACITATING = as.integer(INJURIES_INCAPACITATING),
    INJURIES_NON_INCAPACITATING = as.integer(INJURIES_NON_INCAPACITATING),
    INJURIES_REPORTED_NOT_EVIDENT = as.integer(INJURIES_REPORTED_NOT_EVIDENT),
    INJURIES_NO_INDICATION = as.integer(INJURIES_NO_INDICATION)
  )

# Drop columns "not_right_of_way_i" and "injuries_unknown"
df_c2 <- df_c2 %>%
  select(-NOT_RIGHT_OF_WAY_I, -INJURIES_UNKNOWN)

# Inspect the structure of the dataframe
str(df_c2)

#write_parquet(df_c2, file_parquet_c)
write_parquet(df_c2, "Intmd_data.parquet")


# Read Parquet file into a dataframe
crash_df <- arrow::read_parquet("Intmd_data.parquet")

# Inspect the structure of the dataframe
str(crash_df)

# Rounding at mid-point to nearest round_unit
round_unit <- 5
crash_df$POSTED_SPEED_LIMIT <- (crash_df$POSTED_SPEED_LIMIT %/% round_unit * round_unit) + round((crash_df$POSTED_SPEED_LIMIT %% round_unit) / round_unit) * round_unit

# Counting the occurrences of each rounded value
speed_limit_counts <- table(crash_df$POSTED_SPEED_LIMIT)

# Convert the result to a data frame
speed_limit_counts_df <- as.data.frame(speed_limit_counts)
names(speed_limit_counts_df) <- c("Speed_Limit", "Count")

# Plotting the horizontal bar graph with count labels
ggplot(speed_limit_counts_df, aes(x = as.factor(Speed_Limit), y = Count)) +
  geom_bar(stat = "identity", fill = "skyblue") +
  geom_text(aes(label = Count), hjust = -0.2, vjust = 0.5, color = "black", size = 3) +  # Add count labels
  labs(x = "Count", y = "Speed Limit", title = "Occurrences of Rounded Speed Limits") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1)) +  # Rotate x-axis labels
  coord_flip()  # Flip the coordinates to create a horizontal plot


# Create 'has_injuries' column based on injuries_total
crash_df$has_injuries <- as.integer(crash_df$INJURIES_TOTAL > 0)

# Count the occurrences of each unique value in 'has_injuries' column
table(crash_df$has_injuries)

# Create 'has_fatal' column based on injuries_fatal
crash_df$has_fatal <- as.integer(crash_df$INJURIES_FATAL > 0)

# Count the occurrences of each unique value in 'has_fatal' column
table(crash_df$has_fatal)

# Create 'crash_time_of_day' column using cut function
crash_df$crash_time_of_day <- cut(crash_df$CRASH_HOUR, breaks = c(-Inf, 6, 12, 18, Inf), labels = c("overnight", "morning", "mid_day", "evening"))

# Count the occurrences of each category in 'crash_time_of_day' column
table(crash_df$crash_time_of_day)

# Define list of feature names
features_names <- c(
  'crash_date',
  'crash_year',
  'crash_month',
  'crash_day_of_week',
  'crash_hour',
  'crash_time_of_day', # New
  'latitude',
  'longitude',
  'beat_of_occurrence',
  'address',
  'street_no',
  'street_direction',
  'street_name',
  'posted_speed_limit',
  'traffic_control_device',
  'device_condition',
  'weather_condition',
  'lighting_condition',
  'trafficway_type',
  'alignment',
  'roadway_surface_cond',
  'road_defect',
  'first_crash_type',
  'prim_contributory_cause',
  'sec_contributory_cause',
  'num_units'
)

# Define list of target names
target_names <- c(
  'has_injuries',  # New
  'has_fatal',  # New
  'crash_type',
  'damage',
  'injuries_total',
  'injuries_fatal'
)

# Combine feature and target names
all_columns <- c(features_names, target_names)

colnames(crash_df)

# Assuming df_c is your dataframe
names(crash_df) <- tolower(names(crash_df))
colnames(crash_df)

file_crash_df_parquet <- "crash_df.parquet"

# Write dataframe to Parquet format
write_parquet(crash_df[all_columns], file_crash_df_parquet)

# Replace crash_df with your data frame variable name and all_columns with the columns you want to include
write_parquet(crash_df[, all_columns], "file_crash_df.parquet")

# Replace file_crash_df_parquet with the path to your Parquet file
crash_df <- read_parquet("file_crash_df.parquet")

# Replace file_crash_df_parquet with the path to your Parquet file
crash_df <- read_parquet("file_crash_df.parquet")

# Replace crash_df with your data frame variable name and target_names with the names of the columns you want to select
crash_targets <- crash_df %>%
  select(target_names)

# Print information about the selected columns
str(crash_targets)
str(crash_df)

# File path to the Parquet file
file_crash_df_parquet <- "crash_df.parquet"

# Read the Parquet file into a dataframe
crash_df <- arrow::read_parquet(file_crash_df_parquet)

# Print information about the dataframe
str(crash_df)

# Subset the data where crash_year > 2017 and crash_year < 2021
subset_df <- subset(crash_df, crash_year > 2017 & crash_year < 2021)

# Print the subsetted dataframe
head(subset_df)

# Filter rows based on the condition (crash_year > 2017 & crash_year < 2021)
crash_df <- crash_df %>%
  filter(crash_year > 2017 & crash_year < 2021)

# Retrieve column names of the dataframe crash_df
column_names <- colnames(crash_df)
column_names

nrow(crash_df)
table(crash_df$crash_year)

features_names <- c(
  'crash_date',
  'crash_year',
  'crash_month',
  'crash_day_of_week',
  'crash_hour',
  'crash_time_of_day', # New
  'latitude',
  'longitude',
  'beat_of_occurrence',
  'address',
  #    'street_no',
  #    'street_direction',
  #    'street_name',
  'posted_speed_limit',
  'traffic_control_device',
  'device_condition',
  'weather_condition',
  'lighting_condition',
  'trafficway_type',
  'alignment',
  'roadway_surface_cond',
  'road_defect',
  'first_crash_type',
  'prim_contributory_cause',
  'sec_contributory_cause',
  'num_units'
)

target_names <- c(
  'has_injuries',  # New
  'has_fatal',  # New
  'crash_type',
  'damage',
  'injuries_total',
  'injuries_fatal'
  #    'injuries_incapacitating',
  #    'inj_non_incap',
  #    'inj_report_not_evdnt',
  #    'injuries_no_indication',
  #    'most_severe_injury'
)

all_columns <- c(features_names, target_names)
all_columns

table(crash_df$posted_speed_limit)

table(round(crash_df$posted_speed_limit, -1))

crash_df %>%
  mutate(rounded_speed_limit = (posted_speed_limit %/% 10 * 10) + round((posted_speed_limit %% 10) / 10) * 10) %>%
  count(rounded_speed_limit)

crash_df %>%
  mutate(rounded_speed_limit = (posted_speed_limit %/% 5 * 5) + round((posted_speed_limit %% 5) / 5) * 5) %>%
  count(rounded_speed_limit)

rounding_function <- function(x) {
  (floor(x / 10) * 10) + round((x %% 10) / 10) * 10
}

result <- rounding_function(6)
result

crash_df %>%
  pull(beat_of_occurrence) %>%
  n_distinct()

crash_df %>%
  pull(address) %>%
  n_distinct()

crash_df %>%
  group_by(latitude, longitude) %>%
  summarise(n = n())

table(crash_df$crash_type)

table(as.integer(crash_df$injuries_total > 0))

table(as.integer(crash_df$injuries_fatal > 0))

table(crash_df$first_crash_type)

table(crash_df$trafficway_type)

table(crash_df$prim_contributory_cause)

table(crash_df$sec_contributory_cause)

table(crash_df$traffic_control_device)

summary(crash_df$injuries_total)

# Week day mapping
week_days <- c("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

# Weekday/Weekend mapping
is_weekday <- c("Weekend", "Weekday")

# Assuming crash_df is your dataframe
par(mfrow=c(3, 1))  # Set the layout to 3 rows and 1 column
for(year in unique(crash_df$crash_year)) {
  hist(subset(crash_df, crash_year == year)$crash_hour, 
       breaks = 24,
       main = paste("Crashes by Hour - Year", year),
       xlab = "Hour",
       ylab = "Frequency",
       col = "lightblue",
       xlim = c(0, 24),
       ylim = c(0, max(table(crash_df$crash_hour))),
       axes = FALSE)
  axis(1, at=seq(0, 24, by=2))  # Add x-axis labels every 2 hours
  axis(2)  # Add y-axis
}


# Filter dataframe for crashes with injuries
crashes_with_injuries <- subset(crash_df, has_injuries == 1)

# Plot histograms by crash_year
ggplot(crashes_with_injuries, aes(x = crash_hour)) +
  geom_histogram(binwidth = 1) +
  facet_wrap(~ crash_year, nrow = 3) +
  theme_minimal() +
  labs(x = "Crash Hour", y = "Count") +
  theme(legend.position = "none")

z <- cut(crash_df$crash_hour, breaks = c(-Inf, 6, 12, 18, Inf), labels = c('overnight', 'morning', 'mid_day', 'evening'))

head(z)

table(z)

plot(z)

sum(is.na(crash_df$crash_hour))

sum(is.na(z))

summary(crash_df$crash_hour)

crash_agg <- crash_df %>%
  group_by(crash_year, crash_month, crash_hour) %>%
  summarise(injuries_total = sum(injuries_total, na.rm = TRUE),
            injuries_fatal = sum(injuries_fatal, na.rm = TRUE)) %>%
  mutate(across(c(injuries_total, injuries_fatal), ~ifelse(is.na(.), 0, .))) %>%
  ungroup()

crash_pivot <- crash_agg %>%
  pivot_wider(names_from = crash_year, 
              values_from = c(injuries_total, injuries_fatal),
              values_fn = sum,
              names_sep = "_")

# Aggregate data
crash_agg <- crash_df %>%
  group_by(crash_year, crash_month, crash_time_of_day) %>%
  summarise(has_injuries = sum(has_injuries),
            has_fatal = sum(has_fatal),
            injuries_total = sum(injuries_total),
            injuries_fatal = sum(injuries_fatal)) %>%
  mutate_at(vars(has_injuries, has_fatal, injuries_total, injuries_fatal), ~ ifelse(is.na(.), 0, .))

# Plotting
ggplot(crash_agg, aes(x = crash_month, y = has_injuries, color = crash_time_of_day)) +
  geom_line() +
  facet_wrap(~ crash_year, ncol = 3) +
  labs(title = "Chicago Crashes - Time of Day Trends", y = "Number of Injuries") +
  theme_bw() +
  scale_x_continuous(breaks = 1:12)


# Aggregate data
crash_agg <- crash_df %>%
  group_by(crash_year, crash_month, crash_day_of_week) %>%
  summarise(has_injuries = sum(has_injuries),
            injuries_total = sum(injuries_total),
            injuries_fatal = sum(injuries_fatal)) %>%
  mutate_at(vars(has_injuries, injuries_total, injuries_fatal), ~ ifelse(is.na(.), 0, .))

# Plotting
ggplot(crash_agg, aes(x = crash_month, y = has_injuries, fill = factor(crash_day_of_week))) +
  geom_bar(stat = "identity", position = "dodge") +
  facet_wrap(~ crash_year, ncol = 3) +
  theme_minimal() +
  labs(x = "Month", y = "Total Injuries", title = "Total Injuries by Month and Day of Week") +
  scale_fill_brewer(palette = "Set1") +
  guides(fill = guide_legend(title = "Day of Week")) +
  scale_x_continuous(breaks = 1:12)

sum(crash_df$weather_condition == 'OTHER', na.rm = TRUE)

# Grouping and aggregating
crash_agg <- crash_df %>%
  group_by(crash_year, crash_month, lighting_condition) %>%
  summarise(has_injuries = sum(has_injuries, na.rm = TRUE),
            injuries_total = sum(injuries_total, na.rm = TRUE),
            injuries_fatal = sum(injuries_fatal, na.rm = TRUE)) %>%
  mutate_at(vars(has_injuries, injuries_total, injuries_fatal), ~ ifelse(is.na(.), 0, .)) %>%
  ungroup() %>%
  filter(lighting_condition != "UNKNOWN")

# Creating FacetGrid plot
ggplot(crash_agg, aes(x = crash_month, y = has_injuries, col = lighting_condition)) +
  geom_line() +
  facet_wrap(~ crash_year, ncol = 3) +
  labs(title = "Chicago Crashes - Lighting Condition Trends", y = "Has Injuries") +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))+
  scale_x_continuous(breaks = 1:12)

crash_agg <- crash_df %>%
  group_by(crash_year, crash_month, weather_condition) %>%
  summarise(has_injuries = sum(has_injuries),
            injuries_total = sum(injuries_total),
            injuries_fatal = sum(injuries_fatal)) %>%
  mutate(across(c(has_injuries, injuries_total, injuries_fatal), ~ifelse(is.na(.), 0, .))) %>%
  ungroup()

ggplot(crash_agg, aes(x = crash_month, y = has_injuries, color = weather_condition)) +
  geom_line() +
  facet_wrap(~crash_year, ncol = 3) +
  labs(title = "Chicago Crashes - Weather Condition Trends", y = "Total Injuries") +
  theme(plot.title = element_text(hjust = 0.5))+
  scale_x_continuous(breaks = 1:12)

# Grouping and aggregating
crash_agg <- crash_df %>%
  group_by(crash_year, crash_month, weather_condition) %>%
  summarise(has_injuries = sum(has_injuries),
            injuries_total = sum(injuries_total),
            injuries_fatal = sum(injuries_fatal)) %>%
  mutate_at(vars(has_injuries, injuries_total, injuries_fatal), ~ ifelse(is.na(.), 0, .)) %>%
  filter(!weather_condition %in% c("CLEAR")) %>%
  ungroup()

# Creating facet grid
ggplot(crash_agg, aes(x = crash_month, y = has_injuries, color = weather_condition)) +
  geom_line() +
  facet_wrap(~crash_year, ncol = 3) +
  labs(title = "Chicago Crashes - Weather Condition Trends (Excluding CLEAR)", y = "Has Injuries") +
  theme(plot.title = element_text(face = "bold"))+
  scale_x_continuous(breaks = 1:12)

# Create a copy of the dataframe
crash_sdf <- crash_df

# Select columns to be standardized
col_names <- c('injuries_total', 'injuries_fatal')
features <- crash_sdf[col_names]

# Standardize the selected columns
scaled_features <- scale(features)

# Update the original dataframe with standardized values
crash_sdf[col_names] <- scaled_features

# Display the first 5 rows of the updated dataframe
head(crash_sdf, 5)

# Plot Map
crash_agg <- crash_df %>%
  group_by(longitude, latitude) %>%
  summarise(crashes = n()) %>%
  ungroup()

head(crash_agg)

#  Create a copy of crash_df
crash_df_ <- crash_df

# Group by 'longitude', 'latitude', and 'crash_year', then summarize
crash_df_ <- crash_df_ %>%
  group_by(longitude, latitude, crash_year) %>%
  summarize(
    crashes = n(),
    has_injuries = max(has_injuries),
    is_weekday = max(is_weekday)  # is_weekday is a binary indicator
  ) %>%
  ungroup() %>%
  filter(crashes > 0)

head(crash_df_)

# Filter data for crash_year == 2018
crash_df_2018 <- subset(crash_df_, crash_year == 2018)

# Create scatter plot - 2018
ggplot(crash_df_2018, aes(x = longitude, y = latitude, size = crashes, color = has_injuries)) +
  geom_point(alpha = 0.5) +                # Add points with transparency
  scale_size_continuous(range = c(0, 5)) + # Adjust the size range
  labs(x = NULL, y = NULL, title = "Crashes in Chicago 2018") +  # Remove axis labels and set title
  theme_minimal() +  # Minimal theme
  theme(axis.text = element_blank(),  # Remove axis text
        axis.title = element_blank(),  # Remove axis title
        plot.title = element_text(hjust = 0.5))  # Center plot title

# Filter data for crash_year == 2019
crash_df_2019 <- subset(crash_df_, crash_year == 2019)

# Create scatter plot - 2019
ggplot(crash_df_2019, aes(x = longitude, y = latitude, size = crashes, color = has_injuries)) +
  geom_point(alpha = 0.5) +                # Add points with transparency
  scale_size_continuous(range = c(0, 5)) + # Adjust the size range
  labs(x = NULL, y = NULL, title = "Crashes in Chicago 2019") +  # Remove axis labels and set title
  theme_minimal() +  # Minimal theme
  theme(axis.text = element_blank(),  # Remove axis text
        axis.title = element_blank(),  # Remove axis title
        plot.title = element_text(hjust = 0.5))  # Center plot title

# Filter data for crash_year == 2020
crash_df_2020 <- subset(crash_df_, crash_year == 2020)

# Create scatter plot - 2019
ggplot(crash_df_2020, aes(x = longitude, y = latitude, size = crashes, color = has_injuries)) +
  geom_point(alpha = 0.5) +                # Add points with transparency
  scale_size_continuous(range = c(0, 5)) + # Adjust the size range
  labs(x = NULL, y = NULL, title = "Crashes in Chicago 2020") +  # Remove axis labels and set title
  theme_minimal() +  # Minimal theme
  theme(axis.text = element_blank(),  # Remove axis text
        axis.title = element_blank(),  # Remove axis title
        plot.title = element_text(hjust = 0.5))  # Center plot title

# Modeling
# File path to the Parquet file
file_crash_df_parquet <- "crash_df.parquet"

# Read the Parquet file into a dataframe
crash_df <- arrow::read_parquet(file_crash_df_parquet)

# Print information about the dataframe
dim(crash_df)

# Filter rows based on the condition (crash_year > 2017 & crash_year < 2021)
crash_df <- crash_df %>%
  filter(crash_year > 2017 & crash_year < 2021)
dim(crash_df)

# Week day mapping
week_days <- c("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

# Weekday/Weekend mapping
is_weekday <- c("Weekend", "Weekday")

# Define the condition
crash_df$is_weekday <- ifelse(crash_df$crash_day_of_week %in% c(1, 7), 0, 1)
table(crash_df$is_weekday)
features_names <- c(
  "crash_year",
  "crash_month",
  "crash_time_of_day",  # New
  "is_weekday",  # New
  "latitude",
  "longitude",
  "posted_speed_limit",
  "traffic_control_device",
  "device_condition",
  "weather_condition",
  "lighting_condition",
  "trafficway_type",
  "alignment",
  "roadway_surface_cond",
  "road_defect",
  "first_crash_type",
  "prim_contributory_cause",
  "sec_contributory_cause",
  "num_units"
)

target_names <- c(
  "has_injuries"  # New
)

all_columns <- c(features_names, target_names)
all_columns

# Filter data for the years 2018 to 2020
crash_df <- subset(crash_df, crash_year > 2017 & crash_year < 2021)

# Select all columns except 'crash_year' and filter for the year 2019
crash_2019_df <- subset(crash_df, crash_year == 2019, select = -c(crash_year))

# Display information about the dataframe
dim(crash_2019_df)

# Subset features columns
features <- crash_2019_df[, features_names[2:length(features_names)]]

# Extract target column
target <- crash_2019_df[, target_names]

# Print the shape of features and target
print(dim(features))
print(dim(target))

str(features)
str(target)

# Checking categorical columns
features %>%
  select_if(is.factor) %>%
  colnames()

# Convert 'crash_month' to a categorical variable
features$crash_month <- factor(features$crash_month)

str(features)

####################################################################################
####################################################################################
####################################################################################
####################################################################################
# Define parameters for grid search
params <- expand.grid(
  max_depth = round(log2(seq(2, 128, length.out = 3))),
  min_samples_split = seq(0.01, 0.5, length.out = 5)
)

# Perform grid search with cross-validation
set.seed(123)  # Set seed for reproducibility
cv_model <- train(
  x = X_train,
  y = as.factor(y_train),  # Convert target to factor
  method = "rpart",
  tuneGrid = params,
  trControl = trainControl(method = "cv", number = 5),
  metric = "F1"
)

# Print best hyperparameters
print("Best Hyperparameters:")
print(cv_model$bestTune)

# Plot grid search results
plot(cv_model)


####################################################################################
####################################################################################
####################################################################################
####################################################################################

# Split data into Features X and Target y
X <- features_dm
y <- target

# Print shapes of X and y
print("X and y shapes:")
print(dim(X), dim(y), "\n")

# Print target ratio
print("Target Ratio:")
print(prop.table(table(y)), "\n")

# Split data into Train and Test
set.seed(123)  # Set seed for reproducibility
train_indices <- sample(1:nrow(X), 0.8 * nrow(X), replace = FALSE)
X_train <- X[train_indices, ]
X_test <- X[-train_indices, ]
y_train <- y[train_indices]
y_test <- y[-train_indices]

# Print shapes of train and test sets
print("Train and Test shapes:")
print(dim(X_train), dim(X_test), dim(y_train), dim(y_test))

####################################################################################
####################################################################################
####################################################################################
####################################################################################

# Define the pipeline
cl_pl <- pipeline(steps = list(
  "scale" = scaler(),
  "classifier" = caret::trainControl(method = "glm", family = "binomial", classProbs = TRUE, summaryFunction = twoClassSummary),
  "method" = "glm"
))

# Define parameters for grid search
params <- expand.grid(C = 10^seq(-3, 3, length.out = 7))

# Create a grid search object
gs_lr <- caret::train(
  x = X_train,
  y = as.factor(y_train),
  method = cl_pl,
  tuneGrid = params,
  trControl = caret::trainControl(method = "cv", number = 5),
  metric = "ROC",
  verbose = FALSE
)

# Print tuning results for the 2019 train set
cat("2019 Train_Set Tuning Results:\n")
print(gs_lr$results)

# Print results for the 2019 test set
cat("\n2019 Test_Set Results:\n")
preds <- predict(gs_lr, X_test)
confusionMatrix(preds, as.factor(y_test))

# Print results for the 2020 data
cat("\n2020 Results:\n")
results <- predict(gs_lr, newdata = features_2020_dm)
confusionMatrix(results, as.factor(target_2020))



####################################################################################
####################################################################################
####################################################################################
####################################################################################

library(caret)
library(mlr)
library(dplyr)

# Define Preprocessing Pipeline
preprocess_pipeline <- makePreprocWrapper(
  learner = "classif.sgd",
  classif.sgd.scale = TRUE
)

# Define Classifier
classifier <- makeLearner("classif.sgd", predict.type = "prob")

# Define Parameters for Grid Search
param_grid <- list(
  classifier.sgd.l1 = seq(0, 1, by = 0.25),
  classifier.sgd.alpha = c(1e-4, 1e-3, 1e-2)
)

# Define Task
task <- makeClassifTask(id = "crash_injury_prediction", data = X_train, target = "has_injuries")

# Define Resampling Strategy
resampling <- makeResampleDesc("CV", iters = 5)

# Define Grid Search
grid_search <- makeTuneControlGrid()

# Perform Grid Search
grid_result <- tuneParams(learner = classifier,
                          task = task,
                          resampling = resampling,
                          measures = list(acc),
                          par.set = param_grid,
                          control = grid_search,
                          preproc = preprocess_pipeline)

# Print Tuning Results
print("2019 Train_Set Tuning Results:")
print(grid_result)

# Evaluate Model on Test Set
predictions <- predict(grid_result$learner, newdata = X_test)
accuracy <- confusionMatrix(predictions$data$response, y_test)$overall["Accuracy"]
print("2019 Test_Set Results:")
print(accuracy)

# Evaluate Model on 2020 Data
predictions_2020 <- predict(grid_result$learner, newdata = features_2020_dm)
accuracy_2020 <- confusionMatrix(predictions_2020$data$response, target_2020)$overall["Accuracy"]
print("2020 Results:")
print(accuracy_2020)

