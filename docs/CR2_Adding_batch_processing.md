<style>
</style>

# **CR#2: Adding batch processing**

For each csv file in a given folder compute: number of observations,
P95, intensity as requests per second. To calculate intensity, use the number
of observations divided by time difference in seconds between max Date value
and min Date value.

Put the figures into a summary table which include following columns:

Date, n, P95, Time of Day, Intensity,

where

Date – the date when the sample was retrieved; extract from
Date column of a given csv file

n – the number of observations in a given csv file,

P95 – the 95th percentile of the Duration data in
a given csv file,

Time of Day – ‘Morning’, ‘Afternoon’, ‘Evening’; derived
from Date columns in a given csv file,

Intensity – request per second for a given csv file.

Save the summary table as a CSV file called 'summary.csv'


