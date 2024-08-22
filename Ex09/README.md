# Exercise 9
As usual, **you may not write any loops** in your code.

When asked for Spark programs, **all calculations must be done with Spark DataFrames**. With small test data sets, it may be possible to bring the data into a Pandas DataFrame (or similar) and do the calculation locally. Of course, that's not the point: keep the data in Spark until we have some rules for when it can be safely collected back to the driver process.

## Aside: Big Data data files

Distributing input data for this part of the course is challenging. We will distribute a few different input sets so you can test your programs at different scales. See some more detail on the data sets instructions page.

tl;dr: You'll get a toy-sized data set in the `.zip` file you're used to downloading. Bigger data sets can be found to do more serious “big data” testing.

For the exercises (and in general with big data), you should start with small data sets to get the basics working, then scale up as you work out performance problems.

## Getting Started with Spark
For this question, see the program `first_spark.py` in this week. We want to get it running on your computer (or a CSIL Linux workstation): see the Running Spark Jobs Locally instructions for guidance. This is generally the right way to start working on Spark programs: locally with small data sets where you can test quickly.

This program takes the `xyz-*` data sets as input. These are randomly-generated values to give us something to work with. The program groups the values by their `id % 10` value, sums the x values, and outputs in CSV files. [Don't look for any meaning in these calculations: they are a random thing to calculate so we can see Spark do something.]

You should be able to run it with a command like:
```
spark-submit first_spark.py xyz-1 output
```

Assuming it runs correctly, you should be able to see its output (in CSV files) with a command like this:
```
cat output/part-* | less
```
To make sure you have to at least look at the code, **add a column to the output** which is the mean average of the y values in each group. (Hint: [avg](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.avg.html).)

## Extract-Transform-Load GHCN data
You have worked several times with data from the [GHCN](https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/global-historical-climatology-network-ghcn). We have always extracted the parts you needed: the full data set is >160MB of gzipped CSV per year. It is cumbersome to deal with without big data tools. But now…

When doing work with the maximum daily temperatures, it's not completely trivial to get that out of the original data files. This is very much an ETL task, but one where the big data tools become necessary.

Write a program `weather_etl.py` that takes two command-line arguments: the input and output directories, so it can be started like this:
```
spark-submit weather_etl.py weather-1 output # locally
spark-submit weather_etl.py /courses/353/weather-1 output # cluster
```
You'll find some code to get you started in weather_etl_hint.py. There is a directory of data files distributed as `weather-1` in the ZIP file. You'll also find (as described above) medium- and large-sized data sets `weather-2` and `weather-3`. These are partitioned subsets of the original GHCN data, but the exact same file format.

Things that need to be done to get the data into a more usable shape:
1. Read the input directory of `.csv.gz` files.
2. Keep only the records we care about:
    a. field `qflag` (quality flag) is null; ([Hint](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.Column.isNull.html))
    b. the `station` starts with 'CA'; ([Hint option 1](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.Column.startswith.html); [Hint option 2](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.substring.html))
    c. the observation is 'TMAX'. 
3. Divide the temperature by 10 so it's actually in °C, and call the resulting column `tmax`.
4. Keep only the columns `station`, `date`, and `tmax`.
5. Write the result as a directory of JSON files GZIP compressed (in the Spark one-JSON-object-per-line way). 

Here are two lines of correct output (after uncompressing with `gunzip` or similar)
```
{"station":"CA001173242","date":"20161203","tmax":1.0}
{"station":"CA004038116","date":"20161203","tmax":4.0}
```

Since the output here is compressed and a little larger, you'll need to uncompress and page the output to view it:
```
cat output/* | zless # local
hdfs dfs -cat output/* | zless # cluster (not applicable)
```
## Reddit Average Scores
I had originally put the “Reddit Average Scores” question on this exercise, but moved it to next week. You might want to start looking at that question and “Spark Overhead” from Exercise10 this week, but I won't force you to submit (until next week).
