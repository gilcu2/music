# Top music of longest session

## Results

data/ReynaldoTopSongResult.csv

## Building, test, run

1. Download 
[spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz)
, unzip in a dir and add bin subdirectory to your PATH

1. Download 
[data](http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz)
and unzip the tsv files in data directory

1. Test: sbt test

1. Create jar: sbt assembly

1. Run scripts/topSongs.sh, it should create a file topSongs.tsv in data directory

## Explanation

To find the top songs of the largest session:

1. Find user sessions grouping tracks by user id, sort it by time and separate the tracks in different sessions if the time between then greater than 20 minutes  

1. Compute the length of the session, sort by size descending and the the first n

1. Take the artist and songs, group by artist and song, compute length, sort descending and take the first n

Everything implemented in Scala using Apache Spark SQL DataFrame and tested using ScalaTest