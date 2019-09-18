# Top music of longest session

1. Download 
[spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz)
, unzip in a dir and add bin subdirectory to your PATH

1. Download 
[data](http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz)
and unzip it in data directory

1. Test: sbt test

1. Create jar: sbt assembly

1. Run scripts/music.sh, it should create a file top__from_longest_sessions.tsv

