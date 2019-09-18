# Traffic analysis

1. Download 
[spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz)
, unzip in a dir and add bin subdirectory to your PATH

1. Download 
[data](http://data.dft.gov.uk.s3.amazonaws.com/road-accidents-safety-data/Stats19_Data_2005-2014.zip)
and unzip it in data directory

1. Test: sbt test

1. Create jar: sbt assembly

1. Run scripts/traffic.sh. Options:

   1. -o Domain of all features 
   
   1. -f Frequency of some features over the selected severity
   
   1. -r Relative frequency of some features over the selected severity
   
   1. -h Generate frequent accidents clusters (hot spots) in directory hotspots.json over the selected severity, 
   minimum accident in point and minimum distance between points
   
   1. -s \<severity> set the severity, default 1 (with casual fatalities)
   
   1. -a \<minimum accidents> set the minimum number of accident in a point for 
   clustering (default 2)
   
   1. -d \<minimun distance> set the minimum distance for points consider near for 
                                clustering (default 100)