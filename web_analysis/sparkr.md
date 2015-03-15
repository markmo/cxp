### To setup

Start R
Sys.setenv(SPARK_VERSION="1.3.0")
Sys.setenv(SPARK_HADOOP_VERSION="2.6.0")
library(devtools)
install_github("amplab-extras/SparkR-pkg", ref="master", subdir="pkg")

### To run

library(SparkR)
sc <- sparkR.init(master="local[*]")
dictionary <- textFile(sc, "hdfs://localhost:9000/lab/cxp/dictionary.csv")
take(dictionary, 3)

data <- textFile(sc, "hdfs://localhost:9000/lab/cxp/output/features")
take(data, 10)
count(data)

parseFields <- function(record) {
    Sys.setlocale("LC_ALL", "C") # necessary for strsplit() to work correctly
    parts <- strsplit(record, ",")[[1]]
    list(customerId=parts[1], webRequest=parts[2], pageViewsPerSession=parts[3], sessionLengthSeconds=parts[4], averageSessionLengthSeconds=parts[5])
}

parsedRDD <- lapply(data, parseFields)
cache(parsedRDD)

take(parsedRDD, 5)

ipAddresses <- lapply(parsedRDD, function(x) { x$customerId})
take(ipAddresses, 10)

_Top 10 customers by average session length_

top10 <- take(sortBy(parsedRDD, function(x) {x[["averageSessionLengthSeconds"]]}, F), 10) # F is a boolean that indicates descending sort order; default is ascending