scala> val readRDD = sc.wholeTextFiles("adl://<adls_name>.azuredatalakestore.net/datalake-prod/tmp/<application>/data/<table_name>/date_part=2018-09-17/FILENAME_2018-09-17_18-00-15.076.txt")
readRDD: org.apache.spark.rdd.RDD[(String, String)] = adl://<adls_name>.azuredatalakestore.net/datalake-prod/tmp/<application>/data/<table_name>/date_part=2018-09-17/FILENAME_2018-09-17_18-00-15.076.txt MapPartitionsRDD[10] at wholeTextFiles at <console>:24
 
sscala> val tranformRDD = readRDD.map(x => x._2.replaceAll("\\\\(\n|\r|\r\n)", "\\\\ ").split("\u0007").mkString("\u0007"))
tranformRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[15] at map at <console>:25
 
scala> tranformRDD.saveAsTextFile("adl://<adls_name>.azuredatalakestore.net/datalake-prod/raw/<application>/data/<table_name>/date_part=2018-09-47") 
