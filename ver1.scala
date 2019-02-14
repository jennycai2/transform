val rdd1 = sc.textFile("/Users/jenny/Downloads/data.csv")

val rdd2 = rdd1.map(l => {
         val array1 = l.split(";")
         (array1(0), array1(1))
})
val rdd3 = rdd2.distinct()

val result = rdd3.reduceByKey((word1, word2) => word1 + "| " + word2)


result.foreach(println)
