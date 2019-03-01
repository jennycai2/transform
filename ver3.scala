val rdd1 = sc.textFile("/Users/jenny/Downloads/bc_web_words.csv")
val rdd2 = rdd1.distinct()
val rdd3 = rdd2.map(l => {
         val array1 = l.split(";")
         (array1(0), array1(1))
})

val result = rdd3.reduceByKey((word1, word2) => word1 + "| " + word2)
result.foreach(println)