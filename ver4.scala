//processing file 2
val rdd1 = sc.textFile("/Users/jenny/Downloads/bc_web_words2.csv")
rdd1.count
val rdd2 = rdd1.distinct()
rdd2.count
val pair2 = rdd2.map(l => {
         val array1 = l.split(";")
         (array1(0), array1(1))
})

//processing file 3
val rdd1 = sc.textFile("/Users/jenny/Downloads/bc_web_words3.csv")
rdd1.count
val rdd2 = rdd1.distinct()
rdd2.count
val pair3 = rdd2.map(l => {
         val array1 = l.split(";")
         (array1(0), array1(1))
})

//processing file 4
val rdd1 = sc.textFile("/Users/jenny/Downloads/bc_web_words4.csv")
rdd1.count
val rdd2 = rdd1.distinct()
rdd2.count
val pair4 = rdd2.map(l => {
         val array1 = l.split(";")
         (array1(0), array1(1))
})

//Combine the pair RDDs
val pairs = pair2.union(pair3).union(pair4)
//merge words according to domain
val result = pairs.reduceByKey((word1, word2) => word1 + "| " + word2).collect

//save the result to a file
import java.io._
val file = new File("/Users/jenny/Downloads/bc_web_words_merged.csv")
val bw = new BufferedWriter(new FileWriter(file))
for (c <- result) {
    bw.write(c + "\n")
}
println("wrote %s records", result.length) 
bw.close()