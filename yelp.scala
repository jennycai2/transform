val df = spark.read.json("/Users/jenny/Downloads/yelp_dataset/review.json")
df.show(3)
val df3 = df.select("stars", "text").limit(500)
val rev3 = df3.rdd.collect

import java.io._
val file = new File("/Users/jenny/Downloads/rev1.csv")
val bw = new BufferedWriter(new FileWriter(file))
for (c <- rev3) {
    bw.write(c + "\n")
}
println("wrote %s records", rev3.length) 
bw.close()

