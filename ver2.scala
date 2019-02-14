// step 1, read into memory
val rdd_prospect = sc.textFile("/Users/jenny/Downloads/prospects-2019-02-13_combined.csv") 
val rdd_manufacturing = sc.textFile("/Users/jenny/Downloads/companies_electrical_manufacturing.csv") 

// step 2, check how many records, how many are unique
rdd_prospect.count
res18: Long = 2171
rdd_prospect.distinct().count
res19: Long = 1974

rdd_manufacturing.count
res20: Long = 3807
rdd_manufacturing.distinct().count
res21: Long = 2573

// step 3, look at two rows
rdd_prospect.take(2).foreach(println)
rdd_manufacturing.take(2).foreach(println)

// step 4, get the unique rows
val pros = rdd_prospect.distinct()
val manu =  rdd_manufacturing.distinct()

pros.take(2).foreach(println)
manu.take(2).foreach(println)

// step 5, process "prospect" to get the domain name from email address
// get email
val emailRdd = pros.map(l => {
         val array1 = l.split(",")
         if (array1.length >= 5) 
           array1[4]
         else 
           "NONE"
})

emailRdd.count
res41: Long = 1974

// get non empty email
val validEmailRdd = emailRdd.filter(line => !line.contains("NONE"))
validEmailRdd.count
res42: Long = 1971

emailRdd.take(4)
res25: Array[String] = Array(nathan.degidio@asd-lighting.com, seancfox@gmail.com, llee@btsled.com, jack@allstarlighting.com)

// get the domain from email
val validCompanyRdd = validEmailRdd.map(l => {
         val array1 = l.split("@")
           if (array1.length >= 2)
              array1(1)
           else
              "NONE"
}).filter(line => !line.contains("NONE"))

validCompanyRdd.count
res43: Long = 1966

validCompanyRdd.take(4)
res44: Array[String] = Array(asd-lighting.com, gmail.com, btsled.com, allstarlighting.com)

// remove domain of gmail.com and yahoo.com

val nonGmailRdd = validCompanyRdd.filter(line => !line.contains("gmail.com")).filter(line => !line.contains("yahoo.com")).distinct()

remove gmail
res45: Long = 1906
only 60 records have gmail address
remove yahoo email
res63: Long = 1877
only 29 records have yahoo address
remove domain name duplicates
res65: Long = 989

// convert rdd to an array
val contacted = nonGmailRdd.collect

Step 6, remove records that are in "prospects" //2573 records decreased to 952 records
def keep_non_contacted(manu_record: String): Boolean = {
    val array1 = manu_record.split(",")
    if (array1.length >=5) {
        val website = array1(4)
        if (website contains ".com")
          return true
    }
    return false
}

val updated_manu =  manu.filter(line => keep_non_contacted(line)).collect

// step 7, write to a file
import java.io._
val file = new File("/Users/jenny/Downloads/updated_manufacturing.csv")
val bw = new BufferedWriter(new FileWriter(file))
for (c <- updated_manu) {
    bw.write(c + "\n")
}
println("wrote %s records", updated_manu.length) 
bw.close()
