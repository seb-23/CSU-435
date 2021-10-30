
import org.apache.spark.sql.SparkSession


object Taxation {

	def main(args: Array[String]): Unit = {

		val sc = SparkSession.builder().master("spark://pierre:30160").getOrCreate().sparkContext
		val titles = sc.textFile(args(0), 10).zipWithIndex().mapValues(x=>(x+1).toString).map(_.swap).persist()

		val lines = sc.textFile(args(1), 10)
		val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1))).persist()

		val count = titles.count()

		var ranks = links.mapValues(k=>(1.0f/count))

		val constant = links.mapValues(k=>(0.15f * 1.0f / count)).persist()

		for(i<-1 to 25) {


		 val tempRank = links.join(ranks,10).values.flatMap{

		   case(urls,rank) => (urls.split(" ").map(url=>(url, 0.85f*rank/urls.split(" ").count(z=>true).toFloat)))

		 }

		 val r = tempRank.reduceByKey(_+_)
		 ranks = r.join(constant).mapValues(f=>(f._1+f._2))
		}

		val out = titles.join(ranks).map{case(k,v) => (v._1,v._2)}.persist()
		val put = out.top(10)(Ordering[Float].on(_._2))
		sc.parallelize(put).saveAsTextFile(args(2))

	}
}
