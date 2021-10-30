

import org.apache.spark.sql.SparkSession



object WordCount {

	def main(args: Array[String]): Unit = {

		val sc = SparkSession.builder().master("spark://pierre:30160").getOrCreate().sparkContext
		val titles = sc.textFile(args(0),10).zipWithIndex().mapValues(x=>(x+1).toString).map(_.swap).persist()

		val Surfing = titles.filter(t=>(t._2.toLowerCase.contains("surfing"))).persist()

		val lines = sc.textFile(args(1),10)
		val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1))).persist()

		val RockyMtn = titles.filter(t=>(t._2.toLowerCase().equals("rocky_mountain_national_park"))).persist()
		val index = RockyMtn.keys.first()

		val WebGraph = links.join(Surfing).mapValues(v=>v._1 + " " + index).persist()

		val Web = WebGraph.union(RockyMtn).persist()
		val count = Web.count()

		var ranks = Web.mapValues(k => (1.0f / count))

		for(i<-1 to 25) {

		 val tempRank = Web.join(ranks,10).values.flatMap{

		   case(urls,rank) => (urls.split(" ").map(url=>(url, rank/urls.split(" ").count(z=>true).toFloat)))

		 }

		 ranks = tempRank.reduceByKey(_+_)

		}
		val RocknSurf = Surfing.union(RockyMtn).persist()
		val out = RocknSurf.join(ranks).map{case(k,v) => (v._1,v._2)}.persist()
		val put = out.top(10)(Ordering[Float].on(_._2))
		sc.parallelize(put).saveAsTextFile(args(2))

	}
}
