package example

import com.haiyisoft.bds.api.data.one2one.DataProcessInter
import org.apache.spark.sql.SparkSession

/**
  * Created by XingxueDU on 2018/3/15.
  */
object Plugin {

  def getClassByName(name:String): DataProcessInter ={
    val cls = Class.forName(name)
    println(cls)
    println("Pass")
//    cls.newInstance().asInstanceOf[DataMining]
    cls.getConstructor().newInstance().asInstanceOf[DataProcessInter]
  }

  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark2Test")
      .config("spark.sql.warehouse.dir","file:///F:/Travail/workspace").
      config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
      getOrCreate()
//    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    import spark.implicits._


    val df = spark.sparkContext.parallelize(List(Student("jack",12,3),
      Student("tom",11,1),Student("jet",14,6),Student("jerry",23,12))).toDF()


    df.show()

    val addudf = getClassByName("bin.AddUdf")
    println(addudf)
    addudf.asInstanceOf[Udf2DataTransformInter]
      .setInputCols("age","grade")
      .setOutputCol("features")
    addudf.transform(df).show
  }
}
//  def getClassByName2(name:String): DataMining ={
//    val cls = Class.forName(name)
//    if(cls == classOf[DataMining]){
//      val constructor = cls.getConstructor(classOf[Integer],classOf[String])
//      constructor.newInstance(1,"a").asInstanceOf[DataMining]
//    }else null
//  }
case class Student(name: String, age: Int, grade: Int)
