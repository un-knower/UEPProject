package example


import com.haiyisoft.bds.api.data.DataTransformInter
import com.haiyisoft.bds.api.data.one2one.Udf2DataMining
import org.apache.spark.sql.SparkSession

/**
  * Created by XingxueDU on 2018/3/15.
  */
object Main {

  def getClassByName(name:String): DataTransformInter ={
    val cls = Class.forName(name)
    println(cls)
    println("Pass")
//    cls.newInstance().asInstanceOf[DataTransformInter]
    cls.getConstructor().newInstance().asInstanceOf[DataTransformInter]
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
    addudf.asInstanceOf[Udf2DataMining]
      .setInputCols("age","grade")
      .setOutputCol("features")
    addudf.transform(df).show

    new Process().run()
  }
}
//  def getClassByName2(name:String): DataTransformInter ={
//    val cls = Class.forName(name)
//    if(cls == classOf[DataTransformInter]){
//      val constructor = cls.getConstructor(classOf[Integer],classOf[String])
//      constructor.newInstance(1,"a").asInstanceOf[DataTransformInter]
//    }else null
//  }
case class Student(name: String, age: Int, grade: Int)
