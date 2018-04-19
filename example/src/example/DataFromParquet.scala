package example

import com.haiyisoft.bds.api._
import com.haiyisoft.bds.api.data.DataLoadInter
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by XingxueDU on 2018/3/15.
  */
class DataFromParquet extends DataLoadInter{

  /**
    * 获取默认参数
    *
    * @param key 参数名
    * @return 参数值
    */
  override def getDefaultParam(key: String): Any = null

  /**
    * DefaultParam中是否包含指定key
    *
    * @param key 带查询的参数名
    * @return true代表存在
    */
  override def containsDefaultParam(key: String): Boolean = false

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = Array()

  def setLoadPath(path:String): Unit ={
    this.addParam("loadPath",path)
  }

  override def getData: Dataset[Row] = {
    val spark = SparkSession.builder().getOrCreate()
    spark.read.parquet(getParam("loadPath").toString)
  }
}
