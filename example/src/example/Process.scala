package example

import com.haiyisoft.bds.api.action.Action
import com.haiyisoft.bds.api.data.DataTransformInter
import com.haiyisoft.bds.api.data.one2one.Udf2DataMining
import com.haiyisoft.bds.api.saveMethod.{DataFrameSaveToParquet, DataFrameSaver}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.udf

/**
  * Created by XingxueDU on 2018/3/15.
  */
class Process {



  def run(): Unit ={

    //数据获取流程
    val dataFrom = new DataFromParquet()
    dataFrom.setLoadPath("hdfs://usr/data/mydata")


    //变换流程
    val dmList = new ArrayBuffer[DataTransformInter]()


    val udf1 = udf{(a:Double,b:Double)=>org.apache.spark.ml.linalg.Vectors.dense(a,b)}
    val udfDm = Udf2DataMining(udf1,"features","x1","x2")

    dmList+=udfDm

    //数据处理
    val data = dataFrom.getData
    val df1 = (data/:dmList){(df,dm)=>dm.transform(df)}

    /////////////////////////
    //////model训练例
    //////////////////////
    val action:Action[_] = new LR()

    action.setSaveMode("overwrite")
    action.setSavePath("hdfs://usr/data/myModel")
    action.run(df1)

    ///////////////
    ////model使用例
    ///////////////

    val action2 = new LRModel()
    val saver = DataFrameSaver.toParquet("overwrite","hdfs://usr/data/myResult")
    action2.setSaver(saver)
    action2.run(df1)

    ///////////////
    ////model使用例2
    ///////////////

    val action3 = new LRModel()with DataFrameSaveToParquet
    action3.setSaveMode("overwrite")
    action3.setSavePath("hdfs://usr/data/myResult2")
    action3.run(df1)

  }
}
