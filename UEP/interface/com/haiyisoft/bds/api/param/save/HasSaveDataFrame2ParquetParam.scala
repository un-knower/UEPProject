package com.haiyisoft.bds.api.param.save

import java.util.logging.Logger

import org.apache.spark.sql.DataFrame

import scala.util.Try

/** 该特性实现了将DataFrame保存为parquet的方法
  * Created by XingxueDU on 2018/3/16.
  */
trait HasSaveDataFrame2ParquetParam extends HasSaveDataFrameParam{

  override def save(stored: DataFrame): Unit = {
    if(saver!=null){
      val saverSupport = Try{saver.save(stored)}
      if(saverSupport.isFailure){
        Try{Logger.getLogger("com.haiyisoft.bds").log(java.util.logging.Level.WARNING,"Save Failure",saverSupport.failed.get)}
      }
    }
    stored.write.mode(getSaveMode).parquet(getSavePath)
  }
}
