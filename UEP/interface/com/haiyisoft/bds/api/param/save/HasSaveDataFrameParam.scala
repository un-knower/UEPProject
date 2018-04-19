package com.haiyisoft.bds.api.param.save

import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

/**
  * Created by XingxueDU on 2018/3/17.
  */
trait HasSaveDataFrameParam extends HasSaveParamFor[DataFrame] {
  protected var saver: HasSaveParamFor[DataFrame] = null

  override def save(stored: DataFrame): Unit = {
    if (saver != null) {
      if (this.getSaveMode != null) {
        saver.setSaveMode(getSaveMode)
      }
      if (this.getSavePath != null) {
        saver.setSavePath(getSavePath)
      }
      saver.save(stored)
    }
    else if (getSavePath.contains("hdfs")) {
      HasSaveDataFrameParam.toParquet(getSaveMode, getSavePath).save(stored)
    }
    else {
      throw new NoSuchMethodException("Need to set a DataFrame save method")
    }
  }

  def setSaver(md: HasSaveParamFor[DataFrame]): this.type = {
    this.saver = md
    this
  }

  override protected def getDefaultParamList: ArrayBuffer[ParamFromUser] = {
    val old = super.getDefaultParamList
    old += ParamFromUser.save.mode().setShortName("保存方式")
    old += ParamFromUser.save.savePath().setShortName("保存路径")
    old
  }
}

object HasSaveDataFrameParam {

  def toParquet(mode: String, path: String) = {
    new SaveToParquet(mode, path)
  }
}

class SaveToParquet(mode: String, path: String) extends HasSaveDataFrame2ParquetParam {
  setSaveMode(mode)
  setSavePath(path)

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = throw new NoSuchMethodException()

  override def getPluginName: String = "Undefined Plugin's Name"
}