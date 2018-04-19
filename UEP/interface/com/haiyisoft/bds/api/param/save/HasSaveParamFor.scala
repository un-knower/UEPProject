package com.haiyisoft.bds.api.param.save

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

/** 继承此特性的步骤代表其结果可以被保存
  * 该特性的实现包括例如DataFrame to Parquet/to Oracle/to CSV等
  *
  * @tparam T 生成的结果类型，如ModelTraining的结果就是Model，而model的结果是DataFrame
  *
  * Created by XingxueDU on 2018/3/16.
  */
trait HasSaveParamFor[T] extends WithParam{
  val SAVE_MODE = ParamFromUser.variables.saveMode
  val SAVE_PATH = ParamFromUser.variables.savePath
  addParam(SAVE_MODE,"overwrite")
  addParam(SAVE_PATH,"DO NOT SAVE")

  /**
    * 保存对象.注意：保存的是结果而不是自身
    *
    * @param stored 被保存对象（dataFrame或者Model）
    */
  def save(stored:T):Unit

  /**
    * 设置保存方式，如覆盖，追加等
    * overwrite
 *
    * @param mode 保存方式
    */
  def setSaveMode(mode:String):this.type ={
    addParam(SAVE_MODE,mode)
  }

  /**
    * 获取保存方式
 *
    * @return 保存方式
    */
  def getSaveMode:String ={
    getStringParam(SAVE_MODE)
  }

  /**
    * 设置结果的保存路径
    *
    * @param path 保存路径
    */
  def setSavePath(path: String):this.type ={
    addParam(SAVE_PATH,path)
  }

  /**
    * 读取保存路径
    */
  def getSavePath:String ={
    getStringParam(SAVE_PATH)
  }

  def isWantSave:Boolean = {
    val s = getStringParam("SAVE_PATH")
    s!=null && s!="DO NOT SAVE"
  }
}
