package com.haiyisoft.bds.plugins.action

import com.haiyisoft.bds.api.action.Action
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/22.
  */
class ShowDFAction extends Action[DataFrame]{
  /**
    * 保存对象.注意：保存的是结果而不是自身
    *
    * @param stored 被保存对象（dataFrame或者Model）
    */
  override def save(stored: DataFrame): Unit = {
    print("ShowDFAction.save")
  }

  override def run(data:DataFrame):Unit={
    println("ShowDFAction.run")
    data.show()
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    Array()
  }

  override def getPluginName: String = "[DEBUG]打印表格"
}
