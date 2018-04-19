package com.haiyisoft.bds.plugins.loader

import com.haiyisoft.bds.api.data.DataLoadInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.sql.{HasSQLCodeParam, HasSQLPropertiesParam}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by XingxueDU on 2018/3/26.
  */
class OracleDataLoader extends DataLoadInter with HasSQLPropertiesParam with HasSQLCodeParam {
  override def getData: DataFrame = {
    SparkSession.builder().getOrCreate().read.jdbc(getURL, s"(${getSQLCodes.head})", getProperties)
  }


  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val user = userNameFromUser
    val password = passwordFromUser
    val driver = driverFromUser
    val url = urlFromUser
    val sql = SQLCodeFromUser(true)
    Array(user, password, driver, url, sql)
  }

  override def getPluginName: String = "从Oracle读取"
}
