package com.haiyisoft.bds.plugins.loader

import com.haiyisoft.bds.api.param.ParamFromUser

/**
  * Created by XingxueDU on 2018/3/26.
  */
class DLSQLDataLoader extends OracleDataLoader {

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val user = userNameFromUser
    val password = passwordFromUser
    val driver = driverFromUser
    val sql = SQLCodeFromUser(true).setTypeName("dlsql")
    Array(user, password, driver, sql)
  }

  override def getPluginName: String = "向导读取Oracle"
}
