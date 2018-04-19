package com.haiyisoft.bds.api.param.sql

import java.sql.ResultSet

/**
  * Created by XingxueDU on 2018/3/26.
  */
trait SelectSQLCodeParam extends HasSQLCodeParam{
  protected final val SQL_MODE = "GET"
  def getConnection: java.sql.Connection
  def commit:Array[ResultSet]={
    val connection = getConnection
    val sql = getSQLCodes
    val res = sql.map{code=>
      val runner =connection.prepareStatement(code)
      runner.executeQuery()
    }
    connection.close()
    res
  }
}
