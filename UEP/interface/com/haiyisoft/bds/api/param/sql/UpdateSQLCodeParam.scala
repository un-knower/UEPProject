package com.haiyisoft.bds.api.param.sql

/**
  * Created by XingxueDU on 2018/3/26.
  */
trait UpdateSQLCodeParam extends HasSQLCodeParam{
  protected final val SQL_MODE = "MET"
  def getConnection: java.sql.Connection

  def commit():this.type={
    val connection = getConnection
    val sql = getSQLCodes
    sql.foreach{code=>
      val runner =connection.prepareStatement(code)
      runner.execute()
      runner.executeQuery()
    }
    connection.close()
    this
  }

}
