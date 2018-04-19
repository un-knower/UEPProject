package com.haiyisoft.bds.api.param.sql

import java.sql.SQLException

import scala.util.Try

/**
  * Created by XingxueDU on 2018/3/26.
  */
trait HasSQLPoolConnectionParam extends HasSQLPropertiesParam{
  private[bds] var pool: org.apache.commons.dbcp.BasicDataSource = _
  def connect():this.type ={
    if(pool!=null){
      if(!pool.isClosed)disconnect
    }
    pool = {
      val res = new org.apache.commons.dbcp.BasicDataSource()
      //    Class.forName(driver); //加载驱动程序
      res.setUrl(getURL)
      res.setUsername(getUserName)
      res.setPassword(getPassword)
      res.setDriverClassName(getDriver)
      var trycount = 0d
      while(trycount < 3){
        val s = Try{res.getConnection.close()}
        if(s.isFailure){
          if(trycount == 2){
            throw new SQLException(s.failed.get)
          }else trycount +=1
        }else trycount = 3
      }
      res
    }
    this
  }
  def isConnected:Boolean = {
    pool!=null && !pool.isClosed && Try{pool.getConnection.close()}.isSuccess
  }
  def disconnect:this.type ={
    if(pool!=null)pool.close()
    pool = null
    this
  }
  def getConnection: java.sql.Connection = {
    if(!isConnected)connect()
    pool.getConnection
  }
}
