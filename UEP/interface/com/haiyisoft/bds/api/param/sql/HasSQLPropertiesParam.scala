package com.haiyisoft.bds.api.param.sql

import java.util.Properties

import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.conf.{HasPasswordParam, HasUserNameParam}


/**
  * Created by XingxueDU on 2018/3/26.
  */
trait HasSQLPropertiesParam extends HasUserNameParam with HasPasswordParam {

  final val SQL_DRIVER_KEY = "sql.driver.class.name"
  final val SQL_URL_KEY = "sql.url"

  def setDriver(driver: String): this.type = {
    addParam(SQL_DRIVER_KEY, driver)
    this
  }

  def getDriver: String = {
    $(SQL_DRIVER_KEY)
  }

  def driverFromUser: ParamFromUser = ParamFromUser
    .String(SQL_DRIVER_KEY, "请输入SQL.Driver", notEmpty = true)
    .setTypeName("sql.driver.class.name")
    .setShortName("数据库驱动")

  def setURL(url: String): this.type = {
    addParam(SQL_URL_KEY, url)
  }

  def getURL: String = {
    $(SQL_URL_KEY)
  }

  def getProperties: java.util.Properties = {
    val res = new Properties()
    res.setProperty("user", getUserName)
    res.setProperty("password", getPassword)
    res.setProperty("driver", getDriver)
    res
  }

  def urlFromUser: ParamFromUser = ParamFromUser
    .String(SQL_URL_KEY, "请输入SQL.Url", true)
    .setShortName("数据路路径")

  override def addParam(key: String, value: Any): this.type = {
    if (key != null && key == SQL_DRIVER_KEY)
      Class.forName(value.toString)
    super.addParam(key, value)
  }

}
