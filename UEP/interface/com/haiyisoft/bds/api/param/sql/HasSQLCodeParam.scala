package com.haiyisoft.bds.api.param.sql

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

/**
  * Created by XingxueDU on 2018/3/26.
  */
trait HasSQLCodeParam extends WithParam {
  final val SQL_CODE_KEY = "sql.code"

  def getSQLCodes: Array[String] = getArrayStringParam(SQL_CODE_KEY)

  def addSQLCodes(code: String*): this.type = addParam(SQL_CODE_KEY,
    multiParam2Str(code: _*)
    //code.mkString(ParamFromUser.variables.separator.toString)
  )

  def SQLCodeFromUser(onlyOneCode: Boolean = true): ParamFromUser = ParamFromUser
    .String(SQL_CODE_KEY, "请输入SQL代码")
    .setAllowMultipleResult(!onlyOneCode)
    .setShortName("查询语句")
}
