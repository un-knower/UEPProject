package com.haiyisoft.bds.api.param.conf

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

/**
  * Created by XingxueDU on 2018/3/26.
  */
trait HasPasswordParam extends WithParam {
  val PASSWORD_KEY = "user.password"

  def setPassword(pw: String): this.type = {
    addParam(PASSWORD_KEY, pw)
    this
  }

  def getPassword: String = {
    $(PASSWORD_KEY)
  }

  def passwordFromUser: ParamFromUser = ParamFromUser
    .String(PASSWORD_KEY, "请输入密码", notEmpty = false)
    .setTypeName("password")
    .setShortName("密码")
}
