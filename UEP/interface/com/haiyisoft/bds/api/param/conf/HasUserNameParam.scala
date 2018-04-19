package com.haiyisoft.bds.api.param.conf

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

/**
  * Created by XingxueDU on 2018/3/26.
  */
trait HasUserNameParam extends WithParam {
  final val USER_NAME_KEY = "user.name"

  def setUserName(pw: String): this.type = {
    addParam(USER_NAME_KEY, pw)
    this
  }

  def getUserName: String = {
    $(USER_NAME_KEY)
  }

  def userNameFromUser: ParamFromUser = ParamFromUser
    .String(USER_NAME_KEY, "请输入用户名", notEmpty = false)
    .setTypeName("username")
    .setShortName("用户名")

}
