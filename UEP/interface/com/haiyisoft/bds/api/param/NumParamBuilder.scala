package com.haiyisoft.bds.api.param

/**
  * 注意：在js中，t为Number(a)，a为输入的内容[[String]]
  * Created by XingxueDU on 2018/3/22.
  */
class NumParamBuilder(name:String, description:String=ParamFromUser.DEFAULT) extends ParamFromUser(name,description,null){
  var condParam = "!isNaN(t)"
  setTypeName("bigDecimal")
  override def checkJsFunction:String = "function check(a){var t = Number(a);return ("+condParam+");}check(\"#value#\")"
  def fix:ParamFromUser={
    new ParamFromUser(name,description,checkJsFunction)
      .setAllowMultipleResult(this.getAllowMultipleResult)
      .setTypeName(this.getTypeName)
      .addDefaultValue(getDefaultValue:_*)
  }

  def >(s:Any): this.type ={
    condParam+="&&t>"+s
    this
  }
  def <(s:Any):this.type ={
    condParam+="&&t<"+s
    this
  }
  def >=(s:Any): this.type ={
    condParam+="&&t>="+s
    this
  }
  def <=(s:Any): this.type ={
    condParam+="&&t<="+s
    this
  }
  def ===(s:Any): this.type ={
    condParam+="&&t=="+s
    this
  }
  def !==(s:Any): this.type ={
    condParam+="&&t!="+s
    this
  }
  def more(s:Any):this.type={
    condParam+="&&"+s
    this
  }
  def in(left:Any,right:Any):this.type={
    condParam+="&&t>="+left+"&&t<="+right
    this
  }

  def ||>(s:Any): this.type ={
    condParam+=")||(t>"+s
    this
  }
  def ||<(s:Any):this.type ={
    condParam+=")||(t<"+s
    this
  }
  def ||>=(s:Any): this.type ={
    condParam+=")||(t>="+s
    this
  }
  def ||<=(s:Any): this.type ={
    condParam+=")||(t<="+s
    this
  }
  def ||===(s:Any): this.type ={
    condParam+=")||(t=="+s
    this
  }
  def ||!==(s:Any): this.type ={
    condParam+=")||(t!="+s
    this
  }
  def orMore(s:Any):this.type={
    condParam+=")||("+s
    this
  }

}