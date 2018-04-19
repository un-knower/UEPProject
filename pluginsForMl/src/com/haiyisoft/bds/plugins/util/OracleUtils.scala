package com.haiyisoft.bds.plugins.util

import java.sql.{Statement, Connection, SQLException}
import java.util.Properties

import com.haiyisoft.bds.api.param.Logging
import org.apache.http.ConnectionClosedException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Random, Try}

/**
  * Created by DU Xingxue on 2017/9/29.
  *
  * @author DU Xingxue
  * @version 7.9.29
  */
class OracleUtils(var url:String = "jdbc:oracle:thin:@172.20.36.46:1521:hydb",
                  var user: String,
                  var password: String,
                  var driver:String = "oracle.jdbc.driver.OracleDriver")extends Serializable with Logging{
  private val id = Random.nextInt(10000)
  private def logInfo(msg:String)= {
    val log = s"[${new java.util.Date().toString.substring(11,19)}][${java.net.InetAddress.getLocalHost.getHostAddress}:$id] $msg"
    super.logInfo(log)
  }
  private def logError(msg:String)= {
    val log = s"[${new java.util.Date().toString.substring(11,19)}][${java.net.InetAddress.getLocalHost.getHostAddress}:$id] $msg"
    super.logError(log)
  }

  private val pool = {
    val res = new org.apache.commons.dbcp.BasicDataSource()
    //    Class.forName(driver); //加载驱动程序
    res.setUrl(url)
    res.setUsername(user)
    res.setPassword(password)
    res.setDriverClassName(driver)
    var trycount = 0d
    while(trycount < 3){
      val s = Try{res.getConnection.close()}
      if(s.isFailure){
        logError(s"check connection failure retry[${2-trycount}]")
        if(trycount == 2){
          throw new SQLException(s.failed.get)
        }else trycount +=1
      }else trycount = 3
    }
    res
  }
  private var localConnect: java.sql.Connection = null

  def setParam(param:OracleUtils.DefaultParam)={
    this.url = param.url
    this.user = param.user
    this.password = param.password
    this.driver = param.driver

    this.pool.setUrl(url)
    this.pool.setPassword(password)
    this.pool.setDriverClassName(driver)

    if(this.isConnected())connect("reconnect")
  }
  def connect(actionWhenConnected:String="skip"):Boolean={
    if(isConnected()){
      actionWhenConnected.toLowerCase() match{
        case "skip" =>
          true
        case "nothing" =>
          true
        case "reconnect"=>
          disconnect()
          connect()
        case "disconnect"=>
          disconnect()
          connect()
        case "close"=>
          disconnect()
          connect()
      }
    }else{
      try{
        localConnect = getConnection
        true
      }catch {
        case ex: ClassNotFoundException =>
          logError("Driver not found")
          false
        case ex: SQLException =>
          logError("Connect Failure[SQL]:" + ex)
          false
      }
    }
  }
  def disconnect()={
    if(localConnect!=null){
      if(!localConnect.isClosed)localConnect.close()
      localConnect = null
    }
  }
  def isConnected(timeOut:Int = -1)=localConnect!=null&& !localConnect.isClosed &&(timeOut<=0||localConnect.isValid(timeOut))
  def getConnection:Connection= {
    if(pool.isClosed)throw new ConnectionClosedException("get connection from pool closed")
    var trycount = 0d
    var res:Connection = null
    while(trycount < 3){
      val s = Try{pool.getConnection}
      if(s.isFailure){
        logError(s"Failure to get connection retry[${2-trycount}]")
        if(trycount == 2){
          throw new SQLException(s.failed.get)
        }else trycount +=1
      }else {
        res = s.get
        trycount = 3
      }
    }
    res
  }
  def saveMsgIntoSQLOnce(cond:(String,String), MSG:String, insertBefore:Boolean, table:String, values:(String,String)*):Boolean={
    val res = saveMsgIntoSQL(cond,MSG,insertBefore,table,values:_*)
    disconnect()
    res
  }
  /**
    * [insert and] update table set values._1 = values._2 where cond._1 = cond._2
    * 处理了语句过长问题
    *
    * @param MSG 仅用于输出log，无实际意义
    */
  def saveMsgIntoSQL(cond:(String,String), MSG:String, insertBefore:Boolean, table:String, values:(String,String)*):Boolean ={
    logInfo("update "+MSG+" information...")
    if(!isConnected())connect()
    if(localConnect.isReadOnly){
      logInfo("Driver read only")
      false
    }else{
      try {
        if(insertBefore){
          val runner = localConnect.prepareStatement(s"INSERT INTO $table(${cond._1}) VALUES('${cond._2}')")
          runner.executeUpdate()
        }
        for(x<-values){
          val SQL = "UPDATE "+table+" SET "+x._1+" = '"+x._2+"' WHERE " + cond._1+" = '" + cond._2+"'"
          logInfo("execute SQL = " + SQL.replaceAll("}", "}\n"))
          if(x._2.length<3500){
            val SQL = "UPDATE "+table+" SET "+x._1+" = '"+x._2+"' WHERE " + cond._1+" = '" + cond._2+"'"
            print("saving...")
            val runner = localConnect.prepareStatement(SQL)
            runner.executeUpdate()
          }
          else{
            val splitedSql = TextUtils.splitStringByLength(x._2,3500)

            def getSql(input:String)="UPDATE "+table+" SET "+x._1+" = "+x._1+"||'"+input+"' WHERE " + cond._1+" = '" + cond._2+"'"

            {
              val runner = localConnect.prepareStatement("UPDATE "+table+" SET "+x._1+" = '' WHERE " + cond._1+" = '" + cond._2 + "'")
              runner.executeUpdate()
            }

            for(x<-splitedSql){
              val runner = localConnect.prepareStatement(getSql(x))
              runner.executeUpdate()
            }
          }
        }
        logInfo("SUCCESS")
        true
      } catch {
        case ex: SQLException =>
          logError("Save failure[SQL]:" + ex)
          false
      }
    }
  }
  def InsertRows(table:String, rows:Iterator[Row], schema:StructType, batchSize:Int, threadsNum:Int, msg:String=null)={
    assert(batchSize >= 1,
      s"Invalid value `${batchSize.toString}` for parameter " +
        s"`batchSize`. The minimum value is 1.")
    assert(table!=null&&table.nonEmpty)
    val logmsg = if(msg==null||msg.isEmpty)table else msg
    logInfo("insert "+logmsg+" information...")
    //    var res = Array[Int]()
    var committed = false
    var log2 = 5
    val connections = (0 until threadsNum).par.map{ idx=>
      val connect = {
        val connect = Try{pool.getConnection}
        if(connect.isFailure){
          logError("failure to get connection")
          throw new SQLException(connect.failed.get)
        }
        else connect.get
      }
      connect.setAutoCommit(false)
      (idx,connect)
    }
    try {
      val colNames:Array[String] = schema.fields.map(_.name)
      val colsCode = colNames.mkString(",")
      val placeholders = colNames.map(_=>"?").mkString(",")
      var sizeCache = 0
      val colLength = colNames.length
      val dataTypes = schema.fields.map(_.dataType)
      val nullTypes = dataTypes.map{
        case IntegerType => java.sql.Types.NUMERIC
        case LongType => java.sql.Types.NUMERIC
        case DoubleType => java.sql.Types.NUMERIC
        case FloatType => java.sql.Types.NUMERIC
        case ShortType => java.sql.Types.NUMERIC
        case ByteType => java.sql.Types.NUMERIC
        case BooleanType => java.sql.Types.BOOLEAN
        case StringType => java.sql.Types.VARCHAR
        case BinaryType => java.sql.Types.BINARY
        case TimestampType => java.sql.Types.TIMESTAMP
        case DateType => java.sql.Types.DATE
        case t: DecimalType => java.sql.Types.NUMERIC
        case _ =>java.sql.Types.VARCHAR
      }
      //        val hasArray = dataTypes.contains(ArrayType)
      val sql = s"""INSERT /*+ append */ INTO $table ($colsCode) VALUES ($placeholders)"""
      val stmtPool =connections.map{case (idx1,connec1)=>
        val stmt = connec1.prepareStatement(sql)
        (idx1,connec1,stmt)
      }
      var idx = 0
      var cache = mutable.MutableList[Row]()
      while(rows.hasNext){
        idx+=1
        val ttx = idx % threadsNum
        val row = rows.next()
        cache:+= row
        if(ttx==0){
          idx = 0
          sizeCache+=1
          stmtPool.foreach{case (idx1,_,stmt)=>
            val row = cache(idx)
            for(i<-0 until colLength){
              if (row.isNullAt(i)) {
                stmt.setNull(i + 1,nullTypes(i))
              } else {
                dataTypes(i) match {
                  case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                  case LongType => stmt.setLong(i + 1, row.getLong(i))
                  case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                  case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                  case ShortType => stmt.setInt(i + 1, row.getShort(i))
                  case ByteType => stmt.setInt(i + 1, row.getByte(i))
                  case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                  case StringType => stmt.setString(i + 1, row.getString(i))
                  case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                  case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                  case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                  case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
                  case x => throw new IllegalArgumentException(
                    s"Can't translate non-null value for field $i with $x")
                }
              }
            }
            stmt.addBatch()
          }
          cache.clear()
        }
        if(sizeCache>=batchSize){
          sizeCache = 0
          val start = new java.util.Date().getTime
          stmtPool.foreach(_._3.executeBatch())
          if(log2<5){
            logInfo(s"executeBatch batch use time [${new java.util.Date().getTime - start}]")
            log2 +=1
          }
          //            res ++= stmtPool.flatMap(_._3.executeBatch())
        }
      }
      if(sizeCache>0){
        stmtPool.flatMap(_._3.executeBatch())
        //          res ++= stmtPool.flatMap(_._3.executeBatch())
      }
      logInfo("begin to commit")
      @volatile
      var success = 1
      stmtPool.foreach{case(index,connection,stmt)=>
        stmt.close()
        connection.commit()
        logInfo(s"success [$success/$threadsNum]")
        success+=1
      }
      committed = true
      logInfo("ALL SUCCESS")
    } catch {
      case ex: SQLException =>
        logError("Save failure[SQL]:" + ex)
        if(ex.getMessage.contains("driver"))logError("driver = "+driver)
    } finally {
      if(!committed)connections.foreach(_._2.rollback())
      connections.foreach{case(_,connect1)=>
        if(Try{connect1.close()}.isFailure)logError("connect close failure but commit success")
      }
    }
    //      res
  }
  def clearTable(table:String)={
    var con: Connection =null
    var stmt: Statement = null
    try{
      con = pool.getConnection
      try{
        stmt = con.createStatement()
        val query = s"DELETE FROM $table"
        logInfo("[SQL]"+query)
        val deletedRows=stmt.executeUpdate(query)
        if(deletedRows>0){
          logInfo("Deleted All Rows In The Table Successfully...")
        }else{
          logInfo("Table already empty.")
        }
      } catch{
        case s:SQLException=>
          logError("Deleted All Rows In  Table Error. ")
          s.printStackTrace()
      }
      // close Connection
      con.commit()
      con.close()
    }catch{
      case e:Exception =>e.printStackTrace()
    }
  }
  override def equals(obj:Any):Boolean={
    val res = obj!=null&&obj.isInstanceOf[OracleUtils]&&{
      val o = obj.asInstanceOf[OracleUtils]
      o.url == this.url && o.user == this.user && o.password == this.password && o.driver == this.driver
    }
    res
  }
  def destroy()={
    if(OracleUtils.cache.equals(this))OracleUtils.cache = null
    disconnect()
    pool.close()
    localConnect = null
  }
}

object OracleUtils{
  lazy private val con = new OracleUtils(null,null,null,"oracle.jdbc.driver.OracleDriver")
  private[OracleUtils] var cache:OracleUtils=null
  private def getOrCreate(url:String,user:String,password:String,driver:String)={
    if(cache==null || cache.url!=url || cache.user!=user || cache.password!=password || cache.driver!=driver){
      val __new__  = new OracleUtils(url,user,password,driver)
      if(cache==null|| !cache.isConnected()){
        cache = __new__
      }
      __new__
    }else cache
  }
  case class DefaultParam(url:String,user:String,password:String,driver:String="oracle.jdbc.driver.OracleDriver")
  def apply(url:String,user:String,password:String,driver:String="oracle.jdbc.driver.OracleDriver"): OracleUtils ={
    getOrCreate(url,user,password,driver)
  }
  def apply(defaultParam: DefaultParam):OracleUtils=getOrCreate(defaultParam.url,defaultParam.user,defaultParam.password,defaultParam.driver)
  def apply(properties: Properties):OracleUtils=getOrCreate(properties.getProperty("url"),properties.getProperty("user"),properties.getProperty("password"),properties.getProperty("driver"))
  def saveMsgIntoSQLOnce(param:DefaultParam,key:(String,String),MSG:String,insertBefore:Boolean, table:String, values:(String,String)*)={
    con.setParam(param)
    con.saveMsgIntoSQLOnce(key,MSG,insertBefore,table,values:_*)
  }
}
