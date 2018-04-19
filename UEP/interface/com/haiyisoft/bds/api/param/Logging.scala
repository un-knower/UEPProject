package com.haiyisoft.bds.api.param

import java.io.PrintStream
import java.text.SimpleDateFormat

import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by XingxueDU on 2018/2/8.
  *
  * @author DU Xingxue
  * @version 8.2.8
  */
private[bds] trait Logging {
  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var log_ : Logger = null
  def setLevel(level:Int)={
    val levelapache = level match{
      case Logging.DEBUG => org.apache.log4j.Level.DEBUG
      case Logging.INFO => org.apache.log4j.Level.INFO
      case Logging.WARN => org.apache.log4j.Level.WARN
      case Logging.ERROR => org.apache.log4j.Level.ERROR
    }
    org.apache.log4j.Logger.getLogger(this.getClass).setLevel(levelapache)
    Logging.level = level
  }
  def setErrorStream(stream:PrintStream) = Logging.errStream=stream
  def setInfoStream(stream:PrintStream) = Logging.outStream=stream

  // Method to get or create the logger for this object
  protected def log: Logger = {
    if (log_ == null) {
      log_ = LoggerFactory.getLogger(this.getClass)
    }
    log_
  }
  private val timeFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss")
  private val classname = s"${this.getClass.getName.replaceAll("com\\.lduxx","du").replaceAll("du\\.util","dut")}"
  private def logLocal(tp:String,msg: => String,level:Int): Unit ={
    if(level>=Logging.level){
      val date = new java.util.Date()
      val msga = s"${timeFormat.format(date)} $tp $classname:$msg"
      Logging.outStream.println(msga)
    }
  }
  private def logLocalError(tp:String,msg: => String,level:Int): Unit ={
    if(level>=Logging.level){
      val date = new java.util.Date()
      val msga = s"${timeFormat.format(date)} $tp $classname:$msg"
      Logging.errStream.println(msga)
    }
  }
  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
    else logLocal("INFO",msg,Logging.INFO)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
    else logLocal("DEBUG",msg,Logging.DEBUG)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
    else logLocalError("TRACE",msg,Logging.WARN)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
    else logLocalError("WARN",msg,Logging.WARN)

  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
    else logLocalError("ERROR",msg,Logging.ERROR)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
    else {
      logLocal("INFO",msg,Logging.INFO)
      throwable.printStackTrace(Logging.outStream)
    }
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
    else {
      logLocal("DEBUG",msg,Logging.DEBUG)
      throwable.printStackTrace(Logging.outStream)
    }
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
    else {
      logLocalError("TRACE",msg,Logging.WARN)
      throwable.printStackTrace(Logging.errStream)
    }
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
    else {
      logLocalError("WARN",msg,Logging.WARN)
      throwable.printStackTrace(Logging.errStream)
    }
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
    else {
      logLocalError("ERROR",msg,Logging.ERROR)
      throwable.printStackTrace(Logging.errStream)
    }
  }

  protected def isTraceEnabled: Boolean = log.isTraceEnabled
}
object Logging{
  val DEBUG = 0
  val INFO = 1
  val WARN = 2
  val ERROR = 3
  private var level = 1
  private var errStream = System.err
  private var outStream = System.err
  def setLevel(level:Int)={
    val levelapache = level match{
      case Logging.DEBUG => org.apache.log4j.Level.DEBUG
      case Logging.INFO => org.apache.log4j.Level.INFO
      case Logging.WARN => org.apache.log4j.Level.WARN
      case Logging.ERROR => org.apache.log4j.Level.ERROR
    }
    val path = "/org/apache/spark/log4j-defaults.properties"
    val url = Thread.currentThread().getClass.getResource(path)
    if(url!=null){
      System.err.println(s"Using Spark's default log4j profile: $path")
      org.apache.log4j.PropertyConfigurator.configure(url)
    }
    val logger = org.apache.log4j.Logger.getLogger("com.lduxx")
    logger.setLevel(levelapache)
    Logging.level = level
  }
  def setErrorStream(stream:PrintStream) = Logging.errStream=stream
  def setInfoStream(stream:PrintStream) = Logging.outStream=stream
}