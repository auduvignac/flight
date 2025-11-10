package com.emiasd.flight.util

import org.slf4j.{Logger, LoggerFactory}

/** Mixin simple pour fournir un logger nommé automatiquement. */
trait Logging {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def info(msg: => String): Unit  = logger.info(msg)
  def warn(msg: => String): Unit  = logger.warn(msg)
  def error(msg: => String): Unit = logger.error(msg)
  def debug(msg: => String): Unit = logger.debug(msg)
}
