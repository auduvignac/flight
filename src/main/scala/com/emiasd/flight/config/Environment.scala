package com.emiasd.flight.config

sealed trait Environment
object Environment {
  case object Local  extends Environment
  case object Hadoop extends Environment
}
