package com.emiasd.flight.config

sealed trait Environment
object Environment {
  case object Local  extends Environment
  case object Hadoop extends Environment

  /**
   * Convertit une chaîne en instance d’environnement (insensible à la casse).
   */
  def fromString(s: String): Environment = s.trim.toLowerCase match {
    case "local"  => Local
    case "hadoop" => Hadoop
    case other =>
      throw new IllegalArgumentException(
        s"Environnement inconnu: '$other'. Valeurs autorisées: local, hadoop"
      )
  }
}
