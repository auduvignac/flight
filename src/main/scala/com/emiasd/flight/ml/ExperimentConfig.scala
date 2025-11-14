package com.emiasd.flight.ml

/** 
  * Configuration d'une expérience de modélisation ML.
  * 
  * @param ds Dataset source (ex : "D1", "D2", "D3", "D4")
  * @param th Seuil de retard en minutes
  * @param originHours Fenêtre météo autour de l'aéroport de départ (heures)
  * @param destHours Fenêtre météo autour de l'aéroport d'arrivée (heures)
  * @param tag Nom lisible de l'expérience (utilisé pour logs et fichiers)
  */
final case class ExperimentConfig(
  ds: String,
  th: Int,
  originHours: Int,
  destHours: Int,
  tag: String
)