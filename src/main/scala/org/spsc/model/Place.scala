package org.spsc.model

case class PlaceObject(place_type: String,
                       id: String,
                       country_code: String,
                       country: String,
                       geo: GeoEntity,
                       name: String,
                       full_name: String)

case class GeoEntity(`type`: String,
                     bbox: Array[Float])