package sql

case class MovieRating(
                      userId:String,
                      itemId:String,
                      rating:Double,
                      timestamp:Long
                      )
