package cn.yongjie.moiverecom.model

import org.apache.spark.mllib.recommendation.Rating

case class MyRating (rating: Rating, timestamp: Long)
