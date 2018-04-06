import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD
import com.mapr.db.MapRDB
import com.mapr.db.Table
import com.mapr.db.spark._
import com.mapr.db.spark.impl.OJAIDocument

object FlightAnalysis {

  case class CancelledFlights(Year: String,
                              Month: String,
                              DayofMonth: String,
                              FlightNum: String,
                              Origin: String,
                              Dest: String,
                              Cancelled: String,
                              CancellationCode: String)
      extends Serializable

  case class CancelledFlightsWithReason(Year: String,
                                        Month: String,
                                        DayofMonth: String,
                                        FlightNum: String,
                                        Origin: String,
                                        Dest: String,
                                        CancellationReason: String)
      extends Serializable

  case class FlightJsonDocument(_id: String,
                                FlightNum: String,
                                Origin: String,
                                Dest: String,
                                CancellationReason: String)
      extends Serializable

  def createJsonDocument(r: CancelledFlightsWithReason): FlightJsonDocument = {
    val _id = r.Year + "_" + r.Month + "_" + r.DayofMonth
    FlightJsonDocument(_id, r.FlightNum, r.Origin, r.Dest, r.CancellationReason)
  }

  def main(args: Array[String]) {

    val csvfile = "/tmp/flightdata/2008.csv"
    var csvDF = spark.read.option("header", "true").csv(csvfile)
    csvDF.createOrReplaceTempView("flightdata")
    val cancelledFlightsDS: Dataset[CancelledFlights] = spark
      .sql(
        "select Year,Month,DayofMonth,FlightNum,Origin,Dest,Cancelled,CancellationCode from flightdata where Cancelled='1'")
      .as[CancelledFlights]

    val cancelledFlightsWithReasonDS = cancelledFlightsDS.withColumn(
      "CancellationReason",
      lookup(cancelledFlightsDS("CancellationCode")))
    spark
      .sql(
        "select Year,Month,DayofMonth,FlightNum,Origin,Dest,Cancelled,CancellationCode from flightdata where Cancelled='1' limit 2")
      .show

    val lookup = udf[String, String](cancellationCode =>
      cancellationCode match {
        case "A" => "carrier"
        case "B" => "weather"
        case "C" => "NAS"
        case "D" => "security"
        case _   => "unknown"
    })

    val cancelledFlightsWithReasonDS = cancelledFlightsDS.withColumn(
      "CancellationReason",
      lookup(cancelledFlightsDS("CancellationCode")))
    cancelledFlightsWithReasonDS.createOrReplaceTempView("cancelledflightdata")

    val cancelledFlightsWithReasonDS2: Dataset[CancelledFlightsWithReason] =
      spark
        .sql(
          "select Year,Month,DayofMonth,FlightNum,Origin,Dest,CancellationReason from cancelledflightdata")
        .as[CancelledFlightsWithReason]

    val ojaiRDD = cancelledFlightsWithReasonDS2
      .map(createJsonDocument)
      .toJSON
      .rdd
      .map(MapRDBSpark.newDocument)

    ojaiRDD.saveToMapRDB("/tmp/flighttable",
                         createTable = true,
                         idFieldPath = "_id")

  }
}
