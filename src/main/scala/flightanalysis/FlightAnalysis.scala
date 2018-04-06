// Import necessary packages
import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD
import com.mapr.db.MapRDB
import com.mapr.db.Table
import com.mapr.db.spark._
import com.mapr.db.spark.impl.OJAIDocument

object FlightAnalysis {

  // Case Class containing the Year, Month, DayofMonth, Scheduled departure time(CRSDepTime), 
  // FlightNumber, Origin, Destination, Cancellation status and Cancellation code.
  case class CancelledFlights(Year: String,
                              Month: String,
                              DayofMonth: String,
                              CRSDepTime: String,
                              FlightNum: String,
                              Origin: String,
                              Dest: String,
                              Cancelled: String,
                              CancellationCode: String)
      extends Serializable

  // Case Class containing the Year, Month, DayofMonth, Scheduled departure time(CRSDepTime), 
  // FlightNumber, Origin, Destination and Cancellation reason.
  case class CancelledFlightsWithReason(Year: String,
                                        Month: String,
                                        DayofMonth: String,
                                        CRSDepTime: String,
                                        FlightNum: String,
                                        Origin: String,
                                        Dest: String,
                                        CancellationReason: String)
      extends Serializable

  // Case class defining the MapRDB-JSON document. 
  case class FlightJsonDocument(_id: String,
                                FlightNum: String,
                                Origin: String,
                                Dest: String,
                                CancellationReason: String)
      extends Serializable

  // Function creating the _id for MapRDB JSON
  def createJsonDocument(r: CancelledFlightsWithReason): FlightJsonDocument = {
    val _id = r.Year + "_" + r.Month + "_" + r.DayofMonth + "#" + r.FlightNum + "#" + r.CRSDepTime
    FlightJsonDocument(_id, r.FlightNum, r.Origin, r.Dest, r.CancellationReason)
  }

  def main(args: Array[String]) {

    // Reading the input data
    val csvfile = "/tmp/flightdata/2008.csv"
    var csvDF = spark.read.option("header", "true").csv(csvfile)
    
    // Creating a temporary view to query only necessary fields.
    csvDF.createOrReplaceTempView("flightdata")
    
    // Filtering the necessary information.
    // We need only subset of fields from the available 27 fields.
    // Also, we need information only for the cancelled flights.
    // Cancelled flights will have 'Cancelled=1'.
    val cancelledFlightsDS: Dataset[CancelledFlights] = spark
      .sql(
        "select Year,Month,DayofMonth,CRSDepTime,FlightNum,Origin,Dest,Cancelled,CancellationCode from flightdata where Cancelled='1'")
      .as[CancelledFlights]

    // Converting the Cancellation code to more meaningful reason.
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
          "select Year,Month,DayofMonth,CRSDepTime,FlightNum,Origin,Dest,CancellationReason from cancelledflightdata")
        .as[CancelledFlightsWithReason]

    // Creating OJAI MapRDB-JSON RDD
    val ojaiRDD = cancelledFlightsWithReasonDS2
      .map(createJsonDocument)
      .toJSON
      .rdd
      .map(MapRDBSpark.newDocument)

    // Storing the data to MapRDB-JSON table.
    ojaiRDD.saveToMapRDB("/tmp/flighttable",
                         createTable = true,
                         idFieldPath = "_id")

  }
}
