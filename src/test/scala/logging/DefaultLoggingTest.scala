package dev.fb.dbzpark
package logging

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike

import java.io.PrintWriter

class DefaultLoggingTest extends AnyFunSuiteLike with BeforeAndAfterAll {
  val filePath = "/tmp/db_zpark_logs.log"

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("DefaultLoggingTest")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  test("default logs should read and extract app_name from custom_fields") {
    saveLogs()

    val df = DefaultLogging.readLogs(spark, filePath)

    // Verify the DataFrame was created
    assert(df != null)

    // Verify we have 5 rows
    assert(df.count() == 5)

    // Verify app_name column exists
    assert(df.columns.contains("app_name"))

    // Verify all rows have app_name = "my_app"
    val appNames = df.select("app_name").distinct().collect()
    assert(appNames.length == 1)
    assert(appNames(0).getString(0) == "my_app")

    // Verify some specific fields
    val firstRow = df.filter("level = 'INFO'").first()
    assert(firstRow.getAs[String]("message").contains("Application started successfully"))

    // Show the DataFrame for debugging
    df.show(false)
  }

  private def saveLogs(): Unit = {
    val logEntries = Seq(
      """{"timestamp":"2024-12-15T10:30:45+0000","level":"INFO","thread":"zio-fiber-123","message":"Application started successfully","cause":"","logger_name":"com.example.Application","spans":"","custom_fields":{"app_name":"my_app","userId":"user123","requestId":"req-456"}}""",
      """{"timestamp":"2024-12-15T10:31:12+0000","level":"DEBUG","thread":"zio-fiber-124","message":"Processing user request","cause":"","logger_name":"com.example.RequestHandler","spans":"span1,span2","custom_fields":{"app_name":"my_app","userId":"user456","endpoint":"/api/data"}}""",
      """{"timestamp":"2024-12-15T10:31:45+0000","level":"WARN","thread":"zio-fiber-125","message":"Database connection slow","cause":"","logger_name":"com.example.Database","spans":"db-query","custom_fields":{"app_name":"my_app","duration":"5000ms","query":"SELECT"}}""",
      """{"timestamp":"2024-12-15T10:32:03+0000","level":"ERROR","thread":"zio-fiber-126","message":"Failed to process transaction","cause":"java.sql.SQLException: Connection timeout","logger_name":"com.example.TransactionService","spans":"transaction-processing","custom_fields":{"app_name":"my_app","transactionId":"tx-789","amount":"100.00"}}""",
      """{"timestamp":"2024-12-15T10:32:30+0000","level":"INFO","thread":"zio-fiber-127","message":"Request completed","cause":"","logger_name":"com.example.RequestHandler","spans":"","custom_fields":{"app_name":"my_app","userId":"user456","statusCode":"200","duration":"2500ms"}}"""
    )

    val writer = new PrintWriter(filePath)
    try
      logEntries.foreach(entry => writer.println(entry))
    finally
      writer.close()
  }
}
