import org.jsoup.Jsoup;

import java.io.File;
import scala.collection.JavaConverters._

// Import Row.
import org.apache.spark.sql.Row;

// Import Spark SQL data types
import org.apache.spark.sql.types.{ StructType, StructField, StringType };

/**
 * ScraperTool demos the Etosha-Tab-Scraper features.
 * 
 * Think about interactive data set discovery, using the internet. 
 * 
 * You travel to an URL, index it, inspect it, you identify TABLES or lists. 
 * 
 * Now you cut a table by its identifyer and append it as a new PARTITION 
 * to a HIVE table.
 * 
 * Go on, select more tables and bring them all in into Hadoop. Store all
 * data in Parquet files and index the context of that file.
 * 
 * Finally you can expose your METADATA and the content as an RDF-Graph.
 *   
 */
object ScraperTool {

 
  /**
   * Use Case:
   * 
   * We use Yahoo Financial services to get a list of components in
   * a stock market.
   * 
   * For this list we plan to load all time series for several years,
   * using the Yahoo-Finance API. The Stock-Data-Loader (SDL) is a convinient 
   * helper for this.
   * 
   * We have to extract a list of symbols from the initial list and to track 
   * METADATA. SDL gives us Time-Series-Buckets and stores them in Hadoop.
   * We track all relations in the METASTORE and can expose all tables in an
   * EXCEL-Sheet. But for cluster-side-processing we use DataFrames and RDDs
   * as a convinient scalable approach.
   * 
   */
 
  val selector = "table#table2"
 
  
  
  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    
    
    
    /*
     * The SCHEMA belongs to a particular TableSnippet.
     * 
     * A SchemaTransformer allows creation of SOLR-Schema, 
     * AVRO-Schema, and DataFrame Schema from this data-
     * 
     */
    // 
    // DEFINE a SCHEMA array 
    //
    val cols = Iterator("month", "lang.1", "lang.2", "lang.3", "lang.4", "lang.5", "regions", "participation.1", "participation.2", "participation.3", "usage", "content")
    
    val colsS = "month lang1 lang2 lang3 lang4 lang5 regions participation1 participation2 participation3 usage content"
    
    val z = cols.size.toInt

    val colsS = "month lang1 lang2 lang3 lang4 lang5 regions participation1 participation2 participation3 usage content"
    val z = cols.size.toInt

    // Generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    
    
    
    
    
    val fn = "stats.wikimedia.org_wikisitemap_html_table2"
    val tabName = "wikistats"
    
    
    
    
    // SCRAPE the SNIPPET 
    
        // define the ScrapabeleTableModel
        val stm = new ScrapeableTableModel() 
        val vec = TableScraper.scrapeTable( stm )
    
    // LOAD the SNIPPET FROM XLS
    
    // LOAD the SNIPPET FROM GOOGLE-DOCS
    

    println("# of short rows  =" + e)
    println("# of rows in vec =" + vec.size)

    //
    // LOCAL DATA GOES TO CLUSTER NOW
    //
    val wikistats = sc.parallelize(vec)


    // Convert records of the RDD ( wikistats ) to Rows.
    val rowRDD = wikistats.map(_.split(",")).map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11)))

    // Apply the schema to the RDD.
    val statsDF = sqlContext.createDataFrame(rowRDD, schema)


    // Register the DataFrames as a table.
    statsDF.registerTempTable(tabName)

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT lang4, lang5, usage FROM wikistats")
    results.cache()

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index or by field name.
    results.collect().foreach(println)

    statsDF.printSchema()

    // save results in HDFS...
    statsDF.saveAsParquetFile(fn)
    
    
    
    
    //
    // BACK to local ....
    //

    // GO ON NOW LOCALLY ...
    Vector<Rows> data = statsDF.collect();
  
    /**
     * 
     * Here we define the new TableNode to be handled in the Contectualizer 
     */
    val tableNode = TabNode.createNewTableNode( url, selector, tabName , new File( fn ) );
    
    /**
     * Das Modell wird genutzt, um eine lokale XLS Datei zu erstellen.
     */
    SheetFactory.dumpRowsVectorToExcelSheet( data , tableNode );

    /**
     * Das Modell wird genutzt, um eine Google-Docs Datei zu erstellen.
     */
    SheetFactory.dumpRowsVectorToGoogleDocs( data , tableNode );
  
    /**
     * Das Modell wird im Metastore abgeleg.
     */
    IContextualizer contexter5 = ContextualizerFactory.getBridgedContextualizer();
    tableNode.rememberInContext( contexter5 );
    
  }

}
