package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import java.time.LocalDate;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.*;


public class applicationIncidents {
    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Gestion des incidents aeroport - SPARK STREAM ").getOrCreate();

        //Reccuper tous les incidents de tous les fichiers et les rassembler en un seul dataset
        Dataset<Row> incidentsVols = sparkSession.read().option("header","true").csv("hdfs://localhost:9000/incidentsAeroport/");

        incidentsVols.show();

        //total des incidents pour chaque avion
        Dataset<Row> totalIncidentsByAvion = incidentsVols.groupBy("no_avion").count().withColumnRenamed("count","total_incidents")
                .orderBy(functions.desc("total_incidents"));

        totalIncidentsByAvion.show();

        //l'avion ayant plus d'incidents
        Dataset<Row> avionMaxIncidents = totalIncidentsByAvion.limit(1);

        avionMaxIncidents.show();

        //les deux mois de l'annee en cours ou il y a plus d'incidents
        incidentsVols  = incidentsVols.withColumn("Year", year(to_date(col("date"), "yyyy-MM-dd")));
        incidentsVols  = incidentsVols.withColumn("Month", month(to_date(col("date"), "yyyy-MM-dd")));
        incidentsVols.show();
        incidentsVols.createOrReplaceTempView("incidentsYearMonth");
        int currentYear = LocalDate.now().getYear();
        Dataset<Row> totalIncidentsByAvionMonth = sparkSession.sql("SELECT Month, count(*) AS TotalIncident from incidentsYearMonth where Year = '"
                +currentYear+"' group by Month  order by TotalIncident desc limit 2");

        totalIncidentsByAvionMonth.show();




    }
}
