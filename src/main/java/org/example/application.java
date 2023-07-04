package org.example;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class application {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("aeroport SPARK SQL").master("local[*]").getOrCreate();

        Map<String,String> options = new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/DB_AEROPORT");
        options.put("user","root");
        options.put("password","");

        //Afficher pour charque vol, le nombre de passagers selon le format d’affichage suivant :
        //ID_VOL |DATE DEPART| NOMBRE
        Dataset<Row> aeroportVols = sparkSession.read().format("jdbc").options(options)
                .option("query","select ID_VOL, DATE_DEPART, count(ID_VOL) as NOMBRE from VOLS, RESERVATIONS where ID_VOL = VOLS.ID group by ID_VOL")
                .load();

        aeroportVols.show();

        //Afficher la liste des vols en cours selon le format d’affichage suivant :
        //ID_VOL |DATE DEPART| DATE ARRIVE
        // supposons que la date d aujourd hui est : 06/06/2023

        String dateEnCours  = "2023-06-06";
        //on peut utiliser la date courante avec local date , juste aue jai pas assez de donnes sur la base de donnee
        //LocalDate currentDate = LocalDate.now();
        //dateEnCours = currentDate.format(DateTimeFormatter.ISO_DATE);

        /*Dataset<Row> aeroportVolsEnCours = sparkSession.read().format("jdbc").options(options)
               .option("query","select * from VOLS where DATE_DEPART <= '"+dateEnCours+"' AND DATE_ARRIVE >= '"+dateEnCours+"'")
                .load();*/

        Dataset<Row> aeroportVolsEnCours = sparkSession.read().format("jdbc").options(options)
                .option("dbtable","VOLS")
                .load();
        Dataset<Row> volsEnCours = aeroportVolsEnCours.filter(aeroportVolsEnCours.col("DATE_DEPART").leq(dateEnCours).and(aeroportVolsEnCours.col("DATE_ARRIVE").geq(dateEnCours)));
        volsEnCours.show();
    }
}
