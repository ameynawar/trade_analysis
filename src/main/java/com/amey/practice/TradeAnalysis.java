package com.amey.practice;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.joda.time.DateTime;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import static org.apache.spark.sql.functions.*;

public class TradeAnalysis {
    protected String partitionColumns;
    protected String numberOfDays;

    public TradeAnalysis(String partitionColumns, String numberOfDays) {
        this.numberOfDays = numberOfDays;
        this.partitionColumns = partitionColumns;
    }

    public static void main(String[] args) throws ParseException {
        String number_of_days = args[0];
        //initialize a sparksession.
        SparkSession sparkSession = SparkSession.builder()
                .appName("User_Account_detail")
                .master("local[*]")
                .getOrCreate();
        String partitionColumn = args[1];

        //Read source file from the resource folder.
        Dataset<Row> sourceDataset = sparkSession.read().option("header", true).
                csv("src/main/resources/source.csv");
        String derived_report_column_name = "SALES_" + number_of_days + "_DAY_DATE";

        //Calculate the max(SALES_Date) from table and report_generation date which is max(SALES_Date) - number_of_days.
        ArrayList<String> calculated_dates = getReportDate(sourceDataset, number_of_days);

        sourceDataset = sourceDataset.withColumn(derived_report_column_name, lit(calculated_dates.get(1)))
                .withColumn("MAX_SALES_DATE", lit(calculated_dates.get(0)));

        // Create a window where data is partitioned by partition column and order by sales_date. With this we will get the historical sales pattern
        // for the partitioned columns.
        WindowSpec window = Window.partitionBy(partitionColumn).orderBy(col("SALES_DATE"));

        //Then lag function to get their previous sales_quantity for the our analysis.
        sourceDataset = sourceDataset.withColumn("SALES_" + number_of_days + "_days_QUANTITY", lag("SALES_QUANTITY", 1).over(window));

        //Filter those sales only happened on the max(SALES_Date) date for the report generation.
        sourceDataset = sourceDataset.filter(to_date(col("SALES_DATE"), "dd-MM-yyyy").equalTo(to_date(col("MAX_SALES_DATE"), "dd-MM-yyyy")));

        //Get the total number of sales happened during time period
        sourceDataset = sourceDataset.groupBy(partitionColumn).agg(sum("SALES_" + number_of_days + "_days_QUANTITY").alias("SALES_" + number_of_days + "_days_QUANTITY"),
                sum("SALES_QUANTITY").alias("SALES_TODAY_QUANTITY"));

        sourceDataset = sourceDataset.withColumn(derived_report_column_name, lit(calculated_dates.get(1)))
                .withColumn("SALES_DATE", lit(calculated_dates.get(0)));

        // prepare a final dataset and display the desired result.
        sourceDataset.select(col("SALES_DATE"), col(partitionColumn), col(derived_report_column_name),
                col("SALES_TODAY_QUANTITY"), col("SALES_" + number_of_days + "_days_QUANTITY"))
                .show(false);
    }

    /*
        This function is used to calculate the dates which are getting used in this analysis.
        It will return these values viz. max(SALES_Date) and max(SALES_Date) - Number_days.
     */
    public static ArrayList<String> getReportDate(Dataset<Row> sourceDataset, String number_of_days) throws ParseException {
        Row[] max_date_from_df = (Row[]) sourceDataset.agg(max("SALES_DATE")).collect();
        String max_date = max_date_from_df[0].getString(0);
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
        Date date = format.parse(max_date);
        String report_date = format.format(new DateTime(date).minusDays(Integer.parseInt(number_of_days)).toDate());
        ArrayList<String> resultValue = new ArrayList<>();
        resultValue.add(max_date);
        resultValue.add(report_date);
        return resultValue;
    }
}
