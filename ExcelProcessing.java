/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.myapp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class ExcelProcessing {

    public static void processExcelFile(String filePath) {
        SparkSession spark = SparkSession.builder()
                .appName("ExcelProcessing")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .master("local")
                .getOrCreate();

        Dataset<Row> rawData = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filePath);
        
        List<String> fixedColumns = Arrays.asList("State Code", "State", "Zone/Circle Code","Zone/Circle Name","District Code","District Name","District Category",
                "Sub-District / ULB Code","Sub-District / ULB Name","Block Code","Block Name","Local Assembly Constituency","Parliament Constituency","Facility Type",
                "Facility Sub-Type","Facility Code","Facility Name","Infacility Or Outreach Or Total","Providing Outreach Service","ULB Code","ULB Name","Category",
                "Ownership Classification","Sanctioned Bed Count","Functional Bed Count","Facility NIN Number","Rural/Urban","Ownership","Physical/Notional","Status",
                "Active from date","Active to date"); // Replace with your actual fixed column names
        List<String> variableColumns = Arrays.asList("v1", "v9", "v3","v7","v5","v6","v8","v4","v2","v25","v10","v11","v13","v12","v14","v16","v15","v17","v18","v19",
                "v20","v24","v21","v22","v23","v26","v30","v27","v28","v29","v31","v32","v33","v34","v35","v36","v37","v38","v39","v40","v41","v42","v43","v44","v45",
                "v46","v47","v48","v49","v50","v51","v52","v53","v54","v55","v56","v57"); // Replace with your actual variable column names
        
        // Combine fixed and variable columns
        List<String> allColumns = fixedColumns.stream().map(col -> "`" + col + "`")
                .collect(Collectors.toList());
        allColumns.addAll(variableColumns.stream().map(col -> "`" + col + "`").collect(Collectors.toList()));
        // Read columns with backticks to handle special characters
        Dataset<Row> newData = rawData.selectExpr(allColumns.toArray(new String[0]));
        
        String url = "jdbc:mysql://localhost:3306/ihat";
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "shourey@2003");
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");

        String tableName = "users6";

        Dataset<Row> existingData = spark.read()
                .jdbc(url, tableName, properties);

        List<String> keyColumns = Arrays.asList("Facility Code");
        

        if (existingData.isEmpty()) {
            saveToDatabase(newData, url, properties, tableName);
        } else {
            Dataset<Row> newRows = newData.join(existingData, createJoinExpression(newData, existingData, keyColumns), "left_anti");

            String updateConditions = allColumns.stream()
                    .filter(column -> !keyColumns.contains(column))
                    .map(column -> "`newData`." + column + " != `existingData`." + column)
                    .collect(Collectors.joining(" OR "));

            Dataset<Row> updateRows = newData.as("newData")
                    .join(existingData.as("existingData"), createJoinExpression(newData, existingData, keyColumns))
                    .filter(updateConditions);

            deleteRows(updateRows, url, properties, tableName, keyColumns);

            saveToDatabase(newRows, url, properties, tableName);
            saveToDatabase(updateRows, url, properties, tableName);
        }
    }

    private static Column createJoinExpression(Dataset<Row> newData, Dataset<Row> existingData, List<String> keyColumns) {
        Column joinExpression = null;
        for (String keyColumn : keyColumns) {
            Column condition = newData.col("`" + keyColumn + "`").equalTo(existingData.col("`" + keyColumn + "`"));
            joinExpression = joinExpression == null ? condition : joinExpression.and(condition);
        }
        return joinExpression;
    }
    /*
    private static Column createUpdateConditions(Dataset<Row> newData, Dataset<Row> existingData, String keyColumn) {
        List<String> nonKeyColumns = Arrays.stream(newData.columns())
                                           .filter(col -> !col.equals(keyColumn))
                                           .collect(Collectors.toList());

        Column updateConditions = newData.col(nonKeyColumns.get(0)).notEqual(existingData.col(nonKeyColumns.get(0)));
        for (int i = 1; i < nonKeyColumns.size(); i++) {
            updateConditions = updateConditions.or(newData.col(nonKeyColumns.get(i)).notEqual(existingData.col(nonKeyColumns.get(i))));
        }
        return updateConditions;
    }
*/

    private static void deleteRows(Dataset<Row> rowsToDelete, String url, Properties properties, String tableName, List<String> keyColumns) {
        rowsToDelete.foreachPartition(rows -> {
            try (Connection conn = DriverManager.getConnection(url, properties)) {
                conn.setAutoCommit(false);
                String deleteQuery = "DELETE FROM " + tableName + " WHERE "
                        + keyColumns.stream().map(c -> "`" + c + "` = ?").collect(Collectors.joining(" AND "));

                try (PreparedStatement stmt = conn.prepareStatement(deleteQuery)) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        for (int i = 0; i < keyColumns.size(); i++) {
                            stmt.setObject(i + 1, row.get(row.fieldIndex(keyColumns.get(i))));
                        }
                        stmt.addBatch();
                    }
                    stmt.executeBatch();
                    conn.commit();
                }
            }
        });
    }

    public static void saveToDatabase(Dataset<Row> df, String url, Properties properties, String tableName) {
        df.write()
            .mode(SaveMode.Append)
            .jdbc(url, tableName, properties);
    }
}