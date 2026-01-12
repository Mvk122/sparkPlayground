package sparkplayground;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class Main {
    public static SparkSession createSparkSession() {

        String databasePath = "file:///" + Path.of("./database").toAbsolutePath();

        return SparkSession.builder()
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
                .config("spark.sql.catalog.hadoop_catalog.warehouse", databasePath)
                .config("spark.sql.catalog.hadoop_catalog.table-default.format-version", "1")
                .config("spark.sql.defaultCatalog", "hadoop_catalog")
                .config("spark.master", "local")
                .getOrCreate();
    }

    public static void main(String[] args) throws IOException {
        FileUtils.deleteDirectory(new File("database"));

        SparkSession spark = createSparkSession();

        IcebergTable v1Table = new V1Table(spark, "v1Namespace");
        v1Table.initialize();
        v1Table.insertDummyData();

        System.out.println("Made it to the end");
    }
}
