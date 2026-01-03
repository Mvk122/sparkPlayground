package sparkplayground;

import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;

public class Main {
    public static SparkSession createSparkSession() {

        String databasePath = "file:///" + Path.of("./database").toAbsolutePath();

        return SparkSession.builder()
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
                .config("spark.sql.catalog.hadoop_catalog.warehouse", databasePath)
                .config("spark.sql.defaultCatalog", "hadoop_catalog")
                .config("spark.master", "local")
                .getOrCreate();
    }

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        spark.sql("CREATE NAMESPACE IF NOT EXISTS custom_iceberg_namespace");

        spark.sql(
                "CREATE TABLE IF NOT EXISTS custom_iceberg_namespace.test (id BIGINT, name STRING) USING ICEBERG"
        );
        System.out.println("Hello World");
    }
}
