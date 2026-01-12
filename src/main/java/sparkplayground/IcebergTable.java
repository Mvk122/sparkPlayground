package sparkplayground;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public abstract class IcebergTable {
    final SparkSession sparkSession;
    private final String tableName;
    private final String namespace;

    public IcebergTable(SparkSession sparkSession, String namespace, String tableName) {
        this.sparkSession = sparkSession;
        this.tableName = tableName;
        this.namespace = namespace;
    }

    public void initialize() {
        this.createNamespace();
        this.createTable();
    }

    public Dataset<Row> createNamespace() {
        return sparkSession.sql("CREATE NAMESPACE IF NOT EXISTS " + this.namespace);
    }

    String getSchemaString() {
        String resourcePath = "/" + this.tableName + "/schema.sql";
        try {
            return Utils.readResourcesFile(resourcePath);
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        }
    }

    public String getTablePath() {
        return this.namespace + "." + this.tableName;
    }

    Dataset<Row> createTable() {
        String schema = getSchemaString();
        String statement = String.format("CREATE TABLE IF NOT EXISTS %s.%s %s USING ICEBERG", this.namespace, this.tableName, schema);
        return sparkSession.sql(statement);
    }

    public abstract void insertDummyData();

}
