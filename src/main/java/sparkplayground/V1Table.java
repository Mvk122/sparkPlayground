package sparkplayground;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class V1Table extends IcebergTable {
    public V1Table(SparkSession sparkSession, String namespace) {
        super(sparkSession, namespace, "v1Table");
    }

    @Override
    public void insertDummyData() {
        List<Row> data = List.of(
                RowFactory.create(1L, "Alice", "123 Maple St"),
                RowFactory.create(2L, "Bob", "456 Oak Ave"),
                RowFactory.create(3L, "Madhav", "Address in Heck")
        );

        String tablePath = this.getTablePath();
        StructType tableSchema = this.sparkSession.table(tablePath).schema();
        Dataset<Row> df = this.sparkSession.createDataFrame(data, tableSchema);

        try {
            df.writeTo(tablePath).append();
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }
    }
}
