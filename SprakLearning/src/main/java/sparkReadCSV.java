import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class sparkReadCSV {

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("sparkAPplication");

        SparkSession session;
        session = SparkSession.builder().appName("sparkAPpl").config("spark.master", "local[2]").getOrCreate();

        Dataset<Row> rowDataset = session.read().option("header", "true").option("inferSchema", "true").csv("/Users/mohit.kundra/Downloads/film1.csv").toDF();

        rowDataset.show(5);

        rowDataset.printSchema();

        rowDataset.select("year","Actor","Director","Awards").show(10);

        System.out.println("++++++++++++++++++++++++++++++++++");
        long count = rowDataset.filter("year == 1990 ").count();

        System.out.println("++++++++++++++++++++++++++++++++++"+count);

        rowDataset.filter("Director like 'A%'").show(10);

        rowDataset.groupBy("Director").count();

        rowDataset.filter("Length in (100,111,107)").show(20);

        rowDataset.filter("Subject like 'Comedy'").filter("Year == 1990").show(20);

        rowDataset.groupBy("Director").count().alias("count").filter("count > 10").orderBy(org.apache.spark.sql.functions.col("count").desc()).show();

        Dataset<Row> rowDataset1 = rowDataset.groupBy("Director").count().alias("count").filter("count > 10").orderBy(functions.col("count").desc());
        //rowDataset.groupBy("Director").count().alias("count").filter("count > 10").orderBy("count").

        //Dataset<Row> rank_1 = rowDataset1.withColumn("rank ", functions.rank().over());

        org.apache.spark.sql.expressions.WindowSpec w = org.apache.spark.sql.expressions.Window.orderBy(rowDataset1.col("count"));

        Dataset<Row> rank_1 = rowDataset1.withColumn("rank ", functions.rank().over(w)).toDF("Director","count","rank");

        rank_1.show(20);

        rank_1.printSchema();
        //rank_1.filter("rank == 2").show(10);

        Dataset<Row> rank_2 = rowDataset1.withColumn("rank ", functions.dense_rank().over(w)).toDF("Director","count","rank");

        //Dataset<Row> rowDataset2 = rank_1.toDF(_ *);
        rank_2.show(20);
    }
}
