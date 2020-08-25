import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class RankAndDenseRank {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("sparkAPplication");

        SparkSession session;
        session = SparkSession.builder().appName("sparkAPpl").config("spark.master", "local[2]").getOrCreate();

        Dataset<Row> rowDataset = session.read().option("header", "true").option("inferSchema", "true").csv("src/main/resources/film1.csv").toDF();

        Dataset<Row> rowDataset1 = rowDataset.groupBy("Director").count().alias("count").filter("count > 10").orderBy(functions.col("count").desc());
        //rowDataset.groupBy("Director").count().alias("count").filter("count > 10").orderBy("count").

        //Dataset<Row> rank_1 = rowDataset1.withColumn("rank ", functions.rank().over());

        org.apache.spark.sql.expressions.WindowSpec w = org.apache.spark.sql.expressions.Window.orderBy(rowDataset1.col("count"));

        Dataset<Row> rank_1 = rowDataset1.withColumn("rank ", functions.rank().over(w)).toDF("Director", "count", "rank");

        rank_1.show(20);

        rank_1.printSchema();
        //rank_1.filter("rank == 2").show(10);

        Dataset<Row> rank_2 = rowDataset1.withColumn("rank ", functions.dense_rank().over(w)).toDF("Director", "count", "rank");

        //Dataset<Row> rowDataset2 = rank_1.toDF(_ *);
        rank_2.show(20);
    }

    }
