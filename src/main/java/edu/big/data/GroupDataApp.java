package edu.big.data;

import com.twitter.chill.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class GroupDataApp {
    private static final byte[] CF_ARTICLE_INFO = Bytes.toBytes("article_info");
    private static final byte[] CF_BEHAVIOR = Bytes.toBytes("behavior");
    private static final byte[] COLUMN_AID = Bytes.toBytes("aid");
    private static final byte[] COLUMN_TYPE = Bytes.toBytes("type");
    private static final String SEPARATOR = "_";
    static final String JDBC_URL = "jdbc:mysql://172.17.0.1:3306/bigdata?serverTimezone=CTT&useUnicode=true&useSSL=false&&characterEncoding=utf8";


    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf().setAppName("HBaseRead").setMaster("local[2]");
        JavaSparkContext spark = new JavaSparkContext(sparkConf);
        Configuration hBaseConf = getHBaseConf();
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                spark.newAPIHadoopRDD(hBaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        hBaseRDD.cache();
        JavaPairRDD<String, Integer> userArticleBehavior = hBaseRDD.mapToPair(GetUserBehavior.INSTANCE);
        JavaPairRDD<String, Integer> counts = userArticleBehavior.reduceByKey(Integer::sum);
        JavaRDD<Row> groupRecord = counts.map((rdd) -> {
            String[] columns = rdd._1().split(SEPARATOR);
            return RowFactory.create(columns[0], columns[1], columns[2], columns[3], rdd._2());
        });
        DataFrameWriter<UserArticleDateBehavior> dataFrame = createDataFrame();
        //停止SparkContext
        spark.stop();
    }

    private static DataFrameWriter createDataFrame() {
        return null;
    }

    private static Configuration getHBaseConf() throws IOException {
        Configuration hBaseConf = HBaseConfiguration.create();
        hBaseConf.set(TableInputFormat.INPUT_TABLE, "bigdata:user_behavior");
        Scan scan = new Scan();
        scan.addColumn(CF_ARTICLE_INFO, COLUMN_AID);
        scan.addColumn(CF_BEHAVIOR, COLUMN_TYPE);
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String ScanToString = Base64.encodeBytes(proto.toByteArray());
        hBaseConf.set(TableInputFormat.SCAN, ScanToString);
        return hBaseConf;
    }


    static Properties mysqlProperties() {
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("driver", "com.mysql.cj.jdbc.Driver");
        return properties;
    }

    static class GetUserBehavior implements PairFunction<Tuple2<ImmutableBytesWritable, Result>,
            String, Integer> {
        static final GetUserBehavior INSTANCE = new GetUserBehavior();
        private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        @Override
        public Tuple2<String, Integer> call(Tuple2<ImmutableBytesWritable, Result> triple) {
            String aid = this.getColumnValue(triple._2(), CF_ARTICLE_INFO, COLUMN_AID);
            String articleType = this.getColumnValue(triple._2(), CF_BEHAVIOR, COLUMN_TYPE);
            String uid = Bytes.toString(triple._2().getRow());
            LocalDateTime dateTime = LocalDateTime.ofEpochSecond(triple._2().current().getTimestamp(), 0, ZoneOffset.ofHours(8));
            String key = aid + SEPARATOR + uid + SEPARATOR + articleType + SEPARATOR + dateTime.format(DATE_FORMATTER);
            return new Tuple2<>(key, 1);
        }

        private String getColumnValue(Result result, byte[] cf, byte[] column) {
            return Bytes.toString(result.getValue(cf, column));
        }
    }
}
