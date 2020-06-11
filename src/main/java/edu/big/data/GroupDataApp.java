package edu.big.data;

import com.twitter.chill.Base64;
import com.zaxxer.hikari.HikariConfig;
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
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
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
        SparkContext sparkContext = new SparkContext(sparkConf);
        JavaSparkContext spark = new JavaSparkContext(sparkContext);
        Configuration hBaseConf = getHBaseConf();
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                spark.newAPIHadoopRDD(hBaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        hBaseRDD.cache();
        JavaPairRDD<UserArticleDateBehavior, Integer> userArticleBehavior = hBaseRDD.mapToPair(GetUserBehavior.INSTANCE);
        JavaPairRDD<UserArticleDateBehavior, Integer> counts = userArticleBehavior.reduceByKey(Integer::sum);
        JavaRDD<Row> groupRecord = counts.map((rdd) -> {
            UserArticleDateBehavior userBehavior = rdd._1();
            return RowFactory.create(userBehavior.getUid(), userBehavior.getDomain(), userBehavior.getBehavior(), userBehavior.getBehaviorDate(), rdd._2());
        });
        SparkSession sparkSession = new SparkSession(sparkContext);
        SQLContext sqlContext = new SQLContext(sparkSession);
        StructType structType = buildStructType();
        Dataset<Row> dataset = sqlContext.createDataFrame(groupRecord, structType);
        DataFrameWriter<Row> dataFrame = new DataFrameWriter<>(dataset);
        dataFrame.mode(SaveMode.Append).jdbc(JDBC_URL, "user_article_date_behavior", mysqlProperties());
        sparkContext.stop();
    }

    private static StructType buildStructType() {
        List<StructField> fields = new ArrayList<>();
        StructField uid = DataTypes.createStructField("uid", DataTypes.StringType, true);
        fields.add(uid);
        StructField domain = DataTypes.createStructField("domain", DataTypes.StringType, true);
        fields.add(domain);
        StructField behavior = DataTypes.createStructField("behavior", DataTypes.StringType, true);
        fields.add(behavior);
        StructField behaviorDate = DataTypes.createStructField("behavior_date", DataTypes.DateType, true);
        fields.add(behaviorDate);
        StructField counts = DataTypes.createStructField("counts", DataTypes.IntegerType, true);
        fields.add(counts);
        return DataTypes.createStructType(fields);
    }


    private static Configuration getHBaseConf() throws IOException {
        Configuration hBaseConf = HBaseConfiguration.create();
        hBaseConf.set(TableInputFormat.INPUT_TABLE, "bigdata:user_behavior");
        Scan scan = new Scan();
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
            UserArticleDateBehavior, Integer> {
        static final GetUserBehavior INSTANCE = new GetUserBehavior();
        private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        private static final HikariConfig HIKARI_CONFIG;

        @Override
        public Tuple2<UserArticleDateBehavior, Integer> call(Tuple2<ImmutableBytesWritable, Result> triple) {
            String behaviorType = this.getColumnValue(triple._2(), CF_BEHAVIOR, COLUMN_TYPE);
            String key = Bytes.toString(triple._2().getRow());
            UserArticleDateBehavior userArticleDateBehavior = new UserArticleDateBehavior();
            String[] values = key.split(SEPARATOR);
            userArticleDateBehavior.setBehavior(behaviorType);
            userArticleDateBehavior.setUid(values[0]);
            String aid = values[1];

            LocalDateTime dateTime = LocalDateTime.ofEpochSecond(triple._2().current().getTimestamp(), 0, ZoneOffset.ofHours(8));
            String key = aid + SEPARATOR + uid + SEPARATOR + behaviorType + SEPARATOR + dateTime.format(DATE_TIME_FORMATTER);
            return new Tuple2<>(userArticleDateBehavior, 1);
            HIKARI_CONFIG.getDataSource().getConnection().prepareStatement();
        }

        private String getColumnValue(Result result, byte[] cf, byte[] column) {
            return Bytes.toString(result.getValue(cf, column));
        }
    }
}
