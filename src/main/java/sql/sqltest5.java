package sql;


import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sources.CsvAppendTableSourceFactory;
import org.apache.flink.table.sources.CsvTableSource;
import sun.plugin.cache.OldCacheEntry;

import static org.apache.flink.table.api.Expressions.$;


public class sqltest5 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);


        final Schema schema = new Schema().field("count", DataTypes.INT())
                .field("word", DataTypes.STRING());

        TypeInformation<Tuple2<String, Double>> tupleType = TypeInformation.of(new TypeHint<Tuple2<String, Double>>(){});

        tEnv.connect(new FileSystem().path("c:\\tmp\\qxy.csv")).withFormat(new OldCsv().deriveSchema())
                .withSchema(schema).createTemporaryTable("MySource1");
        tEnv.connect(new FileSystem().path("c:\\tmp\\qxy2.csv")).withFormat(new OldCsv().deriveSchema()).withSchema(schema)
                .createTemporaryTable("MySource2");
        tEnv.connect(new FileSystem().path("c:\\tmp\\sink1")).withFormat(new OldCsv().deriveSchema()).withSchema(schema)
                .createTemporaryTable("MySink1");
        tEnv.connect(new FileSystem().path("c:\\tmp\\sink2")).withFormat(new OldCsv().deriveSchema())
                .withSchema(schema).createTemporaryTable("MySink2");

        Table table1 = tEnv.from("MySource1").where($("word").like("F%"));
        table1.insertInto("MySink1");

        Table table2 = table1.unionAll(tEnv.from("MySource2"));
        table2.insertInto("MySink2");

        /**
         * 输出执行计划
         */
        //String explanation = tEnv.explain(false);
        //System.out.println(explanation);
        tEnv.execute("qxy");

    }
}






























