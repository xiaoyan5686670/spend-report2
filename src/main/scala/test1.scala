import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/** *******************************
 * @Author:xiaoyan.qin
 * @Description:
 * @Date:Created in 9:08 2020/9/24
 * @Modified By:
 *           ******************************** */
object test1 {
  def main(args:Array[String]){
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    //set up execution enviroment
    val env =StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env,settings)



    // to use default dialect
    tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)


    //  tableEnv.executeSql(drop table streaming_user_active_log_pg)
    val createTableSql ="""
                          |CREATE TABLE datagen (
                          |                 userid int,
                          |                 proctime as PROCTIME()
                          |                ) WITH (
                          |                 'connector' = 'datagen',
                          |                 'rows-per-second'='100',
                          |                 'fields.userid.kind'='random',
                          |                 'fields.userid.min'='1',
                          |                 'fields.userid.max'='100'
                          |                )
      """.stripMargin
    tableEnv.executeSql(createTableSql)


    tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

    //   tableEnv.executeSql(DROP TABLE IF EXISTS ihr_pg_test)

    val sink_ddl =""" CREATE TABLE pv (
                    |                 day_str STRING,
                    |                  pv bigINT,
                    |                  PRIMARY KEY (day_str) NOT ENFORCED
                    |                ) WITH (
                    |                   'connector' = 'jdbc',
                    |                   'username' = 'root',
                    |                   'password' = 'password',
                    |                   'url' = 'jdbc:mysql://192.168.217.132:3306/test',
                    |                   'table-name' = 'pv'
                    |                )"""
      .stripMargin

    tableEnv.executeSql(sink_ddl)

    tableEnv.executeSql("insert into pv SELECT DATE_FORMAT(proctime, 'yyyy-MM-dd') as day_str, count(*) \n" +
      "FROM datagen \n" +
      "GROUP BY DATE_FORMAT(proctime, 'yyyy-MM-dd')");
    //streamEnv.execute("qxy")

    tableEnv.execute("qxy")
    env.execute("qxy2")
  }


}
