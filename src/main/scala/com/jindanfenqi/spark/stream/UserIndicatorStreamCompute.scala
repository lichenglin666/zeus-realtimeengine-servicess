package com.jindanfenqi.spark.stream

import java.sql.Timestamp
import java.time.{Duration, LocalDate, LocalDateTime}

import com.jindanfenqi.spark.conf.JdbcConfig
import com.jindanfenqi.spark.model._
import com.jindanfenqi.spark.sources.UserIndicatorJdbcWriter
import com.jindanfenqi.spark.utils.{JdbcUtils, JsonUtils}
import org.apache.spark.sql.functions.{expr, lit}
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming._

import scala.collection.mutable

/**
  * User Indicator transformation and calc
  */
object UserIndicatorStreamCompute {

  /**
    * 转换成 kafka 模型
    */
  def toKafkaModel(spark: SparkSession, dataFrame: DataFrame): Dataset[KafkaModel] = {
    import spark.implicits._
    dataFrame.select($"key".cast("string"), $"value".cast("string"), $"topic", $"partition",
      $"offset", $"timestamp", $"timestampType").as[KafkaModel]
  }

  /**
    * 转换 value 字段
    */
  def toValueField(spark: SparkSession, dataFrame: Dataset[KafkaModel]): Dataset[ValueFieldModel] = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    dataFrame.select($"value".cast("string")).select(
      get_json_object($"value", "$.database").as("database"),
      get_json_object($"value", "$.table").as("table"),
      get_json_object($"value", "$.type").as("operation"),
      get_json_object($"value", "$.ts").cast("long").cast("timestamp").as("timestamp"),
      get_json_object($"value", "$.data").as("data")
    ).as[ValueFieldModel]
  }


  def dataFieldAsUser(spark: SparkSession, dataFrame: DataFrame): Dataset[UserModel] = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    dataFrame.select(
      get_json_object($"data", "$.id").cast("long").as("id"),
      get_json_object($"data", "$.regist_time").cast("timestamp").as("registerTime"),
      get_json_object($"data", "$.channel_id").cast("long").as("channelId"),
      get_json_object($"data", "$.source").cast("int").as("source"),
      get_json_object($"data", "$.gmt_modified").cast("timestamp").as("modifiedTime")
    ).as[UserModel]
  }

  def dataFieldAsUserAuth(spark: SparkSession, dataFrame: DataFrame): Dataset[NewUserAuthModel] = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    dataFrame.select(
      get_json_object($"data", "$.id").cast("long").as("id"),
      get_json_object($"data", "$.user_id").cast("long").as("userId"),
      get_json_object($"data", "$.auth_item_code").as("authItemCode"),
      get_json_object($"data", "$.state").as("state"),
      get_json_object($"data", "$.gmt_create").cast("timestamp").as("createTime"),
      get_json_object($"data", "$.gmt_modified").cast("timestamp").as("modifiedTime")
    ).as[NewUserAuthModel]
  }

  /**
    * 根据数据库表名称字段进行过滤
    * @return
    */
  def filterTable(spark: SparkSession, dataFrame: DataFrame, tableName: String): DataFrame = {
    import spark.implicits._
    dataFrame.filter($"table" === tableName)
  }

  /**
    * 过滤出合法的用户
    *
    */
  def filterValueField(spark: SparkSession, dataset: Dataset[ValueFieldModel]): Dataset[ValueFieldModel] = {
    dataset.filter(v => v.isValidUser || v.isValidAuth || v.isValidAppSession)
  }

  /**
    * 生成用户事件
    *
    */
  def toUserEvent(spark: SparkSession, dataset: Dataset[ValueFieldModel]): Dataset[UserEventModel] = {
    import spark.implicits._
    dataset.map(v => {
      v.table match {
        // 处理用户表
        case "cl_user" => {
          val user = JsonUtils.toUserTableModel(v.data)
          UserEventModel(user.id, v.timestamp, user.registerTime, user.channelId, UserEventType.NEW_USER_REGISTER)
        }

        // 处理认证表
        case "cl_new_user_auth" => {
          val auth = JsonUtils.toNewUserAuthTableModel(v.data)
          UserEventModel(auth.userId,v.timestamp, auth.createTime, 0L, auth.authItemCode)
        }

        // 处理 app session 表
        case "cl_app_session" => {
          val appSession = JsonUtils.toAppSessionTableModel(v.data)
          UserEventModel(appSession.userId, v.timestamp, appSession.lastAccessTime, 0L, UserEventType.APP_SESSION)
        }
      }
    })
  }

  /**
    * 计算用户指标
    *
    */
  def calcUserIndicatorModel(spark: SparkSession, ds: Dataset[UserEventModel]): Dataset[UserIndicatorModel] = {
    import spark.implicits._
    ds.groupByKey(event => event.userId).flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.ProcessingTimeTimeout)(mapUserIndicatorModelFunc)
  }

  /**
    *  用于映射用户指标的函数，对 userId 相同的数据进行计算
    */
  val mapUserIndicatorModelFunc: (Long, Iterator[UserEventModel], GroupState[NewUserStateModel]) =>
    Iterator[UserIndicatorModel] = (_, userEvents, state) => {

    val events = userEvents.toList
    val it = events.filter(e => e.event.equalsIgnoreCase(UserEventType.NEW_USER_REGISTER))

    // 当前批次中是否存在新用户注册事件
    val existsNewUser = it.nonEmpty
    val newUser: UserEventModel = if (existsNewUser) it.head else null

    val result: Iterator[UserIndicatorModel] = if (state.exists || existsNewUser) {

      val flags = if (state.exists) state.get.flags else mutable.HashSet[String]()
      val filteredEvents = events.filter(e => flags.add(e.event))

      // 过滤后的事件满足统计要求
      if (filteredEvents.nonEmpty) {

        // 更新状态
        updateUserState(state, newUser, flags)

        // 产生用户指标
        genUserIndicator(filteredEvents, state, existsNewUser, newUser)
      } else {
        // userId 已经计算过了，丢弃
        List[UserIndicatorModel]().toIterator
      }
    } else {
      // userId 不是当日注册用户，丢弃
      List[UserIndicatorModel]().toIterator
    }

    result
  }

  def genUserIndicator(filteredEvents: List[UserEventModel], state: GroupState[NewUserStateModel],
                               existsNewUser: Boolean, newUser: UserEventModel) = {

    filteredEvents.map(userEvent => if (existsNewUser) {
      UserIndicatorModel(newUser.userId, newUser.ts, newUser.channelId, userEvent.event)
    } else {
      val userState = state.get
      UserIndicatorModel(userState.userId, userState.ts, userState.channelId, userEvent.event)
    }).toIterator
  }

  def updateUserState(state: GroupState[NewUserStateModel], newUser: UserEventModel, flags: mutable.HashSet[String]) = {
    if (state.exists) {
      val userState = state.get
      state.update(NewUserStateModel(userState.userId, userState.ts, userState.channelId, flags))
    } else {
      state.update(NewUserStateModel(newUser.userId, newUser.ts, newUser.channelId, flags))
      if (!state.hasTimedOut) {
        state.setTimeoutDuration(
          Duration.between(LocalDateTime.now(), LocalDate.now().plusDays(1).atStartOfDay()).abs().toMillis
        )
      }
    }
  }

  /**
    * 计算用户指标，生产数据库需要的格式数据
    */
    def calcUserIndicatorResult(spark: SparkSession, ds: Dataset[UserIndicatorModel], channel: Dataset[ChannelModel]) = {
      import spark.implicits._
      import org.apache.spark.sql.functions._

      val curDateTime = LocalDateTime.now().withNano(0).withSecond(0).minusMinutes(1)
      val curTimestamp = Timestamp.valueOf(curDateTime)
      ds.filter(expr("date_format(ts, 'yyyy-MM-dd HH:mm:00')").cast("timestamp") === curTimestamp)
        .join(channel, ds("channelId") === channel("channelId"))
        .select(
          expr("date_format(ts, 'yyyy-MM-dd HH:mm:00')").cast("timestamp").as("ts"),
          ds("userId"), channel("channelType"), ds("eventType"))
        .groupBy($"ts", $"channelType", $"eventType")
        .agg(count("*").as("quantity")).as[MySqlUserIndicatorModel]
    }


  /**
    * 保存数据流结果到数据库中
    *
    */
  def writeStreamToDatabase(spark: SparkSession, ds: Dataset[(String, String, Long)]) = {
    import spark.implicits._
    import org.apache.spark.sql.functions.lit

    // 截取到分
    val ts = Timestamp.valueOf(LocalDateTime.now().withSecond(0).withNano(0))
    JdbcUtils.saveStream(ds.withColumn("ts", lit(ts)).as[(String, String, Long, Timestamp)])
  }


  /**
    * 计算当天累计新增用户注册数
    *
    * @param spark SparkSession
    * @param ds  用户事件 Dataset
    * @return
    */
  def calcTotalNewUserQuantity(spark: SparkSession, ds: Dataset[UserEventModel]) = {
    import spark.implicits._

    val channel = getChannel(spark)

    val user = ds.filter($"event" === UserEventType.NEW_USER_REGISTER)
      .withWatermark("ts", "1 day")
      .where(expr("to_date(ts) = current_date()"))
      .dropDuplicates("userId")
      .join(channel, ds("channelId") === channel("channelId"))
      .select("userId", "channelType")
      .groupBy("channelType")
      .count()
      .withColumn("ts", lit(
        Timestamp.valueOf(LocalDateTime.now.withNano(0).withSecond(0).minusMinutes(1))
      ))
      .select(
        $"ts",
        $"channelType",
        lit(UserEventType.TOTAL_NEW_USER_REGISTER).as("eventType"),
        expr("count as quantity")
      ).as[MySqlUserIndicatorModel]

    val jdbcOptions = JdbcConfig.tableConfig()

    user.writeStream
      .foreach(new UserIndicatorJdbcWriter(jdbcOptions(JdbcConfig.URL), jdbcOptions(JdbcConfig.TABLE),
          jdbcOptions(JdbcConfig.USER), jdbcOptions(JdbcConfig.PASSWORD))
      )
      .outputMode("update")
      .option("checkpointLocation", "/user/root/user_indicator_total_new_user")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start
}


  /**
    * 获取用户渠道
    *
    * @param spark SparkSession
    * @return
    */
  def getChannel(spark: SparkSession) = {
    import spark.implicits._
    spark.sql("select cast(id as long) as channelId, typename as channelType from dim.dim_biz_channel").cache().as[ChannelModel]
  }
}
