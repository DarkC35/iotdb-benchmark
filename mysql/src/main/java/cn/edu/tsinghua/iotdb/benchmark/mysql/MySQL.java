/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iotdb.benchmark.mysql;

import cn.edu.tsinghua.iotdb.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.enums.DBInsertMode;
import cn.edu.tsinghua.iotdb.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.DeviceQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.VerificationQuery;
import com.mysql.cj.jdbc.StatementImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

public class MySQL implements IDatabase {

  protected static Config config = ConfigDescriptor.getInstance().getConfig();
  protected static final Logger LOGGER = LoggerFactory.getLogger(MySQL.class);

  protected static final String MYSQL_JDBC_NAME = "com.mysql.cj.jdbc.Driver";
  protected static final String MYSQL_URL = "jdbc:mysql://%s:%s/%s?allowLoadLocalInfile=true";

  protected static final String dropTable = "DROP TABLE IF EXISTS %s;";
  protected static final AtomicBoolean schemaInit = new AtomicBoolean(false);
  protected static final CyclicBarrier schemaBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());

  protected static String tableName;
  protected Connection connection;
  protected DBConfig dbConfig;

  public MySQL(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    tableName = dbConfig.getDB_NAME();
  }

  @Override
  public void init() throws TsdbException {
    try {
      Class.forName(MYSQL_JDBC_NAME);
      // default username=mysql and password=mysql
      connection =
          DriverManager.getConnection(
              String.format(
                  MYSQL_URL,
                  dbConfig.getHOST().get(0),
                  dbConfig.getPORT().get(0),
                  dbConfig.getDB_NAME()),
              dbConfig.getUSERNAME(),
              dbConfig.getPASSWORD());
    } catch (Exception e) {
      LOGGER.error("Initialize MySQL failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    // delete old data
    try (Statement statement = connection.createStatement()) {
      statement.execute(String.format(dropTable, tableName));
    } catch (Exception e) {
      LOGGER.warn("delete old data table {} failed, because: {}", tableName, e.getMessage());
      if (!e.getMessage().contains("does not exist")) {
        throw new TsdbException(e);
      }
    }
  }

  @Override
  public void close() throws TsdbException {
    if (connection == null) {
      return;
    }
    try {
      connection.close();
    } catch (Exception e) {
      LOGGER.error("Failed to close MySQL connection because: {}", e.getMessage());
      throw new TsdbException(e);
    }
  }

  /**
   * Map the data schema concepts as follow: DB_NAME(table name), storage group name(table field)
   * device name(table field), sensors(table fields)
   *
   * @return
   */
  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    long start;
    long end;
    start = System.nanoTime();
    if (schemaInit.compareAndSet(false, true)) {
      try (Statement statement = connection.createStatement()) {
        String sql = getCreateTableSql(tableName, schemaList.get(0).getSensors());
        LOGGER.debug("CreateTableSQL Statement:  {}", sql);
        statement.execute(sql);
      } catch (SQLException e) {
        LOGGER.error("Can't create MySQL table because: {}", e.getMessage());
        throw new TsdbException(e);
      }
    }
    try {
      schemaBarrier.await();
    } catch (Exception e) {
      throw new TsdbException(e.getMessage());
    }
    end = System.nanoTime();
    return TimeUtils.convertToSeconds(end - start, "ns");
  }

  @Override
  public Status insertOneBatch(Batch batch) {

    if (config.getDbConfig().getDB_SWITCH().getInsertMode()
        == DBInsertMode.INSERT_USE_SQL_SINGLE_ROW) {
      try (Statement statement = connection.createStatement()) {
        for (Record record : batch.getRecords()) {
          String sql =
              getInsertOneBatchSql(
                  batch.getDeviceSchema(), record.getTimestamp(), record.getRecordDataValue());
          statement.addBatch(sql);
        }

        statement.executeBatch();

        return new Status(true);
      } catch (Exception e) {
        return new Status(false, 0, e, e.toString());
      }
    } else if (config.getDbConfig().getDB_SWITCH().getInsertMode()
        == DBInsertMode.INSERT_USE_SQL_MULTI_ROW) {
      try (Statement statement = connection.createStatement()) {
        boolean first = true;
        StringBuilder builder = new StringBuilder();
        DeviceSchema deviceSchema = batch.getDeviceSchema();
        List<Sensor> sensors = deviceSchema.getSensors();

        Map<Long, Record> uniqueTimestampRecords = new HashMap<>();
        batch.getRecords().stream().forEach(r -> uniqueTimestampRecords.put(r.getTimestamp(), r));

        builder.append("insert into ").append(tableName).append("(time, sGroup, device");
        for (Sensor sensor : sensors) {
          builder.append(",").append(sensor.getName());
        }
        builder.append(") values(");
        for (Record record : uniqueTimestampRecords.values()) {
          if (!first) {
            builder.append("),(");
          } else {
            first = false;
          }
          builder.append(record.getTimestamp());
          builder.append(",'").append(deviceSchema.getGroup()).append("'");
          builder.append(",'").append(deviceSchema.getDevice()).append("'");
          for (Object value : record.getRecordDataValue()) {
            builder.append(",'").append(value).append("'");
          }
        }
        builder.append(") ON DUPLICATE KEY UPDATE ");
        builder
            .append(sensors.get(0).getName())
            .append("=VALUES(")
            .append(sensors.get(0).getName())
            .append(")");
        for (int i = 1; i < sensors.size(); i++) {
          builder
              .append(",")
              .append(sensors.get(i).getName())
              .append("=VALUES(")
              .append(sensors.get(i).getName())
              .append(")");
        }
        if (!config.isIS_QUIET_MODE()) {
          LOGGER.debug("getInsertOneBatchSql: {}", builder);
        }
        statement.execute(builder.toString());
        return new Status(true);
      } catch (Exception e) {
        return new Status(false, 0, e, e.toString());
      }
    } else if (config.getDbConfig().getDB_SWITCH().getInsertMode()
        == DBInsertMode.INSERT_USE_SQL_COPY) {
      try (StatementImpl statement = (StatementImpl) connection.createStatement()) {
        DeviceSchema deviceSchema = batch.getDeviceSchema();
        List<Sensor> sensors = deviceSchema.getSensors();
        StringBuilder builder = new StringBuilder();
        StringBuilder sql = new StringBuilder();

        builder.append("time,sGroup,device");
        for (Sensor sensor : sensors) {
          builder.append(",").append(sensor.getName());
        }
        builder.append("\n");
        for (Record record : batch.getRecords()) {
          builder.append(record.getTimestamp());
          builder.append(",").append(deviceSchema.getGroup());
          builder.append(",").append(deviceSchema.getDevice());
          for (Object value : record.getRecordDataValue()) {
            builder.append(",").append(value);
          }
          builder.append("\n");
        }

        statement.setLocalInfileInputStream(
            new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8)));
        sql.append("LOAD DATA LOCAL INFILE '' INTO TABLE ").append(tableName);
        sql.append(" FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 LINES");
        statement.execute(sql.toString());

        return new Status(true);
      } catch (Exception e) {
        return new Status(false, 0, e, e.toString());
      }
    } else {
      return new Status(
          false, 0, new Exception("InsertMode not supported."), "InsertMode not supported.");
    }
  }

  /**
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') and time=1535558400000.
   *
   * @param preciseQuery universal precise query condition parameters
   */
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    int sensorNum = preciseQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getSampleQuerySqlHead(preciseQuery.getDeviceSchema());
    builder.append(" AND time = ").append(preciseQuery.getTimestamp());
    return executeQueryAndGetStatus(builder.toString(), sensorNum, Operation.PRECISE_QUERY);
  }

  /**
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') AND (time >= 1535558400000 AND
   * time <= 1535558650000).
   *
   * @param rangeQuery universal range query condition parameters
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    int sensorNum = rangeQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getSampleQuerySqlHead(rangeQuery.getDeviceSchema());
    addWhereTimeClause(builder, rangeQuery);
    return executeQueryAndGetStatus(builder.toString(), sensorNum, Operation.RANGE_QUERY);
  }

  /**
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') and (s_2 > 78).
   *
   * @param valueRangeQuery contains universal range query with value filter parameters
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    int sensorNum = valueRangeQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getSampleQuerySqlHead(valueRangeQuery.getDeviceSchema());
    addWhereTimeClause(builder, valueRangeQuery);
    addWhereValueClause(
        valueRangeQuery.getDeviceSchema(), builder, valueRangeQuery.getValueThreshold());
    return executeQueryAndGetStatus(builder.toString(), sensorNum, Operation.VALUE_RANGE_QUERY);
  }

  /**
   * eg. SELECT device, count(s_2) FROM tutorial WHERE (device='d_2') AND (time >= 1535558400000 and
   * time <= 1535558650000) GROUP BY device.
   *
   * @param aggRangeQuery contains universal aggregation query with time filter parameters
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    int sensorNum = aggRangeQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder =
        getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(), aggRangeQuery.getAggFun());
    addWhereTimeClause(builder, aggRangeQuery);
    builder.append("GROUP BY device");
    return executeQueryAndGetStatus(builder.toString(), sensorNum, Operation.AGG_RANGE_QUERY);
  }

  /**
   * eg. SELECT time, count(s_2) FROM tutorial WHERE (device='d_2') AND (s_2>10) GROUP BY device.
   *
   * @param aggValueQuery contains universal aggregation query with value filter parameters
   */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    int sensorNum = aggValueQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder =
        getAggQuerySqlHead(aggValueQuery.getDeviceSchema(), aggValueQuery.getAggFun());
    addWhereValueClause(
        aggValueQuery.getDeviceSchema(), builder, aggValueQuery.getValueThreshold());
    builder.append(" GROUP BY device");
    return executeQueryAndGetStatus(builder.toString(), sensorNum, Operation.AGG_VALUE_QUERY);
  }

  /**
   * eg. SELECT time, count(s_2) FROM tutorial WHERE (device='d_2') AND (time >= 1535558400000 and
   * time <= 1535558650000) AND (s_2>10) GROUP BY device.
   *
   * @param aggRangeValueQuery contains universal aggregation query with time and value filters
   *     parameters
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    int sensorNum = aggRangeValueQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder =
        getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(), aggRangeValueQuery.getAggFun());
    addWhereTimeClause(builder, aggRangeValueQuery);
    addWhereValueClause(
        aggRangeValueQuery.getDeviceSchema(), builder, aggRangeValueQuery.getValueThreshold());
    builder.append("GROUP BY device");
    return executeQueryAndGetStatus(builder.toString(), sensorNum, Operation.AGG_RANGE_VALUE_QUERY);
  }

  /**
   * eg. SELECT CAST((time / 5000) as signed) * 5000 AS sampleTime, device, count(s_2) FROM tutorial
   * WHERE (device='d_2') AND (time >= 1535558400000 and time <= 1535558650000) GROUP BY sampleTime,
   * device.
   *
   * @param groupByQuery contains universal group by query condition parameters
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    int sensorNum = groupByQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder =
        getGroupByQuerySqlHead(
            groupByQuery.getDeviceSchema(),
            groupByQuery.getAggFun(),
            groupByQuery.getGranularity());
    addWhereTimeClause(builder, groupByQuery);
    builder.append(" GROUP BY sampleTime");
    return executeQueryAndGetStatus(builder.toString(), sensorNum, Operation.GROUP_BY_QUERY);
  }

  /**
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') ORDER BY time DESC LIMIT 1. The
   * last and first commands do not use indexes, and instead perform a sequential scan through their
   * groups. They are primarily used for ordered selection within a GROUP BY aggregate, and not as
   * an alternative to an ORDER BY time DESC LIMIT 1 clause to find the latest value (which will use
   * indexes).
   *
   * @param latestPointQuery contains universal latest point query condition parameters
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    int sensorNum = latestPointQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getSampleQuerySqlHead(latestPointQuery.getDeviceSchema());
    builder.append("ORDER BY time DESC LIMIT 1");
    return executeQueryAndGetStatus(builder.toString(), sensorNum, Operation.LATEST_POINT_QUERY);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    int sensorNum = rangeQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getSampleQuerySqlHead(rangeQuery.getDeviceSchema());
    addWhereTimeClause(builder, rangeQuery);
    addOrderByClause(builder);
    return executeQueryAndGetStatus(
        builder.toString(), sensorNum, Operation.RANGE_QUERY_ORDER_BY_TIME_DESC);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    int sensorNum = valueRangeQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getSampleQuerySqlHead(valueRangeQuery.getDeviceSchema());
    addWhereTimeClause(builder, valueRangeQuery);
    addWhereValueClause(
        valueRangeQuery.getDeviceSchema(), builder, valueRangeQuery.getValueThreshold());
    addOrderByClause(builder);
    return executeQueryAndGetStatus(
        builder.toString(), sensorNum, Operation.VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC);
  }

  /**
   * Using in verification
   *
   * @param verificationQuery
   */
  @Override
  public Status verificationQuery(VerificationQuery verificationQuery) {
    DeviceSchema deviceSchema = verificationQuery.getDeviceSchema();
    List<DeviceSchema> deviceSchemas = new ArrayList<>();
    deviceSchemas.add(deviceSchema);

    List<Record> records = verificationQuery.getRecords();
    if (records == null || records.size() == 0) {
      return new Status(false);
    }

    StringBuilder sql = getSampleQuerySqlHead(deviceSchemas);
    Map<Long, List<Object>> recordMap = new HashMap<>();
    sql.append(" and (time = ").append(records.get(0).getTimestamp());
    recordMap.put(records.get(0).getTimestamp(), records.get(0).getRecordDataValue());
    for (int i = 1; i < records.size(); i++) {
      Record record = records.get(i);
      sql.append(" or time = ").append(record.getTimestamp());
      recordMap.put(record.getTimestamp(), record.getRecordDataValue());
    }
    sql.append(")");
    int point = 0;
    int line = 0;
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql.toString());
      while (resultSet.next()) {
        long timeStamp = resultSet.getLong(1);
        List<Object> values = recordMap.get(timeStamp);
        for (int i = 0; i < values.size(); i++) {
          String value = String.valueOf(resultSet.getObject(i + 2));
          String target = String.valueOf(values.get(i));
          if (!value.equals(target)) {
            LOGGER.error("Using SQL: " + sql + ",Expected:" + value + " but was: " + target);
          } else {
            point++;
          }
        }
        line++;
      }
    } catch (Exception e) {
      LOGGER.error("Query Error: " + sql);
      return new Status(false);
    }
    if (recordMap.size() != line) {
      LOGGER.error(
          "Using SQL: " + sql + ",Expected line:" + recordMap.size() + " but was: " + line);
    }
    return new Status(true, point);
  }

  @Override
  public Status deviceQuery(DeviceQuery deviceQuery) throws SQLException {
    DeviceSchema deviceSchema = deviceQuery.getDeviceSchema();
    List<DeviceSchema> deviceSchemas = new ArrayList<>();
    deviceSchemas.add(deviceSchema);
    StringBuilder sql = getSampleQuerySqlHead(deviceSchemas);
    sql.append(" AND (time >= ").append(deviceQuery.getStartTimestamp());
    sql.append(" AND time < ").append(deviceQuery.getEndTimestamp()).append(")");
    sql.append(" ORDER BY time DESC");
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("MySQL:" + sql);
    }
    List<List<Object>> result = new ArrayList<>();
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql.toString());
      int columnNumber = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        List<Object> line = new ArrayList<>();
        for (int i = 1; i <= columnNumber; i++) {
          line.add(resultSet.getObject(i));
        }
        result.add(line);
      }
    }
    return new Status(true, 0, sql.toString(), result);
  }

  @Override
  public DeviceSummary deviceSummary(DeviceQuery deviceQuery) throws SQLException, TsdbException {
    DeviceSchema deviceSchema = deviceQuery.getDeviceSchema();
    StringBuilder sql = new StringBuilder("select count(1)");
    sql.append(" FROM ").append(tableName);
    addDeviceCondition(sql, Arrays.asList(deviceSchema));
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(sql.toString());
    resultSet.next();
    int totalLineNumber = Integer.parseInt(resultSet.getString(1));

    sql = new StringBuilder("select min(time)");
    sql.append(" FROM ").append(tableName);
    addDeviceCondition(sql, Arrays.asList(deviceSchema));
    resultSet = statement.executeQuery(sql.toString());
    resultSet.next();
    long minTimeStamp = Long.parseLong(resultSet.getString(1));

    sql = new StringBuilder("select max(time)");
    sql.append(" FROM ").append(tableName);
    addDeviceCondition(sql, Arrays.asList(deviceSchema));
    resultSet = statement.executeQuery(sql.toString());
    resultSet.next();
    long maxTimeStamp = Long.parseLong(resultSet.getString(1));

    statement.close();
    return new DeviceSummary(deviceSchema.getDevice(), totalLineNumber, minTimeStamp, maxTimeStamp);
  }

  protected Status executeQueryAndGetStatus(String sql, int sensorNum, Operation operation) {
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.debug("{} the query SQL: {}", Thread.currentThread().getName(), sql);
    }
    List<List<Object>> records = new ArrayList<>();
    int line = 0;
    int queryResultPointNum = 0;
    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          line++;
          if (config.isIS_COMPARISON()) {
            List<Object> record = new ArrayList<>();
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
              switch (operation) {
                case AGG_RANGE_QUERY:
                case AGG_VALUE_QUERY:
                case AGG_RANGE_VALUE_QUERY:
                  if (i == 1) {
                    continue;
                  }
                  break;
                default:
                  break;
              }
              record.add(resultSet.getObject(i));
            }
            records.add(record);
          }
        }
      }
      queryResultPointNum = line * sensorNum * config.getQUERY_DEVICE_NUM();
      if (config.isIS_COMPARISON()) {
        return new Status(true, queryResultPointNum, sql, records);
      } else {
        return new Status(true, queryResultPointNum);
      }
    } catch (Exception e) {
      return new Status(false, queryResultPointNum, e, sql);
    }
  }

  /**
   * 创建查询语句--(带有聚合函数的查询) . SELECT device, avg(cpu) FROM metrics WHERE (device='d_1' OR device='d_2')
   */
  protected StringBuilder getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT device");
    addFunSensor(aggFun, builder, devices.get(0).getSensors());
    builder.append(" FROM ").append(tableName);
    addDeviceCondition(builder, devices);
    return builder;
  }

  /**
   * 创建查询语句--(带有GroupBy函数的查询) . SELECT CAST((time / 5) as signed) * 5 AS sampleTime, device,
   * avg(cpu) FROM metrics WHERE (device='d_1' OR device='d_2').
   */
  protected StringBuilder getGroupByQuerySqlHead(
      List<DeviceSchema> devices, String aggFun, long timeUnit) {
    StringBuilder builder = new StringBuilder();

    builder
        .append("SELECT CAST((time / ")
        .append(timeUnit)
        .append(") as signed) * ")
        .append(timeUnit)
        .append(" AS sampleTime");

    addFunSensor(aggFun, builder, devices.get(0).getSensors());

    builder.append(" FROM ").append(tableName);
    addDeviceCondition(builder, devices);
    return builder;
  }

  /** 创建查询语句--(不带有聚合函数的查询) . SELECT time, cpu FROM metrics WHERE (device='d_1' OR device='d_2'). */
  protected StringBuilder getSampleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT time");
    addFunSensor(null, builder, devices.get(0).getSensors());

    builder.append(" FROM ").append(tableName);

    addDeviceCondition(builder, devices);
    return builder;
  }

  protected void addFunSensor(String method, StringBuilder builder, List<Sensor> list) {
    if (method != null) {
      list.forEach(
          sensor ->
              builder.append(", ").append(method).append("(").append(sensor.getName()).append(")"));
    } else {
      list.forEach(sensor -> builder.append(", ").append(sensor.getName()));
    }
  }

  protected void addDeviceCondition(StringBuilder builder, List<DeviceSchema> devices) {
    builder.append(" WHERE (");
    for (DeviceSchema deviceSchema : devices) {
      builder.append("device='").append(deviceSchema.getDevice()).append("'").append(" OR ");
    }
    builder.delete(builder.length() - 4, builder.length());
    builder.append(")");
  }

  /**
   * add time filter for query statements.
   *
   * @param builder sql header
   * @param rangeQuery range query
   */
  protected static void addWhereTimeClause(StringBuilder builder, RangeQuery rangeQuery) {
    builder.append(" AND (time >= ").append(rangeQuery.getStartTimestamp());
    if (rangeQuery instanceof GroupByQuery) {
      builder.append(" and time < ").append(rangeQuery.getEndTimestamp()).append(") ");
    } else {
      builder.append(" and time <= ").append(rangeQuery.getEndTimestamp()).append(") ");
    }
  }

  /**
   * add value filter for query statements.
   *
   * @param devices query device schema
   * @param builder sql header
   * @param valueThreshold lower bound of query value filter
   */
  protected static void addWhereValueClause(
      List<DeviceSchema> devices, StringBuilder builder, double valueThreshold) {
    boolean first = true;
    for (Sensor sensor : devices.get(0).getSensors()) {
      if (first) {
        builder.append(" AND (").append(sensor.getName()).append(" > ").append(valueThreshold);
        first = false;
      } else {
        builder.append(" and ").append(sensor.getName()).append(" > ").append(valueThreshold);
      }
    }
    builder.append(")");
  }

  protected static void addOrderByClause(StringBuilder builder) {
    builder.append(" ORDER BY time DESC");
  }

  /**
   * -- Creating a regular SQL table example.
   *
   * <p>CREATE TABLE group_0 (time BIGINT NOT NULL, sGroup TEXT NOT NULL, device TEXT NOT NULL, s_0
   * DOUBLE PRECISION NULL, s_1 DOUBLE PRECISION NULL);
   *
   * @return create table SQL String
   */
  protected String getCreateTableSql(String tableName, List<Sensor> sensors) {
    StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
    sqlBuilder.append("time BIGINT NOT NULL, sGroup VARCHAR(50), device VARCHAR(50)");
    for (Map.Entry<String, String> pair : config.getDEVICE_TAGS().entrySet()) {
      sqlBuilder.append(", ").append(pair.getKey()).append(" VARCHAR(255)");
    }
    for (int i = 0; i < sensors.size(); i++) {
      sqlBuilder
          .append(", ")
          .append(sensors.get(i))
          .append(" ")
          .append(typeMap(sensors.get(i).getSensorType()))
          .append(" NULL ");
    }
    sqlBuilder.append(",UNIQUE (time, sGroup, device");
    for (Map.Entry<String, String> pair : config.getDEVICE_TAGS().entrySet()) {
      sqlBuilder.append(", ").append(pair.getKey());
    }
    sqlBuilder.append("));");
    return sqlBuilder.toString();
  }

  /**
   * eg.
   *
   * <p>INSERT INTO conditions(time, group, device, s_0, s_1) VALUES (1535558400000, 'group_0',
   * 'd_0', 70.0, 50.0);
   */
  protected String getInsertOneBatchSql(
      DeviceSchema deviceSchema, long timestamp, List<Object> values) {
    StringBuilder builder = new StringBuilder();
    List<Sensor> sensors = deviceSchema.getSensors();
    builder.append("insert into ").append(tableName).append("(time,sGroup,device");
    for (Map.Entry<String, String> pair : deviceSchema.getTags().entrySet()) {
      builder.append(",").append(pair.getKey());
    }
    for (Sensor sensor : sensors) {
      builder.append(",").append(sensor.getName());
    }
    builder.append(") values(");
    builder.append(timestamp);
    builder.append(",'").append(deviceSchema.getGroup()).append("'");
    builder.append(",'").append(deviceSchema.getDevice()).append("'");
    for (Map.Entry<String, String> pair : deviceSchema.getTags().entrySet()) {
      builder.append(",'").append(pair.getValue()).append(",'");
    }
    for (Object value : values) {
      builder.append(",'").append(value).append("'");
    }
    builder.append(") ON DUPLICATE KEY UPDATE ");
    builder
        .append(sensors.get(0).getName())
        .append("=VALUES(")
        .append(sensors.get(0).getName())
        .append(")");
    for (int i = 1; i < sensors.size(); i++) {
      builder
          .append(",")
          .append(sensors.get(i).getName())
          .append("=VALUES(")
          .append(sensors.get(i).getName())
          .append(")");
    }
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.debug("getInsertOneBatchSql: {}", builder);
    }
    return builder.toString();
  }

  @Override
  public String typeMap(SensorType iotdbSensorType) {
    switch (iotdbSensorType) {
      case BOOLEAN:
        return "BOOLEAN";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE PRECISION";
      case TEXT:
        return "TEXT";
      default:
        LOGGER.error(
            "Unsupported data sensorType {}, use default data sensorType: BINARY.",
            iotdbSensorType);
        return "TEXT";
    }
  }
}
