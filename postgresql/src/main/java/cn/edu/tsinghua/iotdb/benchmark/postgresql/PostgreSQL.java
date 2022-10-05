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

package cn.edu.tsinghua.iotdb.benchmark.postgresql;

import cn.edu.tsinghua.iotdb.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.timescaledb.TimescaleDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.DeviceQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

public class PostgreSQL extends TimescaleDB {

  protected static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQL.class);

  protected static final String POSTGRESQL_JDBC_NAME = "org.postgresql.Driver";
  protected static final String POSTGRESQL_URL = "jdbc:postgresql://%s:%s/%s";

  protected static final AtomicBoolean schemaInit = new AtomicBoolean(false);
  protected static final CyclicBarrier schemaBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());

  public PostgreSQL(DBConfig dbConfig) {
    super(dbConfig);
  }

  @Override
  public void init() throws TsdbException {
    try {
      Class.forName(POSTGRESQL_JDBC_NAME);
      // default username=postgres and password=postgres
      connection =
          DriverManager.getConnection(
              String.format(
                  POSTGRESQL_URL,
                  dbConfig.getHOST().get(0),
                  dbConfig.getPORT().get(0),
                  dbConfig.getDB_NAME()),
              dbConfig.getUSERNAME(),
              dbConfig.getPASSWORD());
    } catch (Exception e) {
      LOGGER.error("Initialize PostgreSQL failed because ", e);
      throw new TsdbException(e);
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
      LOGGER.error("Failed to close PostgreSQL connection because: {}", e.getMessage());
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
        String pgsql = getCreateTableSql(tableName, schemaList.get(0).getSensors());
        LOGGER.debug("CreateTableSQL Statement:  {}", pgsql);
        statement.execute(pgsql);
      } catch (SQLException e) {
        LOGGER.error("Can't create PG table because: {}", e.getMessage());
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

  /**
   * eg. SELECT cast((time/timeUnit) as bigint) * timeUnit AS sampleTime, device, count(s_2) FROM
   * tutorial WHERE (device='d_2') AND (time >= 1535558400000 and time <= 1535558650000) GROUP BY
   * time, device.
   *
   * @param groupByQuery contains universal group by query condition parameters
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    int sensorNum = groupByQuery.getDeviceSchema().get(0).getSensors().size();
    long offset = groupByQuery.getStartTimestamp() % groupByQuery.getGranularity();
    StringBuilder builder =
        getGroupByQuerySqlHead(
            groupByQuery.getDeviceSchema(),
            groupByQuery.getAggFun(),
            groupByQuery.getGranularity(),
            offset);
    addWhereTimeClause(builder, groupByQuery);
    builder.append(" GROUP BY sampleTime");
    return executeQueryAndGetStatus(builder.toString(), sensorNum, Operation.GROUP_BY_QUERY);
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
      LOGGER.info("PostgreSQL:" + sql);
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

  /**
   * 创建查询语句--(带有GroupBy函数的查询) . SELECT cast((time/timeUnit) as bigint) * timeUnit AS sampleTime,
   * device, avg(cpu) FROM metrics WHERE (device='d_1' OR device='d_2').
   */
  protected StringBuilder getGroupByQuerySqlHead(
      List<DeviceSchema> devices, String aggFun, long timeUnit, long offset) {
    StringBuilder builder = new StringBuilder();

    builder
        .append("SELECT CAST((time / ")
        .append(timeUnit)
        .append(") as bigint) * ")
        .append(timeUnit)
        .append(" AS sampleTime");

    addFunSensor(aggFun, builder, devices.get(0).getSensors());

    builder.append(" FROM ").append(tableName);
    addDeviceCondition(builder, devices);
    return builder;
  }
}
