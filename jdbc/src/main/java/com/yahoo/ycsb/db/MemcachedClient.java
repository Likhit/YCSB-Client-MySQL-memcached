/**
 * Copyright (c) 2014-2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Empty;

import edu.usc.cs550.rejig.client.ErrorHandler;
// We also use `edu.usc.cs550.rejig.clientMemcachedClient`; it is
// not imported explicitly and referred to with its full path to
// avoid conflicts with the class of the same name in this file.
import edu.usc.cs550.rejig.client.configreader.RejigConfigReader;
import edu.usc.cs550.rejig.client.SockIOPool;
import edu.usc.cs550.rejig.interfaces.Fragment;
import edu.usc.cs550.rejig.interfaces.RejigConfig;
import edu.usc.cs550.rejig.interfaces.RejigReaderGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import org.apache.log4j.Logger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Concrete Memcached client implementation.
 */
public class MemcachedClient extends DB {

  private final Logger logger = Logger.getLogger(getClass());

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private int objectExpirationTime;

  private String coordinatorHost;

  private int coordinatorPort;

  private GrpcConfigReader configReader;

  public static final String OBJECT_EXPIRATION_TIME_PROPERTY = "memcached.objectExpirationTime";
  public static final String DEFAULT_OBJECT_EXPIRATION_TIME = String.valueOf(0);

  public static final String COORDINATOR_HOST = "coordinator.host";

  public static final String COORDINATOR_PORT = "coordinator.port";

  /**
   * The MemcachedClient implementation that will be used to communicate
   * with the memcached server.
   */
  private edu.usc.cs550.rejig.client.MemcachedClient client;

  /**
   * @returns Underlying Memcached protocol client, implemented by
   *     SpyMemcached.
   */
  protected edu.usc.cs550.rejig.client.MemcachedClient memcachedClient() {
    return client;
  }

  @Override
  public void init() throws DBException {
    try {
      client = createMemcachedClient();
      objectExpirationTime = Integer.parseInt(getProperties()
        .getProperty(OBJECT_EXPIRATION_TIME_PROPERTY, DEFAULT_OBJECT_EXPIRATION_TIME)
      );
      coordinatorHost = getProperties().getProperty(COORDINATOR_HOST);
      coordinatorPort = Integer.parseInt(getProperties()
        .getProperty(COORDINATOR_PORT));
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  protected edu.usc.cs550.rejig.client.MemcachedClient createMemcachedClient()
      throws Exception {
    configReader = new GrpcConfigReader(coordinatorHost, coordinatorPort);
    //configReader.setConfig(config);

    SockIOPool.SockIOPoolOptions options = new SockIOPool
      .SockIOPoolOptions();
    options.initConn = 10;
    options.minConn = 5;
    options.maxConn = 25;
    options.maintSleep = 20;
    options.nagle = true;
    options.aliveCheck = true;

    String uuid = UUID.randomUUID().toString();
    ClientErrorHandler errorHandler = new ClientErrorHandler();
    edu.usc.cs550.rejig.client.MemcachedClient client =
      new edu.usc.cs550.rejig.client.MemcachedClient(
        null, errorHandler, uuid, configReader, options);
    return client;
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    key = createQualifiedKey(table, key);
    try {
      Object document = memcachedClient().get(key);
      if (document != null) {
        fromJson((String) document, fields, result);
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error encountered for key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result){
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(
      String table, String key, HashMap<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      Date exp = new Date(System.currentTimeMillis() + objectExpirationTime);
      boolean success = memcachedClient()
        .replace(key, toJson(values), exp);
      if (success) {
        return Status.OK;
      }
      return Status.ERROR;
    } catch (StatusException e) {
      return e.status();
    } catch (Exception e) {
      logger.error("Error updating value with key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(
      String table, String key, HashMap<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      Date exp = new Date(System.currentTimeMillis() + objectExpirationTime);
      boolean success = memcachedClient()
        .add(key, toJson(values), exp);
      if (success) {
        return Status.OK;
      }
      return Status.ERROR;
    } catch (StatusException e) {
      return e.status();
    } catch (Exception e) {
      logger.error("Error inserting value", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    key = createQualifiedKey(table, key);
    try {
       boolean success = memcachedClient()
        .delete(key);
      if (success) {
        return Status.OK;
      }
      return Status.ERROR;
    } catch (StatusException e) {
      return e.status();
    } catch (Exception e) {
      logger.error("Error deleting value", e);
      return Status.ERROR;
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (client != null) {
      configReader.shutDown();
      memcachedClient().shutDown();
    }
  }

  protected static String createQualifiedKey(String table, String key) {
    return MessageFormat.format("{0}-{1}", table, key);
  }

  protected static void fromJson(
      String value, Set<String> fields,
      Map<String, ByteIterator> result) throws IOException {
    JsonNode json = MAPPER.readTree(value);
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields();
         jsonFields.hasNext();
         /* increment in loop body */) {
      Map.Entry<String, JsonNode> jsonField = jsonFields.next();
      String name = jsonField.getKey();
      if (checkFields && !fields.contains(name)) {
        continue;
      }
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.asText()));
      }
    }
  }

  protected static String toJson(Map<String, ByteIterator> values)
      throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
    MAPPER.writeTree(jsonGenerator, node);
    return writer.toString();
  }
}

class GrpcConfigReader implements RejigConfigReader {
  private RejigConfig config;

  private ManagedChannel channel;

  private RejigReaderGrpc.RejigReaderBlockingStub blockingStub;

  public GrpcConfigReader(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port)
      .usePlaintext()
      .build()
    );
  }

  GrpcConfigReader(ManagedChannel channel) {
    this.channel = channel;
    blockingStub = RejigReaderGrpc.newBlockingStub(channel);
  }

  public void shutDown() throws DBException {
    try {
      if (channel != null) {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        channel = null;
      }
    } catch (InterruptedException e) {
      throw new DBException(e);
    }
  }

  @Override
  public RejigConfig getConfig() {
    RejigConfig config;
    try {
      config = blockingStub.getConfig(Empty.getDefaultInstance());
    } catch (StatusRuntimeException e) {
      throw new RuntimeException(e);
    }
    return config;
  }

  public void setConfig(RejigConfig config) {
    this.config = config;
  }
}

class ClientErrorHandler implements ErrorHandler {
  @Override
  public void handleErrorOnInit(
    final edu.usc.cs550.rejig.client.MemcachedClient client,
    final Throwable error) {
      throw new StatusException(
        new Status("INIT_ERROR", error.getMessage()));
  }

  @Override
  public void handleErrorOnGet(
    final edu.usc.cs550.rejig.client.MemcachedClient client,
    final Throwable error,
    final String cacheKey) {
      throw new StatusException(
        new Status("GET_ERROR", error.getMessage()));
  }

  @Override
  public void handleErrorOnGet(
    final edu.usc.cs550.rejig.client.MemcachedClient client,
    final Throwable error,
    final String[] cacheKeys) {
      throw new StatusException(
        new Status("MULTI_GET_ERROR", error.getMessage()));
  }

  @Override
  public void handleErrorOnSet(
    final edu.usc.cs550.rejig.client.MemcachedClient client,
    final Throwable error,
    final String cacheKey) {
      throw new StatusException(
        new Status("SET_ERROR", error.getMessage()));
  }

  @Override
  public void handleErrorOnDelete(
    final edu.usc.cs550.rejig.client.MemcachedClient client,
    final Throwable error,
    final String cacheKey) {
      throw new StatusException(
        new Status("DELETE_ERROR", error.getMessage()));
  }

  @Override
  public void handleErrorOnFlush(
    final edu.usc.cs550.rejig.client.MemcachedClient client,
    final Throwable error) {
      throw new StatusException(
        new Status("FLUSH_ERROR", error.getMessage()));
  }

  @Override
  public void handleErrorOnStats(
    final edu.usc.cs550.rejig.client.MemcachedClient client,
    final Throwable error) {
      throw new StatusException(
        new Status("STATS_ERROR", error.getMessage()));
  }

  @Override
  public void handleErrorOnConf(
    final edu.usc.cs550.rejig.client.MemcachedClient client,
    final Throwable error) {
      throw new StatusException(
        new Status("CONF_ERROR", error.getMessage()));
  }

  @Override
  public void handleErrorOnGrantLease(
    final edu.usc.cs550.rejig.client.MemcachedClient client,
    final Throwable error) {
      throw new StatusException(
        new Status("GRANT_ERROR", error.getMessage()));
  }

  @Override
  public void handleErrorOnRevokeLease(
    final edu.usc.cs550.rejig.client.MemcachedClient client,
    final Throwable error) {
      throw new StatusException(
        new Status("REVOKE_ERROR", error.getMessage()));
  }

  @Override
  public void handleErrorOnRefreshAndRetry(
    final edu.usc.cs550.rejig.client.MemcachedClient client,
    final Throwable error) {
      throw new StatusException(
        new Status("REFRESH_AND_RETRY_ERROR", error.getMessage()));
    }
}

class StatusException extends RuntimeException {
  private Status status;

  public StatusException(Status status) {
    this.status = status;
  }

  public Status status() {
    return status;
  }
}