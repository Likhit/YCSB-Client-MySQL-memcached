package com.yahoo.ycsb.db;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

public class JdbcMemcachedClient extends DB {

  private final JdbcDBClient dbClient = new JdbcDBClient();
  private final MemcachedClient memcachedClient = new MemcachedClient();

  @Override
  public void init() throws DBException {
    dbClient.setProperties(getProperties());
    memcachedClient.setProperties(getProperties());
    dbClient.init();
    memcachedClient.init();
  }

  @Override
  public void cleanup() throws DBException {
    dbClient.cleanup();
    memcachedClient.cleanup();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    key = key.substring(4);
    Status status = memcachedClient.read(table, key, fields, result);
    if (Status.OK.equals(status)) {
      return status;
    }
    status = dbClient.read(table, key, fields, result);
    if (Status.OK.equals(status)) {
      memcachedClient.insert(table, key, result);
    }
    return status;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    this.memcachedClient.update(table, key, values);
    return dbClient.update(table, key, values);
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    this.memcachedClient.insert(table, key, values);
    return dbClient.insert(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    this.memcachedClient.delete(table, key);
    return dbClient.delete(table, key);
  }

}