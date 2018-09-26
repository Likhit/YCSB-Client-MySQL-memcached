package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

public class JdbcDBClientSelector extends DB {

	private final JdbcDBClient dbClient = new JdbcDBClient();
	private final MemcachedClient memcachedClient = new MemcachedClient();
	private MemcachedClient insertClient;
	private boolean useCache;
	private boolean asyncInsert;

	@Override
	public void init() throws DBException {
		System.out.println(getProperties());
		dbClient.setProperties(getProperties());
		memcachedClient.setProperties(getProperties());
		dbClient.init();
		memcachedClient.init();
		useCache = Boolean.parseBoolean(getProperties().getProperty("use_cache"));
		asyncInsert = Boolean.parseBoolean(getProperties().getProperty("async_insert"));
		System.out.println("use cache " + useCache);
		if (asyncInsert) {
			insertClient = new MemcachedClient();
			insertClient.setProperties(getProperties());
			insertClient.init();
		}
	}

	@Override
	public void cleanup() throws DBException {
		dbClient.cleanup();
		memcachedClient.cleanup();
	}

	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		key = key.substring(4);
		if (useCache) {
			Status status = memcachedClient.read(table, key, fields, result);
			if (Status.OK.equals(status)) {
				return status;
			}
			status = dbClient.read(table, key, fields, result);
			if (Status.OK.equals(status)) {
				if (asyncInsert) {
					insertClient.asyncInsert(table, key, result);
				} else {
					memcachedClient.insert(table, key, result);
				}
			}
			return status;
		}
		return dbClient.read(table, key, fields, result);
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		if (useCache) {
			return Status.NOT_IMPLEMENTED;
		}
		return dbClient.scan(table, startkey, recordcount, fields, result);
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
		key = key.substring(4);
		if (useCache) {
			this.memcachedClient.update(table, key, values);
		}
		return dbClient.update(table, key, values);
	}

	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		if (useCache) {
			this.memcachedClient.insert(table, key, values);
		}
		return dbClient.insert(table, key, values);
	}

	@Override
	public Status delete(String table, String key) {
		if (useCache) {
			this.memcachedClient.delete(table, key);
		}
		return dbClient.delete(table, key);
	}

}
