package com.mh.mongo.core;

import java.util.ArrayList;
import java.util.List;

import com.mh.mongo.config.MongoConfig;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

public class MongoManager {

	private MongoClient mongo;
	private MongoDatabase db;

	public void initConfig(MongoConfig config) {
		int connectionsPerHost = config.getConnectionsPerHost();
		int threadsAllowedToBlockForConnectionMultiplier = config.getThreadsAllowedToBlockForConnectionMultiplier();
		MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(connectionsPerHost)
				.threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier).build();

		String host = config.getHost();
		int port = config.getPort();
		ServerAddress serverAddress = new ServerAddress(host, port);
		List<ServerAddress> addrs = new ArrayList<ServerAddress>();
		addrs.add(serverAddress);

		String username = config.getUsername();
		String password = config.getPassword();
		String databaseName = config.getDbName();
		MongoCredential credential = MongoCredential.createScramSha1Credential(username, databaseName,
				password.toCharArray());
		List<MongoCredential> credentials = new ArrayList<MongoCredential>();
		credentials.add(credential);

		mongo = new MongoClient(addrs, credential, options);
		db = mongo.getDatabase(databaseName);
	}
	
	public void createCollection(String collectionName) {
		if (db != null) {
			db.createCollection(collectionName);
		}
	}

	public MongoClient getMongo() {
		return mongo;
	}

	public void setMongo(MongoClient mongo) {
		this.mongo = mongo;
	}

	public MongoDatabase getDb() {
		return db;
	}

	public void setDb(MongoDatabase db) {
		this.db = db;
	}

}
