package example;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.net.UnknownHostException;
import java.util.List;

/**
 * User: alexsilva Date: 3/4/14 Time: 12:17 PM
 */
public enum WMMongoClient {
	INSTANCE;

	private final static String MONGO_HOST = "localhost";
	private final static int MONGO_PORT = 27017;
	private final static long WORKER_ID = 1L; // We might not care about this value
	private final static long DATACENTER_ID = 1L; // We might not care about this value
	private final static long MIN_ID_THRESHOLD = 10;
	private final static long MAX_ID_THRESHOLD = 20;
	private final static String USER_AGENT = "WMMONGOCLIENT"; // We might not care about this value
	private final static String DB_NAME = "mydb";
	private final static String IDS = "ids";

	private MongoClient mongoClient;
	private IdWorker idWorker;
	private DB db;

	private WMMongoClient() {
		try {
			mongoClient = new MongoClient( MONGO_HOST , MONGO_PORT );
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		db = mongoClient.getDB( DB_NAME );
		idWorker = new IdWorker(WORKER_ID, DATACENTER_ID);

		DBCollection ids = db.getCollection(IDS);
		seedDatabase(ids, MAX_ID_THRESHOLD);
	}

	public long getId() {
		DBCollection ids = db.getCollection(IDS);
		DBObject id = ids.findAndRemove(new BasicDBObject());
		seedDatabase(ids, MIN_ID_THRESHOLD);
		System.out.println("Id count in db: " + ids.getCount());
		return (Long)id.get("id");
	}

	private void seedDatabase(DBCollection ids, long threshold) {
		if (ids.getCount() < threshold) {
			System.out.println(ids.getCount() + " ids in collection. Reseeding.");
			ids.insert(getNewIdsFromIdWorker(MAX_ID_THRESHOLD - ids.getCount()));
		}
	}

	private List<DBObject> getNewIdsFromIdWorker(long amount) {
		List<DBObject> newIds = Lists.newArrayList();
		for (int i = 0; i < amount; i++) {
			newIds.add(new BasicDBObject("id", idWorker.getId(USER_AGENT)));
		}
		return newIds;
	}

}
