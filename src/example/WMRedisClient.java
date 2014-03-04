package example;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.Set;

/**
 * User: alexsilva Date: 3/4/14 Time: 3:41 PM
 */
public enum WMRedisClient {
	INSTANCE;

	private final static String REDIS_HOST = "localhost";
	private final static int REDIS_PORT = 6379;
	private final static long WORKER_ID = 1L; // We might not care about this value
	private final static long DATACENTER_ID = 1L; // We might not care about this value
	private final static long MIN_ID_THRESHOLD = 10;
	private final static long MAX_ID_THRESHOLD = 20;
	private final static String USER_AGENT = "WMREDISCLIENT"; // We might not care about this value
	public final static String USER_ID_SET = "userid-set";
	public final static String COMPANY_ID_SET = "companyid-set";
	public final static String WORK_ID_SET = "companyid-set";
	public final static String[] ALL_SETS = new String[]{USER_ID_SET, COMPANY_ID_SET, WORK_ID_SET};

	private Jedis jedis;
	private IdWorker idWorker;

	private WMRedisClient() {
		jedis = new Jedis(REDIS_HOST, REDIS_PORT);
		jedis.connect();
		idWorker = new IdWorker(WORKER_ID, DATACENTER_ID);

		seedRedis(USER_ID_SET, MAX_ID_THRESHOLD);
	}

	public long getId(String setName) {
		seedRedis(setName, MIN_ID_THRESHOLD);
		String id = jedis.spop(setName);
		return Long.parseLong(id);
	}

	private void seedRedis(String setName, long threshold) {
		long setSize = jedis.scard(setName);
		if (setSize < threshold) {
			long amountToAdd = MAX_ID_THRESHOLD - setSize;
			System.out.printf("%d ids in %s. Reseeding %d ids\n", setSize, setName, amountToAdd);

			Pipeline p = jedis.pipelined();
			for (int i = 0; i < amountToAdd; i++) {
				p.sadd(setName, String.valueOf(idWorker.getId(USER_AGENT)));
			}
			Response<Set<String>> updatedSet = p.smembers(setName);
			p.sync();

			int updatedSetCount = updatedSet.get().size();
			if (updatedSetCount < MAX_ID_THRESHOLD) {
				// TODO: log exception
				System.out.printf("%d new ids were not added to redis\n", MAX_ID_THRESHOLD - updatedSetCount);
			}
			System.out.printf("%d entries are now in %s\n", jedis.scard(setName), setName);
		}
	}

}
