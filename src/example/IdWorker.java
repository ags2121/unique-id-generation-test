package example;

/**
 * User: alexsilva Date: 3/4/14 Time: 11:13 AM
 */
public class IdWorker {

	private final static long epoch = 1288834974657L;
	private long workerIdBits = 5L;
	private long datacenterIdBits = 5L;
	private long maxWorkerId = ~(-1L << workerIdBits);
	private long maxDatacenterId = ~(-1L << datacenterIdBits);
	private long sequenceBits = 12L;

	private long workerIdShift = sequenceBits;
	private long datacenterIdShift = sequenceBits + workerIdBits;
	private long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
	private long sequenceMask = ~(-1L << sequenceBits);

	private long lastTimestamp = -1L;
	private long workerId;
	private long datacenterId;
	private long sequence;

	public IdWorker(long workerId, long datacenterId, long sequence) {
		checkForBadConstructorParams();
		this.workerId = workerId;
		this.datacenterId = datacenterId;
		this.sequence = sequence;
	}

	public IdWorker(long workerId, long datacenterId) {
		checkForBadConstructorParams();
		this.workerId = workerId;
		this.datacenterId = datacenterId;
		this.sequence = 0L;
	}

	public long getId(String userAgent) {
		if (!isValidUseragent(userAgent)) {
			// TODO: log error
			throw new Error("InvalidUserAgentError");
		}

		long id = nextId();
		// TODO: log id creation
		return id;
	}

	protected long nextId() {
		Long timestamp = timeGen();

		if (timestamp < lastTimestamp) {
//			logger.error("clock is moving backwards.  Rejecting requests until %d.", lastTimestamp);
			throw new Error("Clock moved backwards.  Refusing to generate id for " + (lastTimestamp - timestamp) + " milliseconds");
		}

		if (lastTimestamp == timestamp) {
			sequence = (sequence + 1) & sequenceMask;
			if (sequence == 0) {
				timestamp = tilNextMillis(lastTimestamp);
			}
		} else {
			sequence = 0;
		}

		lastTimestamp = timestamp;
		return ((timestamp - epoch) << timestampLeftShift) |
			(datacenterId << datacenterIdShift) |
			(workerId << workerIdShift) |
			sequence;
	}

	protected long tilNextMillis(long lastTimestamp) {
		long timestamp = timeGen();
		while (timestamp <= lastTimestamp) {
			timestamp = timeGen();
		}
		return timestamp;
	}

	protected long timeGen() {
		return System.currentTimeMillis();
	}

	protected static boolean isValidUseragent(String userAgent) {
		// TODO: do we want to check for bad requests?
		return true;
	}

	protected void checkForBadConstructorParams() {
		if (workerId > maxWorkerId || workerId < 0) {
			// TODO: log exception
			throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
		}

		if (datacenterId > maxDatacenterId || datacenterId < 0) {
			// TODO: log exception
			throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", maxDatacenterId));
		}
	}
}
