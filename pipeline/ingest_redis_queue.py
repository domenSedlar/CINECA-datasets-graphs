import redis
import json

class IngestRedisQueue:
    def __init__(self, queue_name='data_buffer', host='localhost', port=6379, db=0):
        self.queue_name = queue_name
        self.redis = redis.Redis(host=host, port=port, db=db)

    def push(self, data):
        """Push a dict (as JSON) onto the queue."""
        self.redis.lpush(self.queue_name, json.dumps(data))
        logger.info(f"Pushed data to queue: {json.dumps(data)}")

    def pop(self, timeout=0):
        """Pop a dict (from JSON) from the queue. Blocks if empty. Returns None if timeout and nothing is found."""
        item = self.redis.brpop(self.queue_name, timeout=timeout)
        if item:
            # item is a tuple (queue_name, data)
            return json.loads(item[1])
        return None 