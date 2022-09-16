using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;

namespace Redis.Docker.Web;

public class RedisClient
{
    private readonly RedLockFactory _redisLockFactory;
    private readonly IDatabase _redisDatabase;

    public RedisClient(string connectionString)
    {
        var connectionMultiplexer = ConnectionMultiplexer.Connect(connectionString);
        _redisDatabase = connectionMultiplexer.GetDatabase();
        var multiplexers = new List<RedLockMultiplexer>
        {
            connectionMultiplexer
        };
        _redisLockFactory = RedLockFactory.Create(multiplexers);
    }

    public async Task<bool> ExecuteLocked(string key, TimeSpan expiration, TimeSpan wait, Func<Task> action)
    {
        await using var redLock = await _redisLockFactory.CreateLockAsync(key, expiration, wait, TimeSpan.FromSeconds(1));

        if (!redLock.IsAcquired)
        {
            return false;
        }

        await action();

        return true;
    }

    public async Task<string> GetAsync(RedisKey key)
    {
        var value = await _redisDatabase.StringGetAsync(key);
        return value.HasValue ? value.ToString() : string.Empty;
    }

    public async Task SetAsync(string key, string value, TimeSpan? absoluteExpiration)
    {
        await _redisDatabase.StringSetAsync(key, value, absoluteExpiration);
    }

    public async Task AddToKeySetAsync(string keyListKey, RedisValue[] keys)
    {
        await _redisDatabase.SetAddAsync(keyListKey, keys);
    }

    public async Task<RedisValue[]> FindKeysAsync(string keyListKey)
    {
        return await _redisDatabase.SetMembersAsync(keyListKey);
    }

    public async Task<RedisValue[]> GetAsync(RedisKey[] keys)
    {
        return await _redisDatabase.StringGetAsync(keys);
    }
}