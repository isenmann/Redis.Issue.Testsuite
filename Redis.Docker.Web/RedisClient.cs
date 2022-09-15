using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;

namespace Redis.Docker.Web;

public class RedisClient
{
    private readonly ConnectionMultiplexer _connectionMultiplexer;
    private readonly RedLockFactory _redisLockFactory;

    public RedisClient(string connectionString)
    {
        _connectionMultiplexer = ConnectionMultiplexer.Connect(connectionString);
        var multiplexers = new List<RedLockMultiplexer>
        {
            _connectionMultiplexer
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

    public async Task<string> GetAsync(string key)
    {
        var db = _connectionMultiplexer.GetDatabase();
        var value = await db.StringGetAsync(key);
        return value.HasValue ? value.ToString() : string.Empty;
    }

    public async Task SetAsync(string key, string value, TimeSpan? absoluteExpiration)
    {
        var db = _connectionMultiplexer.GetDatabase();
        await db.StringSetAsync(key, value, absoluteExpiration);
    }

    public async Task AddToKeySetAsync(string keyListKey, List<string> keys)
    {
        var db = _connectionMultiplexer.GetDatabase();
        await db.SetAddAsync(keyListKey, keys.Select(k => new RedisValue(k)).ToArray());
    }

    public async Task<List<string>> FindKeysAsync(string keyListKey)
    {
        var db = _connectionMultiplexer.GetDatabase();
        var keys = await db.SetMembersAsync(keyListKey);
        return keys.Select(x => x.ToString()).ToList();
    }

    public async Task<List<string>> GetAsync(List<string> keys)
    {
        var db = _connectionMultiplexer.GetDatabase();
        var values = await db.StringGetAsync(keys.Select(x => new RedisKey(x)).ToArray());
        return values.Select(x => x.ToString()).ToList();
    }
}