using System.Diagnostics;
using System.Net.Sockets;
using StackExchange.Redis;

namespace Redis.Docker.Web;

public class TestSuite
{
    private readonly RedisClient _redisClient;
    private readonly int _numKeys;
    private readonly Random _random = new();
    private readonly List<string> _keys = new();
    private readonly List<string> _values = new()
    {
        @"[{""index"":0,""guid"":""b6127d45-f55f-43c7-bb9a-af0873e25809"",""isActive"":true,""name"":""Jasmine Owens"",""company"":""MINGA"",""phone"":""+1 (898) 469-3816"",""address"":""536 Glendale Court, Boykin, American Samoa, 8427"",""registered"":""2015-02-15T02:49:33 -01:00"",""latitude"":86.480106,""longitude"":-18.782087},{""index"":1,""guid"":""d563da81-8889-4283-a7dc-d95c27df28c5"",""isActive"":true,""name"":""Marianne Cross"",""company"":""EGYPTO"",""phone"":""+1 (981) 424-2405"",""address"":""372 Fleet Place, Venice, South Carolina, 3387"",""registered"":""2017-09-05T07:29:26 -02:00"",""latitude"":29.198647,""longitude"":5.012781},{""index"":2,""guid"":""22ea0843-f669-4007-83cd-fa5ad64f3698"",""isActive"":false,""name"":""Odessa Dennis"",""company"":""KRAGGLE"",""phone"":""+1 (800) 460-2250"",""address"":""310 Calyer Street, Madrid, North Carolina, 4627"",""registered"":""2018-06-25T11:44:31 -02:00"",""latitude"":61.039205,""longitude"":-38.654186},{""index"":3,""guid"":""99f4c070-8478-44aa-a246-1b8013036b37"",""isActive"":true,""name"":""Santos Griffin"",""company"":""BEDLAM"",""phone"":""+1 (821) 457-2136"",""address"":""807 Crystal Street, Albany, Idaho, 1688"",""registered"":""2017-07-09T02:53:30 -02:00"",""latitude"":88.173174,""longitude"":29.956758},{""index"":4,""guid"":""5e4efc71-259f-4d36-a345-b459636d257c"",""isActive"":true,""name"":""Mattie Burris"",""company"":""CUJO"",""phone"":""+1 (995) 535-2027"",""address"":""513 Devon Avenue, Chilton, Palau, 486"",""registered"":""2021-10-17T10:51:17 -02:00"",""latitude"":-79.157764,""longitude"":32.322699}]"
    };

    private readonly RedisValue[] _redisValues;
    private readonly RedisKey[] _redisKeys;
    private readonly Dictionary<int, RedisValue[]> _bucketsValues;
    private readonly Dictionary<int, RedisKey[]> _bucketsKeys;

    public TestSuite(RedisClient redisClient, int numKeys)
    {
        _redisClient = redisClient;
        _numKeys = numKeys;

        for (var i = 0; i < numKeys; i++)
        {
            _keys.Add("value_" + i+1);
        }

        _redisValues = _keys.Select(k => new RedisValue(k)).ToArray();
        _redisKeys = _keys.Select(k => new RedisKey(k)).ToArray();

        _bucketsValues = new Dictionary<int, RedisValue[]>();
        for (var i = 0; i < _keys.Count / 1000; i++)
        {
            if (!_bucketsValues.ContainsKey(i))
            {
                _bucketsValues.Add(i, new RedisValue[1000]);
                _bucketsValues[i] = _redisValues.Skip(i * 1000).Take(1000).ToArray();
            }
        }

        _bucketsKeys = new Dictionary<int, RedisKey[]>();
        for (var i = 0; i < _keys.Count / 1000; i++)
        {
            if (!_bucketsKeys.ContainsKey(i))
            {
                _bucketsKeys.Add(i, new RedisKey[1000]);
                _bucketsKeys[i] = _redisKeys.Skip(i * 1000).Take(1000).ToArray();
            }
        }
    }

    public async Task ExecuteAsync(int numThreads, bool useBuckets)
    {
        await Parallel.ForEachAsync(_keys, new ParallelOptions { MaxDegreeOfParallelism = 50 }, async (key, _) =>
        {
            var stopwatch = new Stopwatch();
            stopwatch.Restart();
            await _redisClient.SetAsync(key, _values[0], null);
            stopwatch.Stop();
        });

        for (var i = 0; i < numThreads; i++)
        {
            _ = Task.Run(Set);
            _ = Task.Run(Get);

            if (!useBuckets)
            {
                _ = Task.Run(() => AddToKeySet("value"));
                _ = Task.Run(() => GetFromKeySet("value"));
            }
            else
            {
                _ = Task.Run(() => AddToKeySetWithBuckets("keys"));
                _ = Task.Run(() => GetFromKeySetWithBuckets("keys"));
            }

            _ = Task.Run(() => Lock("lock_1"));
            _ = Task.Run(() => Lock("lock_2"));
            _ = Task.Run(() => Lock("lock_3"));
        }
    }

    private async Task Lock(string key)
    {
        var stopwatch = new Stopwatch();
        while (true)
        {
            stopwatch.Restart();
            _ = await _redisClient.ExecuteLocked(
                key,
                TimeSpan.FromMilliseconds(500),
                TimeSpan.FromMilliseconds(500),
                async () =>
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(500));
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                });
            Console.WriteLine($"    Lock |  Key: {key} | {stopwatch.ElapsedMilliseconds} ms");
        }
    }

    private async Task Set()
    {
        var stopwatch = new Stopwatch();
        while (true)
        {
            try
            {
                var index = _random.Next(_numKeys);
                stopwatch.Restart();
                await _redisClient.SetAsync(_keys[index], _values[0], null);
                stopwatch.Stop();
                Console.WriteLine($"  SetKey |  Key: {_keys[index]} | {stopwatch.ElapsedMilliseconds} ms");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    private async Task Get()
    {
        var stopwatch = new Stopwatch();
        while (true)
        {
            try
            {
                var index = _random.Next(_numKeys);
                stopwatch.Restart();
                await _redisClient.GetAsync(_keys[index]);
                stopwatch.Stop();
                Console.WriteLine($"  GetKey |  Key: {_keys[index]} | {stopwatch.ElapsedMilliseconds} ms");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    private async Task AddToKeySet(string keySet)
    {
        var stopwatch = new Stopwatch();
        while (true)
        {
            try
            {
                stopwatch.Restart();
                await _redisClient.AddToKeySetAsync(keySet, _redisValues);
                stopwatch.Stop();
                Console.WriteLine($" AddKeys | Keys: {_keys.Count} | {stopwatch.ElapsedMilliseconds} ms");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    private async Task GetFromKeySet(string keySet)
    {
        var stopwatch = new Stopwatch();
        while (true)
        {
            try
            {
                stopwatch.Restart();
                var keys = await _redisClient.FindKeysAsync(keySet);
                stopwatch.Stop();
                Console.WriteLine($"FindKeys | Keys: {keys.Length} | {stopwatch.ElapsedMilliseconds} ms");
                stopwatch.Restart();
                await _redisClient.GetAsync(_redisKeys);
                Console.WriteLine($" GetKeys | Keys: {_redisKeys.Length} | {stopwatch.ElapsedMilliseconds} ms");
                stopwatch.Stop();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    private async Task AddToKeySetWithBuckets(string keySet)
    {
        while (true)
        {
            try
            {
                await Parallel.ForEachAsync(_bucketsValues, new ParallelOptions { MaxDegreeOfParallelism = 10 },
                    async (bucket, _) =>
                    {
                        var stopwatch = new Stopwatch();
                        stopwatch.Restart();
                        await _redisClient.AddToKeySetAsync($"{keySet}_{bucket.Key}", bucket.Value);
                        stopwatch.Stop();
                        Console.WriteLine(
                            $" AddKeys | Keys: {bucket.Value.Length} | {stopwatch.ElapsedMilliseconds} ms");
                    });
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    private async Task GetFromKeySetWithBuckets(string keySet)
    {
        var stopwatch = new Stopwatch();
        while (true)
        {
            try
            {
                await Parallel.ForEachAsync(_bucketsKeys, new ParallelOptions { MaxDegreeOfParallelism = 10 },
                    async (bucket, _) =>
                    {
                        stopwatch.Restart();
                        var keys = await _redisClient.FindKeysAsync($"{keySet}_{bucket.Key}");
                        stopwatch.Stop();
                        Console.WriteLine($"FindKeys | Keys: {keys.Length} | {stopwatch.ElapsedMilliseconds} ms");
                        stopwatch.Restart();
                        await _redisClient.GetAsync(bucket.Value);
                        Console.WriteLine($" GetKeys | Keys: {bucket.Value.Length} | {stopwatch.ElapsedMilliseconds} ms");
                        stopwatch.Stop();
                    });

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}