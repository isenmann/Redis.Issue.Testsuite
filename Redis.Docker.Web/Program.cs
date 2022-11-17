using Redis.Docker.Web;

const bool useBuckets = false;

ThreadPool.GetMinThreads(out var workerThreads, out var completionPortThreads);

Console.WriteLine($"Worker Threads = {workerThreads} - Completion Port Threads = {completionPortThreads}");

ThreadPool.SetMinThreads(workerThreads * 10, completionPortThreads);

ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);

Console.WriteLine($"Worker Threads = {workerThreads} - Completion Port Threads = {completionPortThreads}");

var builder = WebApplication.CreateBuilder(args);

var host = builder.Configuration.GetConnectionString("RedisHost");

var app = builder.Build();

app.MapGet("/health", async () =>
{
    await Task.Delay(TimeSpan.FromMilliseconds(50));
    return new List<string>();
});

var redisClient = new RedisClient(host);
var testSuite = new TestSuite(redisClient, 100000);

await testSuite.ExecuteAsync(3, useBuckets);

app.Run();