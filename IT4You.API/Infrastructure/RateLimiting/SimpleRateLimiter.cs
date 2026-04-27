using Microsoft.Extensions.Caching.Memory;

namespace IT4You.API.Infrastructure.RateLimiting;

public sealed class SimpleRateLimiter
{
    private readonly IMemoryCache _cache;
    private static readonly object LockObj = new();

    public SimpleRateLimiter(IMemoryCache cache)
    {
        _cache = cache;
    }

    public bool TryConsume(string key, int limit, TimeSpan window, out int retryAfterSeconds)
    {
        retryAfterSeconds = 0;
        var now = DateTimeOffset.UtcNow;

        lock (LockObj)
        {
            if (!_cache.TryGetValue<RateLimitEntry>(key, out var entry) || entry is null)
            {
                entry = new RateLimitEntry(now, 0);
            }

            var elapsed = now - entry.WindowStart;
            if (elapsed >= window)
            {
                entry = new RateLimitEntry(now, 0);
                elapsed = TimeSpan.Zero;
            }

            if (entry.Count >= limit)
            {
                retryAfterSeconds = Math.Max(1, (int)Math.Ceiling((window - elapsed).TotalSeconds));
                return false;
            }

            entry = entry with { Count = entry.Count + 1 };
            _cache.Set(key, entry, new MemoryCacheEntryOptions
            {
                AbsoluteExpirationRelativeToNow = window
            });
            return true;
        }
    }

    private sealed record RateLimitEntry(DateTimeOffset WindowStart, int Count);
}
