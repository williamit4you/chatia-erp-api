using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace IT4You.Infrastructure.Services
{
    public class MonitoringWorker : BackgroundService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<MonitoringWorker> _logger;
        private readonly string _connectionString;

        public MonitoringWorker(IConfiguration configuration, ILogger<MonitoringWorker> logger)
        {
            _configuration = configuration;
            _logger = logger;
            _connectionString = _configuration.GetConnectionString("DefaultConnection") 
                ?? throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Scheduled Monitoring Service is starting.");

            var task10s = RunEvery(TimeSpan.FromSeconds(10), ExecuteFastJobs, stoppingToken);
            var task1m  = RunEvery(TimeSpan.FromMinutes(1), ExecuteSlowJobs, stoppingToken);

            await Task.WhenAll(task10s, task1m);
            
            _logger.LogInformation("Scheduled Monitoring Service is stopping.");
        }

        private async Task RunEvery(TimeSpan interval, Func<Task> job, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                var start = DateTime.UtcNow;

                try
                {
                    await job();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error executing monitoring job: {Message}", ex.Message);
                }

                var elapsed = DateTime.UtcNow - start;
                var delay = interval - elapsed;

                if (delay > TimeSpan.Zero)
                {
                    try
                    {
                        await Task.Delay(delay, token);
                    }
                    catch (TaskCanceledException)
                    {
                        // Ignore cancellation
                    }
                }
            }
        }

        private async Task ExecuteFastJobs()
        {
            await ExecuteProcedure("SP_SWIA_GerarAlertasViews");
        }

        private async Task ExecuteSlowJobs()
        {
            await ExecuteProcedure("SP_SWIA_GerarResumoViews");
            await ExecuteProcedure("SP_SWIA_ColetarMonitorViews");
        }

        private async Task ExecuteProcedure(string procName)
        {
            try
            {
                using var conn = new SqlConnection(_connectionString);
                await conn.OpenAsync();

                using var cmd = new SqlCommand(procName, conn)
                {
                    CommandType = CommandType.StoredProcedure
                };

                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calling procedure {ProcName}: {Message}", procName, ex.Message);
                throw; // Rethrow to be caught by RunEvery and logged with more context
            }
        }
    }
}
