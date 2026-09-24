using Microsoft.Extensions.Diagnostics.HealthChecks;
using RabbitMQ.Client;

namespace InventoryService.HealthChecks
{
    public class RabbitMqHealthCheck : IHealthCheck
    {
        private readonly IConfiguration _configuration;

        public RabbitMqHealthCheck(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(
            HealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var hostName = _configuration["RabbitMQ:HostName"];

                if (string.IsNullOrWhiteSpace(hostName))
                {
                    return HealthCheckResult.Unhealthy("RabbitMQ:HostName configuration is missing.");
                }

                var factory = new ConnectionFactory
                {
                    HostName = hostName
                };

                await using var connection =
                    await factory.CreateConnectionAsync(cancellationToken);

                return HealthCheckResult.Healthy("RabbitMQ connection successful.");
            }
            catch (Exception ex)
            {
                return HealthCheckResult.Unhealthy("RabbitMQ connection failed.", ex);
            }
        }
    }
}
