using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using System.Text.Json;

namespace OrderService.Messaging
{
    public class OutboxPublisherWorker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly RabbitMqPublisher _publisher;

        public OutboxPublisherWorker(IServiceScopeFactory scopeFactory, RabbitMqPublisher publisher)
        {
            _scopeFactory = scopeFactory;
            _publisher = publisher;
        }
        // This method is called when the background service starts. It runs in a loop until the service is stopped, checking for new outbox messages to publish.
        //ExecuteAsync() starts automatically when the application starts
        // Every 5 seconds, it will eventually check for unpublished outbox messages.
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            //stoppingToken.IsCancellationRequested becomes true when the application is shutting down
            /* What happens during each loop:
                1. Create scope
                2. Get DbContext
                3. Query messages
                4. Publish messages
                5. Save changes
                6. Dispose scope + DbContext
                7. Wait 5 seconds
                8. Next loop
            Create scope → get DbContext → find unpublished messages → publish → mark IsPublished=true → dispose scope/DbContext → wait 5 sec → repeat
             */
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using (var scope = _scopeFactory.CreateScope())
                    {
                        var dbContext =
                            scope.ServiceProvider.GetRequiredService<OrderDbContext>();

                        var unpublishedMessages = await dbContext.OutboxMessages
                            .Where(message => !message.IsPublished)
                            .Take(10)
                            .ToListAsync(stoppingToken);

                        foreach (var message in unpublishedMessages)
                        {
                            try
                            {
                                var payload = JsonSerializer.Deserialize<JsonElement>(message.Payload);

                                var eventToPublish = new
                                {
                                    MessageId = message.Id, // Table OutboxMessage.Id as MessageId
                                    Id = payload.GetProperty("Id").GetInt32(), //OrderId
                                    Product = payload.GetProperty("Product").GetString(),
                                    Quantity = payload.GetProperty("Quantity").GetInt32()
                                };

                                await _publisher.PublishOrderCreatedAsync(eventToPublish); // Publish the message to RabbitMQ

                                message.IsPublished = true; //IsPublished is set to true in DB after successful publishing to RabbitMQ
                                await dbContext.SaveChangesAsync(stoppingToken);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(
                                    $"Could not publish outbox message {message.Id}: {ex.Message}");
                            }
                        }
                    } // scope and OrderDbContext are disposed HERE
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Outbox worker error: {ex.Message}");
                }

                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }
}
