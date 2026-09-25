using InventoryService.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Data.SqlClient;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using InventoryService.Models;

namespace InventoryService.Messaging
{
    /// <summary>
    /// Background service that listens for OrderCreated messages from RabbitMQ.    
    /// OrderService = Producer
    /// RabbitMQ     = Message Broker
    /// InventoryService = Consumer    
    /// Flow: OrderService -> RabbitMQ -> "order-created" queue -> InventoryService
    /// </summary>
    public class OrderCreatedConsumer : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IConfiguration _configuration;
        private readonly ILogger<OrderCreatedConsumer> _logger;
        private const int RetryDelaySeconds = 5; // Wait 5 seconds before trying to connect to RabbitMQ again.
        public OrderCreatedConsumer(IServiceScopeFactory scopeFactory, IConfiguration configuration, ILogger<OrderCreatedConsumer> logger)
        {
            _scopeFactory = scopeFactory;
            _configuration = configuration;
            _logger = logger;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Read the RabbitMQ host from configuration.
            // In Docker Compose, RabbitMQ__HostName is set to "rabbitmq",
            // which is the RabbitMQ service name in compose.yaml.
            // If the configuration is missing, fail instead of silently using a wrong host.
            var factory = new ConnectionFactory
            {
                HostName = _configuration["RabbitMQ:HostName"] ?? throw new InvalidOperationException("RabbitMQ:HostName configuration is missing.")
            };

            // Try to establish the initial connection to RabbitMQ.
            //
            // RabbitMQ automatic recovery helps if an EXISTING connection is lost,
            // but it does not solve the case where RabbitMQ is unavailable when
            // InventoryService starts.
            //
            // Therefore, keep trying until RabbitMQ becomes available
            // or InventoryService is asked to stop.
            IConnection? connection = null;

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    connection = await factory.CreateConnectionAsync();
                    _logger.LogInformation("Connected to RabbitMQ.");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "RabbitMQ is unavailable. Retrying in {RetryDelaySeconds} seconds.",
                        RetryDelaySeconds);

                    try
                    {
                        await Task.Delay(
                            TimeSpan.FromSeconds(RetryDelaySeconds),
                            stoppingToken);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        // InventoryService is shutting down normally.
                        return;
                    }
                }
            }

            if (connection is null)
            {
                return;
            }


            // Enable publisher confirms on this channel.
            //
            // InventoryService sometimes republishes a failed message to the retry queue.
            // Publisher confirms allow BasicPublishAsync() to wait for RabbitMQ to
            // confirm the publication before we ACK/remove the original message.
            var channelOptions = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
            var channel = await connection.CreateChannelAsync(channelOptions);

            await channel.QueueDeclareAsync(
                queue: "order-created-dead-letter",
                durable: true,
                exclusive: false,
                autoDelete: false);
            var arguments = new Dictionary<string, object?>
            {
                { "x-dead-letter-exchange", "" },
                { "x-dead-letter-routing-key", "order-created-dead-letter" }
            };

            // RETRY QUEUE:
            // Messages that fail because of a temporary problem can be placed here.
            //
            // x-message-ttl:
            // Keep the message in this retry queue for 10 seconds.
            //
            // x-dead-letter-exchange = "":
            // After 10 seconds, use RabbitMQ's default exchange.
            //
            // x-dead-letter-routing-key = "order-created":
            // Send the message back to the original order-created queue
            // so InventoryService can try processing it again.
            var retryQueueArguments = new Dictionary<string, object?>
            {
                { "x-message-ttl", 10000 },
                { "x-dead-letter-exchange", "" },
                { "x-dead-letter-routing-key", "order-created" }
            };

            await channel.QueueDeclareAsync(
                queue: "order-created-retry",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: retryQueueArguments);



            // Declare the queue to ensure it exists before we try to consume from it.
            // This is idempotent, so it won't create a new queue if it already exists. RabbitMQ will not create a duplicate queue.
            // queue: The name of the queue to declare.
            // durable: the queue should survive a RabbitMQ/broker restart.
            // exclusive: The queue is not exclusive, meaning it can be accessed by other connections. Allow other services to use it.
            // autoDelete: the queue will not be deleted when the last consumer unsubscribes/disconnect.
            await channel.QueueDeclareAsync(
                queue: "order-created",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: arguments);

            // Maximum number of times a message may be retried
            // for a temporary failure before it is sent to the dead-letter queue.
            const int maxRetryCount = 3;

            // CONSUMER:
            // Create a consumer that will listen for messages arriving in RabbitMQ.
            var consumer = new AsyncEventingBasicConsumer(channel);

            // MESSAGE RECEIVED:
            // RabbitMQ executes this code whenever a message is delivered
            // from the "order-created" queue to this consumer.
            consumer.ReceivedAsync += async (sender, args) =>
            {
                // Read how many times this message has already been retried.
                // A new message has no x-retry-count header, so it starts at 0.
                var retryCount = 0;

                if (args.BasicProperties.Headers is not null && args.BasicProperties.Headers.TryGetValue("x-retry-count", out var retryHeader) && retryHeader is int retryHeaderValue)
                {
                    retryCount = retryHeaderValue;
                }
                try
                {
                    var body = args.Body.ToArray(); //RabbitMQ sends the message body as bytes.
                    var message = Encoding.UTF8.GetString(body); // Convert bytes -> readable UTF-8 JSON/text.

                    var orderMessage = JsonSerializer.Deserialize<OrderCreatedMessage>(message);
                    if (orderMessage is null)
                    {
                        throw new Exception("Could not deserialize OrderCreated message.");
                    }

                    _logger.LogInformation(
                        "Received OrderCreated message. MessageId: {MessageId}, Product: {Product}, Quantity: {Quantity}",
                        orderMessage.MessageId,
                        orderMessage.Product,
                        orderMessage.Quantity);

                    using var scope = _scopeFactory.CreateScope();

                    var dbContext = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();

                    // ===== DUPLICATE CHECK =====
                    // stoppingToken parameter: Pass stoppingToken so EF Core can stop this database operation gracefully, if InventoryService is shutting down or the BackgroundService is being cancelled.
                    var alreadyProcessed = await dbContext.ProcessedMessages
                    .AnyAsync(x => x.MessageId == orderMessage.MessageId, stoppingToken);

                    if (alreadyProcessed)
                    {
                        _logger.LogInformation(
                            "Duplicate message ignored. MessageId: {MessageId}",
                            orderMessage.MessageId);

                        await channel.BasicAckAsync(
                            deliveryTag: args.DeliveryTag,
                            multiple: false);

                        return;
                    }
                    // ===== End DUPLICATE CHECK =====                    
                    var inventory = await dbContext.Inventories.FirstOrDefaultAsync(x => x.Product == orderMessage.Product, stoppingToken);

                    if (inventory is null)
                    {
                        throw new Exception(
                            $"Inventory not found for product: {orderMessage.Product}");
                    }

                    // Update the inventory quantity in stock based in InventoryDB.
                    // Save DB first, then RabbitMQ ACK second                  
                    // ===== SAVE INVENTORY + PROCESSED MESSAGE TOGETHER =====
                    await using var transaction = await dbContext.Database.BeginTransactionAsync(stoppingToken);

                    inventory.QuantityInStock -= orderMessage.Quantity;

                    dbContext.ProcessedMessages.Add(new ProcessedMessage
                    {
                        MessageId = orderMessage.MessageId,
                        ProcessedAt = DateTime.UtcNow
                    });

                    await dbContext.SaveChangesAsync(stoppingToken);

                    await transaction.CommitAsync();

                    // ===== END INVENTORY + PROCESSED MESSAGE TOGETHER =====


                    // ACKNOWLEDGEMENT (ACK):
                    // Tell RabbitMQ:
                    // "I successfully processed this message."                
                    // RabbitMQ can then remove the message from the queue.
                    await channel.BasicAckAsync(
                        deliveryTag: args.DeliveryTag,
                        multiple: false);

                    _logger.LogInformation(
                        "Inventory updated successfully. MessageId: {MessageId}, Product: {Product}, QuantityReduced: {QuantityReduced}",
                        orderMessage.MessageId,
                        orderMessage.Product,
                        orderMessage.Quantity);
                }
                catch (InvalidOperationException ex) when (ex.InnerException is SqlException)
                {
                    _logger.LogWarning(
                        ex,
                        "Temporary SQL error while processing message. RetryCount: {RetryCount}",
                        retryCount);

                    if (retryCount < maxRetryCount)
                    {
                        // Create properties for the new retry message.
                        var retryProperties = new BasicProperties
                        {
                            Persistent = true,
                            Headers = new Dictionary<string, object?>
                            {
                                { "x-retry-count", retryCount + 1 }
                            }
                        };
                        try
                        {

                            // Publish the same message body to the retry queue.
                            // The retry queue waits 10 seconds and then sends the message
                            // back to "order-created" for another processing attempt.
                            await channel.BasicPublishAsync(
                            exchange: "",
                            routingKey: "order-created-retry",
                            mandatory: false,
                            basicProperties: retryProperties,
                            body: args.Body);

                            // RabbitMQ confirmed the retry copy was accepted,
                            // so it is now safe to ACK/remove the original message.
                            await channel.BasicAckAsync(
                                deliveryTag: args.DeliveryTag,
                                multiple: false);

                            _logger.LogInformation($"Message sent to retry queue. Retry {retryCount + 1} of {maxRetryCount}.");
                        }
                        catch (Exception publishException)
                        {
                            _logger.LogError($"ERROR publishing message to retry queue: {publishException.Message}");

                            // IMPORTANT:
                            // Do not ACK the original message here.
                            // We could not confirm that the retry copy was safely published.
                        }
                    }
                    else
                    {
                        // Maximum retries reached.
                        // Reject the current message without requeueing it.
                        // The dead-letter configuration on "order-created"
                        // sends it to "order-created-dead-letter".
                        await channel.BasicNackAsync(
                            deliveryTag: args.DeliveryTag,
                            multiple: false,
                            requeue: false);

                        _logger.LogWarning($"Maximum retry count ({maxRetryCount}) reached. Message sent to DLQ.");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError($"ERROR processing message: {ex.Message}");

                    // NACK = processing failed.
                    // requeue: false means:
                    // "Do not put this message back into the original queue."
                    // Because the queue has dead-letter settings,
                    // RabbitMQ will route it to "order-created-dead-letter".
                    await channel.BasicNackAsync(
                        deliveryTag: args.DeliveryTag,
                        multiple: false,
                        requeue: false);
                }
            };

            // START CONSUMING:
            // Subscribe this consumer to the "order-created" queue.            
            // autoAck: false means RabbitMQ must wait for us to explicitly. acknowledge the message with BasicAckAsync().
            await channel.BasicConsumeAsync(
                queue: "order-created",
                autoAck: false,
                consumer: consumer);

            // Keep this BackgroundService alive while InventoryService is running.            
            // Without this wait, ExecuteAsync would finish and our RabbitMQ consumer would stop listening.            
            // stoppingToken is triggered when InventoryService shuts down.
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}
