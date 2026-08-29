using InventoryService.Data;
using Microsoft.EntityFrameworkCore;
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
        public OrderCreatedConsumer(IServiceScopeFactory scopeFactory)
        {
            _scopeFactory = scopeFactory;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // RabbitMQ running on localhost
            var factory = new ConnectionFactory
            {
                HostName = "localhost"
            };

            // Creates the network connection between InventoryService and RabbitMQ.
            var connection = await factory.CreateConnectionAsync();
            var channel = await connection.CreateChannelAsync();

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

            // CONSUMER:
            // Create a consumer that will listen for messages arriving in RabbitMQ.
            var consumer = new AsyncEventingBasicConsumer(channel);

            // MESSAGE RECEIVED:
            // RabbitMQ executes this code whenever a message is delivered
            // from the "order-created" queue to this consumer.
            consumer.ReceivedAsync += async (sender, args) =>
            {
                try
                {
                    var body = args.Body.ToArray(); //RabbitMQ sends the message body as bytes.
                    var message = Encoding.UTF8.GetString(body); // Convert bytes -> readable UTF-8 JSON/text.

                    Console.WriteLine($"Received message: {message}");


                    var orderMessage = JsonSerializer.Deserialize<OrderCreatedMessage>(message);
                    if (orderMessage is null)
                    { 
                        throw new Exception("Could not deserialize OrderCreated message.");
                    }

                    using var scope = _scopeFactory.CreateScope();

                    var dbContext =  scope.ServiceProvider.GetRequiredService<InventoryDbContext>();

                    // ===== DUPLICATE CHECK =====
                    // stoppingToken parameter: Pass stoppingToken so EF Core can stop this database operation gracefully, if InventoryService is shutting down or the BackgroundService is being cancelled.
                    var alreadyProcessed = await dbContext.ProcessedMessages
                    .AnyAsync(x => x.MessageId == orderMessage.MessageId, stoppingToken);

                    if (alreadyProcessed)
                    {
                        Console.WriteLine(
                            $"Duplicate message ignored. MessageId: {orderMessage.MessageId}");

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

                    Console.WriteLine($"SUCCESS: {orderMessage.Product} stock reduced by {orderMessage.Quantity}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ERROR processing message: {ex.Message}");

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
