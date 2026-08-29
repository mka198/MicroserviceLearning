using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace OrderService.Messaging
{
    public class RabbitMqPublisher
    {
        public async Task PublishOrderCreatedAsync(object message)
        {
            // RabbitMQ running on localhost
            var factory = new ConnectionFactory
            {
                HostName = "localhost"
            };

            // Creates the network connection between OrderService and RabbitMQ.
            await using var connection = await factory.CreateConnectionAsync();
            // publisherConfirmationsEnabled true: meaning the publisher will wait for an acknowledgment from the broker that the message has been received and stored. This ensures that messages are not lost in transit. When I publish a message, I want confirmation that RabbitMQ accepted it.
            // publisherConfirmationTrackingEnabled true: meaning the publisher will track the confirmations of the messages it publishes. This allows the publisher to know which messages have been successfully confirmed by the broker. It tells the .NET RabbitMQ client to track those confirmations for us.
            var channelOptions = new CreateChannelOptions(publisherConfirmationsEnabled: true,publisherConfirmationTrackingEnabled: true);
            await using var channel = await connection.CreateChannelAsync(channelOptions);

            // Declare a queue named "order-created", where messages will wait.
            // The queue is durable, meaning it will survive even RabbitMQ/broker stop and restart.
            // The queue is not exclusive, meaning it can be accessed by other connections. Allow other services to use it. For us, InventoryService needs to consume from it, so false is correct.
            // The queue is not auto-deleted, meaning it won't be deleted when consumers disconnect/unsubscribes. order-created queue will remain available.
            //await channel.QueueDeclareAsync(
            //    queue: "order-created",
            //    durable: true,
            //    exclusive: false,
            //    autoDelete: false);           

            var arguments = new Dictionary<string, object?>
            {
                { "x-dead-letter-exchange", "" },
                { "x-dead-letter-routing-key", "order-created-dead-letter" }
            };

            // Declare a queue named "order-created", where messages will wait.
            // The queue is durable, meaning it will survive even RabbitMQ/broker stop and restart.
            // The queue is not exclusive, meaning it can be accessed by other connections. Allow other services to use it. For us, InventoryService needs to consume from it, so false is correct.
            // The queue is not auto-deleted, meaning it won't be deleted when consumers disconnect/unsubscribes. order-created queue will remain available.
            await channel.QueueDeclareAsync(
                queue: "order-created",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: arguments);

            var json = JsonSerializer.Serialize(message);
            var body = Encoding.UTF8.GetBytes(json);

            // Set the message properties to make it persistent, meaning it will be saved to disk and survive broker restarts.
            var properties = new BasicProperties
            {
                Persistent = true
            };

            // Publish the message to the queue.           
            // The exchange is empty, which means the default exchange is used.
            //mandatory false: meaning if the message cannot be routed to a queue, it will be dropped. If true, the message will be returned to the publisher if it cannot be routed.
            // basicProperties: the properties of the message, including the persistent property.
            // Send this message to the queue named "order-created".
            await channel.BasicPublishAsync(
                exchange: "",
                mandatory: false,
                basicProperties: properties,
                routingKey: "order-created",
                body: body);
            Console.WriteLine("RabbitMQ confirmed the published message.");
        }
    }
}
