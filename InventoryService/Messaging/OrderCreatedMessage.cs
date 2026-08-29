namespace InventoryService.Messaging
{
    // Dto for the OrderCreated event received from RabbitMQ.
    public class OrderCreatedMessage
    {
        public int Id { get; set; }
        public string Product { get; set; } = "";
        public int Quantity { get; set; }

        // OrderService projects table: Outboxmessage.id = MessageId
        // We will use this to detect duplicate RabbitMQ messages.
        public int MessageId { get; set; }
    }
}
