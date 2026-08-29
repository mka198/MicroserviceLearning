namespace OrderService.Models
{
    //Outbox Pattern: if RabbitMQ is temporarily down, and order created in DB. It will stay in the Order database and can be published later when RabbitMQ comes back
    public class OutboxMessage
    {
        public int Id { get; set; }
        public string Type { get; set; } = "";
        public string Payload { get; set; } = "";
        public DateTime CreatedAt { get; set; }
        public bool IsPublished { get; set; }
    }
}
