namespace InventoryService.Models
{
    public class ProcessedMessage
    {
        public int Id { get; set; }
        public int MessageId { get; set; }
        public DateTime ProcessedAt { get; set; }
    }
}
