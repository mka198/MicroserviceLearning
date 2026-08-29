namespace InventoryService.Models
{
    public class Inventory
    {
        public int Id { get; set; }
        public string Product { get; set; } = "";
        public int QuantityInStock { get; set; }
    }
}
