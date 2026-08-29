using InventoryService.Models;
using Microsoft.EntityFrameworkCore;

namespace InventoryService.Data
{
    public class InventoryDbContext : DbContext
    {
        public InventoryDbContext(DbContextOptions<InventoryDbContext> options) : base(options)
        {
        }
        public DbSet<Inventory> Inventories { get; set; } = null!;
        public DbSet<ProcessedMessage> ProcessedMessages { get; set; } = null!;

        // This tells SQL Server: ProcessedMessages.MessageId must be unique.
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ProcessedMessage>()
                .HasIndex(x => x.MessageId)
                .IsUnique();
        }
    }
}
