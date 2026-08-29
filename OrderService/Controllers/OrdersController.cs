using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Messaging;
using OrderService.Models;
using System.Text.Json;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace OrderService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrdersController : ControllerBase
    {
        private readonly OrderDbContext _context;        

        public OrdersController(OrderDbContext context)
        {
            _context = context;            
        }

        // GET: api/Orders
        [HttpGet]
        public async Task<IActionResult> GetOrders()
        {
            var orders = await _context.Orders.ToListAsync();

            return Ok(orders);
        }

        // POST: api/Orders
        [HttpPost]
        public async Task<IActionResult> CreateOrder(Order order)
        {
            // Start one database transaction.
            // Order + OutboxMessage must either BOTH succeed or BOTH fail.
            await using var transaction =
                await _context.Database.BeginTransactionAsync();

            try
            {
                order.Status = "Created";
                _context.Orders.Add(order);
                await _context.SaveChangesAsync();

                //--- for outbox pattern,the table has created for outbox messages. Usecase: Order has created, but the message is not published to RabbitMQ(RabbitMQ down/crashed). So, we can use this table(OutboxMessage) to store the message in the database and later publish it to RabbitMQ.
                var outboxMessage = new OutboxMessage
                {
                    Type = "OrderCreated",
                    Payload = JsonSerializer.Serialize(new
                    {
                        order.Id,
                        order.Product,
                        order.Quantity
                    }),
                    CreatedAt = DateTime.UtcNow,
                    IsPublished = false
                };
                _context.OutboxMessages.Add(outboxMessage);
                await _context.SaveChangesAsync();
                //-------- end outbox pattern

                //--------- for rabbitmq ---------
                // Publish the order created event to RabbitMQ. It will place the message in the "order-created" Queue, which is later consumed by the consumer/ ex: InventoryService.
                // OrderService publish → RabbitMQ → "order-created" queue → message waiting...
                //await _publisher.PublishOrderCreatedAsync(new
                //{
                //    order.Id,
                //    order.Product,
                //    order.Quantity
                //});
                //--------- end for rabbitmq ---------
                await transaction.CommitAsync();
                return Ok(order);
            }
            catch
            {
                // Something failed.
                // Undo both the Order and OutboxMessage changes.
                await transaction.RollbackAsync();

                throw;
            }

        }
    }
}
