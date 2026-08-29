using InventoryService.Data;
using InventoryService.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace InventoryService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class InventoryController : ControllerBase
    {
        private readonly InventoryDbContext _context;

        public InventoryController(InventoryDbContext context)
        {
            _context = context;
        }

        // GET: api/<Inventory
        [HttpGet]
        public async Task<IActionResult> GetInventory()
        {
            var inventory = await _context.Inventories.ToListAsync();

            return Ok(inventory);
        }

        // POST api/<Inventory
        [HttpPost]
        public async Task<IActionResult> CreateInventory(Inventory inventory)
        {
            _context.Inventories.Add(inventory);
            await _context.SaveChangesAsync();

            return Ok(inventory);
        }
    }
}
