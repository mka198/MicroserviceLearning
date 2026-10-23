using InventoryService.Data;
using InventoryService.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Distributed;
using System.Text.Json;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace InventoryService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class InventoryController : ControllerBase
    {
        private readonly InventoryDbContext _context;
        private readonly IDistributedCache _cache;
        private const string InventoryCacheKey = "inventory:all";
        private readonly ILogger<InventoryController> _logger;

        public InventoryController(InventoryDbContext context, IDistributedCache cache, ILogger<InventoryController> logger)
        {
            _context = context;
            _cache = cache;
            _logger = logger;
        }

        // GET: api/<Inventory
        [HttpGet]
        public async Task<IActionResult> GetInventory()
        {
            Response.Headers["X-Pod-Name"] = Environment.MachineName;  // for load balance checking, to see which pod is serving the request

            // --- redis cache ---
            var cacheUnavailable = false;
            string? cachedJson = null;

            // Try Redis first. If it fails, continue to SQL.
            try
            {
                cachedJson = await _cache.GetStringAsync(InventoryCacheKey);
            }
            catch (Exception ex)
            {
                cacheUnavailable = true;
                _logger.LogWarning(ex, "Redis read failed. Reading inventory from SQL.");
            }

            if (cachedJson != null)
            {
                var cachedInventory =
                    JsonSerializer.Deserialize<List<Inventory>>(cachedJson);

                Response.Headers["X-Cache"] = "HIT"; // HIT: means read from cache. Cache hit.
                return Ok(cachedInventory);
            }

            // SQL remains the main data store.
            var inventory = await _context.Inventories
                .AsNoTracking()
                .ToListAsync();

            // Only try writing the cache if the Redis read succeeded.
            if (!cacheUnavailable)
            {
                try
                {
                    await _cache.SetStringAsync(
                        InventoryCacheKey,
                        JsonSerializer.Serialize(inventory),
                        new DistributedCacheEntryOptions
                        {
                            AbsoluteExpirationRelativeToNow =
                                TimeSpan.FromSeconds(30)
                        });
                }
                catch (Exception ex)
                {
                    cacheUnavailable = true;
                    _logger.LogWarning(ex, "Redis write failed. Returning SQL data.");
                }
            }

            Response.Headers["X-Cache"] = cacheUnavailable ? "BYPASS" : "MISS"; // MISS: means cache miss, therefore read from SQL
            //--- End redis cache ---

            return Ok(inventory);
        }

        // POST api/<Inventory
        [HttpPost]
        public async Task<IActionResult> CreateInventory(Inventory inventory)
        {
            _context.Inventories.Add(inventory);
            await _context.SaveChangesAsync();

            // SQL saved successfully. Cache failure must not fail this request.
            try
            {
                await _cache.RemoveAsync(InventoryCacheKey); // remove redis cache
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Inventory saved, but cache removal failed. InventoryId: {InventoryId}",
                    inventory.Id);
            }

            return Ok(inventory);
        }
    }
}
