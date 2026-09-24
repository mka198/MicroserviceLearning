using Microsoft.EntityFrameworkCore;
using InventoryService.Data;
using InventoryService.Messaging;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using InventoryService.HealthChecks;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Db connection
builder.Services.AddDbContext<InventoryDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("InventoryDbConStr")));
// End Db connection

//RabbitMQ consumer: Register the OrderCreatedConsumer as a hosted service
builder.Services.AddHostedService<OrderCreatedConsumer>();

// Register health checks.
// READINESS:
// Checks whether InventoryService is ready to do its work.
// Run dependency checks tagged with "ready",
// currently dependency: SQL Server and RabbitMQ.
builder.Services.AddHealthChecks()
    .AddDbContextCheck<InventoryDbContext>(
        name: "sqlserver",
        tags: new[] { "ready" })
    .AddCheck<RabbitMqHealthCheck>(
        name: "rabbitmq",
        tags: new[] { "ready" });

builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();
//app.MapGet("/health", () => Results.Ok("OK"));

// LIVENESS:
// Checks whether InventoryService itself is alive.
// Do not run dependency checks such as SQL Server or RabbitMQ.
app.MapHealthChecks("/health/live", new HealthCheckOptions
{
    Predicate = _ => false
});

// READINESS:
// Checks whether InventoryService is ready to do its work.
// Only run health checks tagged with "ready".
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = healthCheck => healthCheck.Tags.Contains("ready")
});

app.Run();
