using Microsoft.EntityFrameworkCore;
using InventoryService.Data;
using InventoryService.Messaging;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Db connection
builder.Services.AddDbContext<InventoryDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("InventoryDbConStr")));
// End Db connection

//RabbitMQ consumer: Register the OrderCreatedConsumer as a hosted service
builder.Services.AddHostedService<OrderCreatedConsumer>();

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
app.MapGet("/health", () => Results.Ok("OK"));

app.Run();
