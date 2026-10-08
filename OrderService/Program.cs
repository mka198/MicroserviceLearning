using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Messaging;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers();
// Db connection
builder.Services.AddDbContext<OrderDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("OrderDbConStr")));
// End Db connection

// RabbitMQ publisher service registration
builder.Services.AddSingleton<RabbitMqPublisher>();

// AddHostedService: it tells ASP.NET When OrderService starts, automatically create this background worker and run ExecuteAsync()
builder.Services.AddHostedService<OutboxPublisherWorker>();

// Register health checks.
// OrderService readiness checks SQL Server.
// RabbitMQ is not included because the Outbox can safely store messages
// while RabbitMQ is temporarily unavailable.
builder.Services.AddHealthChecks()
    .AddDbContextCheck<OrderDbContext>(
        name: "sqlserver",
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
// LIVENESS:
// Confirms that OrderService itself is running.
app.MapHealthChecks("/health/live", new HealthCheckOptions
{
    Predicate = _ => false
});

// READINESS:
// Confirms that OrderService can connect to SQL Server.
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = healthCheck => healthCheck.Tags.Contains("ready")
});

app.Run();
