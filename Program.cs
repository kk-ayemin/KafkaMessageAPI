using Confluent.Kafka;
using KafkaMessageAPI.Services;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("KafkaConfig"));

// Register Kafka services
// builder.Services.AddSingleton<KafkaProducerService>();
// builder.Services.AddSingleton<KafkaConsumerService>();
builder.Services.AddSingleton<KafkaProducerJSONService>();
builder.Services.AddHostedService<KafkaMessageProcessorService>();
builder.Services.AddHostedService<KafkaDLQConsumerService>();


// Add controllers and other services
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
