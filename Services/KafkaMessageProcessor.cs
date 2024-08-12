using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using Polly;
using System;
using System.Threading;
using System.Threading.Tasks;

public class KafkaMessageProcessorService : IHostedService
{
    private readonly string bootstrapServers = "192.168.2.131:9092";
    private readonly string topic = "sms.notifications";
    private readonly string groupId = "test_consumer_group";
    private readonly string deadLetterTopic = "topics.dlq";

    private IConsumer<string, string> _consumer;
    private IProducer<string, string> _producer;
    private CancellationTokenSource _cancellationTokenSource;

    public KafkaMessageProcessorService()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Task.Run(() => StartConsuming(_cancellationTokenSource.Token), cancellationToken);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cancellationTokenSource.Cancel();
        _consumer.Close();
        _producer.Dispose();
        return Task.CompletedTask;
    }

    private void StartConsuming(CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        Console.WriteLine($"Subscribed to topic {topic}. Waiting for messages...");

        var retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetry(4, retryAttempt =>
                TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (exception, timeSpan, retryCount, context) =>
                {
                    Console.WriteLine($"Retry {retryCount} encountered an error: {exception.Message}. Waiting {timeSpan} before next retry.");
                });

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var cr = _consumer.Consume(cancellationToken);
                Console.WriteLine($"Consumed event from topic {topic}: key = {cr.Message.Key}, value = {cr.Message.Value}");

                try
                {
                    retryPolicy.Execute(() =>
                    {
                        ProcessMessage(cr.Message.Key, cr.Message.Value);
                    });

                    _consumer.Commit(cr);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"All retry attempts failed. Sending message to DLQ: {ex.Message}");
                    SendToDLQ(cr.Message.Key, cr.Message.Value, topic, ex.Message);
                    _consumer.Commit(cr);
                }
            }
            catch (ConsumeException ex)
            {
                Console.WriteLine($"Error occurred: {ex.Message}");
            }
        }
    }

    private void ProcessMessage(string key, string value)
    {
        Console.WriteLine($"Processing message: key = {key}, value = {value}");

        var json = JObject.Parse(value);
        string message = json["Message"].ToString();
        int messageNumber = ExtractNumberFromMessage(message);

        if (messageNumber > 700)
        {
            throw new Exception($"Message number {messageNumber} exceeds the allowed limit of 700.");
        }

        Console.WriteLine($"Processed message: key = {key}, number = {messageNumber}");
    }

    private int ExtractNumberFromMessage(string message)
    {
        string[] parts = message.Split(' ');
        if (parts.Length == 3 && int.TryParse(parts[2], out int number))
        {
            return number;
        }

        throw new FormatException("Invalid message format.");
    }

    private void SendToDLQ(string key, string value, string topic, string errorMessage)
    {
        var json = JObject.Parse(value);
        json["Topic"] = topic;
        json["ErrorMessage"] = errorMessage;
        var updatedValue = json.ToString();

        var message = new Message<string, string> { Key = key, Value = updatedValue };

        _producer.Produce(deadLetterTopic, message, (deliveryReport) =>
        {
            if (deliveryReport.Status == PersistenceStatus.Persisted)
            {
                Console.WriteLine($"Message sent to DLQ: key = {key}, value = {updatedValue}");
            }
            else
            {
                Console.WriteLine($"Failed to send message to DLQ: key = {key}, value = {updatedValue}");
            }
        });

        _producer.Flush(TimeSpan.FromSeconds(10));
    }
}
