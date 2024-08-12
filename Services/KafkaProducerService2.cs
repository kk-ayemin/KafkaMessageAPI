using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Text.Json;
using System.Threading.Tasks;
using KafkaMessageAPI.Models;

namespace KafkaMessageAPI.Services
{
    public class KafkaProducerJSONService
    {
        private readonly ProducerConfig _config;

        private readonly string _topic;

        public KafkaProducerJSONService(IOptions<KafkaSettings> settings)
        {
            var kafkaSettings = settings.Value;
            _config = new ProducerConfig
            {
                BootstrapServers = kafkaSettings.BootstrapServers,
                Acks = Enum.Parse<Acks>(kafkaSettings.Acks, true),
            };
            _topic = kafkaSettings.Topic;
        }

        public async Task ProduceMessageAsync(string key, RandomMessage message)
        {
            using var producer = new ProducerBuilder<string, string>(_config).Build();
            var jsonMessage = JsonSerializer.Serialize(message);
            var kafkaMessage = new Message<string, string> { Key = key, Value = jsonMessage };
            await producer.ProduceAsync(_topic, kafkaMessage);
        }
    }
}
