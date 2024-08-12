namespace KafkaMessageAPI.Models
{
    public class RandomMessage
    {
        public int Id { get; set; }
        public required string Message { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
