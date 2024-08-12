public class KafkaSettings
{
    public string? BootstrapServers { get; set; }
    public string? SecurityProtocol { get; set; }
    public string? GroupId { get; set; }
    public string? AutoOffsetReset { get; set; }
    public string? Topic { get; set; }
    public bool EnableAutoCommit { get; set; }
    public int AutoCommitIntervalMs { get; set; }
    public string? Acks { get; set; }

}
