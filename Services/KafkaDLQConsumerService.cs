using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using OfficeOpenXml;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

public class KafkaDLQConsumerService : IHostedService
{
    private readonly KafkaSettings _kafkaSettings;
    private IConsumer<string, string> _consumer;
    private readonly CancellationTokenSource _cancellationTokenSource;


    public KafkaDLQConsumerService(IOptions<KafkaSettings> settings)
    {
        _kafkaSettings = settings.Value;

        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
            GroupId = _kafkaSettings.GroupId,
            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(_kafkaSettings.AutoOffsetReset, true),
            EnableAutoCommit = false,
            SecurityProtocol = Enum.Parse<SecurityProtocol>(_kafkaSettings.SecurityProtocol, true),
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _cancellationTokenSource = new CancellationTokenSource();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        Task.Run(() => StartConsuming(_cancellationTokenSource.Token), cancellationToken);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cancellationTokenSource.Cancel();
        _consumer.Close();
        return Task.CompletedTask;
    }

    private void StartConsuming(CancellationToken cancellationToken)
    {
        var Topic = "topics.dlq";
        _consumer.Subscribe(Topic);
        Console.WriteLine($"Subscribed to topic {Topic}. Waiting for messages...");

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var cr = _consumer.Consume(cancellationToken);
                Console.WriteLine($"Consumed event from topic {Topic}: key = {cr.Message.Key}, value = {cr.Message.Value}");

                try
                {
                    // Parse the JSON and extract the relevant fields
                    var json = JObject.Parse(cr.Message.Value);
                    string id = json["Id"]?.ToString();
                    string randomMessage = json["Message"]?.ToString();
                    string errorMessage = json["ErrorMessage"]?.ToString();
                    string timestamp = json["Timestamp"]?.ToString();

                    // Write the extracted data to the daily Excel file
                    WriteMessageToDailyExcel(id, randomMessage, errorMessage, timestamp);

                    // Commit the offset after processing successfully
                    _consumer.Commit(cr);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing message: {ex.Message}");
                    // Handle errors (e.g., log them)
                }
            }
            catch (ConsumeException ex)
            {
                Console.WriteLine($"Consume error: {ex.Message}");
            }
        }
    }

    private void WriteMessageToDailyExcel(string id, string randomMessage, string errorMessage, string timestamp)
    {
        // Generate the daily file name based on the current date
        string currentDate = DateTime.UtcNow.ToString("yyyy-MM-dd");
        string excelFilePath = $"DLQMessages_{currentDate}.xlsx";
        var fileInfo = new FileInfo(excelFilePath);

        using (var package = new ExcelPackage(fileInfo))
        {
            var worksheet = package.Workbook.Worksheets.Count == 0 
                ? package.Workbook.Worksheets.Add("DLQMessages") 
                : package.Workbook.Worksheets[0];

            // Add the title row if the worksheet is empty
            if (worksheet.Dimension == null)
            {
                worksheet.Cells[1, 1].Value = "ID";
                worksheet.Cells[1, 2].Value = "Random Message";
                worksheet.Cells[1, 3].Value = "Error Message";
                worksheet.Cells[1, 4].Value = "Timestamp";
            }

            // Find the next available row
            int nextRow = worksheet.Dimension?.Rows + 1 ?? 2;

            // Write the extracted data to the worksheet
            worksheet.Cells[nextRow, 1].Value = id;
            worksheet.Cells[nextRow, 2].Value = randomMessage;
            worksheet.Cells[nextRow, 3].Value = errorMessage;
            worksheet.Cells[nextRow, 4].Value = timestamp;

            // Save the changes to the Excel file
            package.Save();
        }

        Console.WriteLine("Message written to Excel.");
    }
}
