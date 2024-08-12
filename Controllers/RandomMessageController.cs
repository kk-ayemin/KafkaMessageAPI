using KafkaMessageAPI.Models;
using KafkaMessageAPI.Services;
using Microsoft.AspNetCore.Mvc;
using System;

namespace KafkaMessageAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class RandomMessageController : ControllerBase
    {
        private readonly KafkaProducerJSONService _kafkaProducerService;

        public RandomMessageController(KafkaProducerJSONService     kafkaProducerService)
        {
            _kafkaProducerService = kafkaProducerService;
        }

        [HttpPost]
        public async Task<IActionResult> ProduceRandomMessage()
        {
            var random = new Random();
            var randomMessage = new RandomMessage
            {
                Id = random.Next(1, 1000),
                Message = "Random message " + random.Next(1, 1000),
                Timestamp = DateTime.UtcNow
            };

            await _kafkaProducerService.ProduceMessageAsync(randomMessage.Id.ToString(), randomMessage);
            return Ok("Message produced successfully");
        }
    }
}
