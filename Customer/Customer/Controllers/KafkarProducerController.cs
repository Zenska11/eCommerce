using System;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Routing;

[Route("api/kafka")]
    [ApiController]
    public class KafkaProducerController : ControllerBase
    {
        private readonly ProducerConfig config = new ProducerConfig
        { BootstrapServers = "localhost:9092" };
        private readonly string topic = "simple_topic";

        [HttpPost]
        public IActionResult Post([FromQuery] string message)
        {
            return Created(string.Empty, SendToKafka(topic, message));
        }

        [HttpGet]
        public async Task<IActionResult> Get()
        {
            var product = new Product
            {
                Id = Guid.NewGuid(),
                Name = "Stift",
                Color = "Rot",
                Description = "Bester Stift der Welt"
            };

            var offering = new Offering
            {
                Id = Guid.NewGuid(),
                Name = "Stift Offering",
                Quantity = 11,
                TotalPrice = 10,
                EffectiveDate = new DateOnly(),
                Status = "auf lager",
                Product = product,
            };


            return Ok(offering);
        }

    private Object SendToKafka(string topic, string message)
        {
            using (var producer =
                    new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    return producer.ProduceAsync(topic, new Message<Null, string> { Value = message })
                        .GetAwaiter()
                        .GetResult();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Oops, something went wrong: {e}");
                }
            }
            return null;
        }
    }

