using System;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Routing;

[Route("api/customer")]
[ApiController]
public class OfferingController : ControllerBase
{
    private readonly ProducerConfig config = new ProducerConfig
    { BootstrapServers = "localhost:9092" };
    private readonly string topic = "offerings_ch";


    [HttpGet("GetAllOfferings")]
    public async Task<IActionResult> Get()
    {
        var product1 = new Product
        {
            Id = Guid.NewGuid(),
            Name = "Stift",
            Color = "Rot",
            Description = "Bester Stift der Welt",
            Price = 14,
        };

        var offering1 = new Offering
        {
            Id = Guid.NewGuid(),
            Name = "Auto Offering",
            Quantity = 11,
            TotalPrice = 10,
            EffectiveDate = DateTime.Today,
            Status = "auf lager",
            Product = product1,
        };

        var product2 = new Product
        {
            Id = Guid.NewGuid(),
            Name = "Auto",
            Color = "Blau",
            Description = "Bestes Auto der Welt",
            Price = 50,
        };

        var offering2 = new Offering
        {
            Id = Guid.NewGuid(),
            Name = "Stift Offering",
            Quantity = 2,
            TotalPrice = 100,
            EffectiveDate = DateTime.Today,
            Status = "auf lager",
            Product = product2,
        };

    var offerings = new List<Offering>();
    offerings.Add(offering1);
    offerings.Add(offering2);


    return Ok(offerings);
    }

    [HttpGet("GetMessage")]
    public async Task<IActionResult> GetMessage([FromQuery] string message)
    {
        SendMessageToKafka(topic, message);

        return Ok();
    }

    private Object SendMessageToKafka(string topic, string message)
    {
        Console.WriteLine("SendMessageToKafka");
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

