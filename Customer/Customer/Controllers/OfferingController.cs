using System;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Routing;
using Newtonsoft.Json;

[Route("api/customer")]
[ApiController]
public class OfferingController : ControllerBase
{
    private readonly ProducerConfig configProducer = new ProducerConfig
    { BootstrapServers = "localhost:9092" };
    private readonly string offeringsRequestTopic = "offeringsRequest_topic";
    private readonly string offeringsResponseTopic = "offeringsResponse_topic";
    private readonly KafkaConsumerHandler _kafkaConsumerHandler;
    [ThreadStatic]
    public static Weather weatherResult = null;
    private List<Thread> threadList = new List<Thread>();

    public OfferingController(KafkaConsumerHandler kafkaConsumerHandler)
    {
        _kafkaConsumerHandler = kafkaConsumerHandler;
        
    }



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
        using var producer = new ProducerBuilder<Null, string>(configProducer).Build();
        //Weather resultWeather = new Weather("leer", 2);


        
        //threadList.Add(new Thread(
        //() => { weatherResult = _kafkaConsumerHandler.ConsumeFromKafka(); }));
        //threadList.Last().Start();
        //threadList.Last().Join();

        try
        {
            var response = await producer.ProduceAsync(offeringsRequestTopic,
                new Message<Null, string> { Value = JsonConvert.SerializeObject(new Weather(message, 70)) });
            // Console.WriteLine(response.Value);
            
        }
        catch (ProduceException<Null, string> exc)
        {
            Console.WriteLine(exc.Message);
        }
        
        

        var serializedResult = JsonConvert.SerializeObject(weatherResult);

        return Ok(serializedResult);
    }

    //public void ConsumeFromKafka()
    //{
    //    Console.WriteLine("Consumer Loop started");

    //    var config = new ConsumerConfig
    //    {
    //        GroupId = "offeringsResponse-consumer-group",
    //        BootstrapServers = "localhost:9092",
    //        AutoOffsetReset = AutoOffsetReset.Earliest,

    //    };
    //    using var consumer = new ConsumerBuilder<Null, string>(config).Build();

    //    consumer.Subscribe(offeringsResponseTopic);

    //    CancellationTokenSource token = new();

    //    try
    //    {

    //        while (true)
    //        {
    //            var response = consumer.Consume(token.Token);
    //            if (response.Message != null)
    //            {
    //                var weather = JsonConvert.DeserializeObject<Weather>(response.Message.Value);
    //                Console.WriteLine($"State: {weather.State}, Temp: {weather.Temparature} Grad Antwort");
    //            }

    //        }


    //    }
    //    catch (ProduceException<Null, string> exc)
    //    {
    //        Console.WriteLine(exc.Message);
    //    }
    //}

}

