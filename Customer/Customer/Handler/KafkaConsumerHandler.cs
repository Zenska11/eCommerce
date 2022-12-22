using System;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Routing;
using Newtonsoft.Json;
public class KafkaConsumerHandler
    {
        private readonly string offeringsRequestTopic = "offeringsRequest_topic";
        private readonly string offeringsResponseTopic = "offeringsResponse_topic";
        public Weather ConsumeFromKafka()
        {
            Console.WriteLine("Consumer Loop started");

            var config = new ConsumerConfig
            {
                GroupId = "offeringsResponse-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,

            };
            using var consumer = new ConsumerBuilder<Null, string>(config).Build();

            consumer.Subscribe(offeringsResponseTopic);

            CancellationTokenSource token = new();

            try
            {

                while (true)
                {
                    var response = consumer.Consume(token.Token);
                    if (response.Message != null)
                    {
                        var weather = JsonConvert.DeserializeObject<Weather>(response.Message.Value);
                        Console.WriteLine($"State: {weather.State}, Temp: {weather.Temparature} Grad Antwort");
                    return weather;
                    //Console.WriteLine("Ich laufe noch, obwohl ich schon returnt habe");
                }
                
                }
                
                
            }
            catch (ProduceException<Null, string> exc)
            {
                Console.WriteLine(exc.Message);
                
            }
            return new Weather("Nichts", 0);
    }
}

