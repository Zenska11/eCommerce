using Confluent.Kafka;
namespace ST_KafkaConsumer.Handlers
{
    public class KafkaConsumerHandler : IHostedService
    {
        private readonly string topic_simple = "simple_topic";
        private readonly string topic_offerings_ch = "offerings_ch";
        private readonly ProducerConfig config = new ProducerConfig
        { BootstrapServers = "localhost:9092" };
        public Task StartAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("Kafka - Werde ich aufgerufen?");
            var conf = new ConsumerConfig
            {
                GroupId = "st_consumer_group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var builder = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                var topics = new List<string>() { "simple_topic", "offerings_ch" };
                builder.Subscribe(topics);
               // builder.Subscribe(topic_offerings_ch);
                var cancelToken = new CancellationTokenSource();
                try
                {
                    while (true)
                    {
                        var consumer = builder.Consume(cancelToken.Token);
                        if (consumer.Value == "Boot")
                        {
                            Console.WriteLine("Message ist Boot!");
                        }
                        if (consumer.Value == "GetMessage")
                        {
                            Console.WriteLine("Message ist GetMessage!");
                            SendMessageToCustomer();
                        }
                        Console.WriteLine($"Message: {consumer.Message.Value} received from {consumer.TopicPartitionOffset}");
                    }
                }
                catch (Exception)
                {
                    builder.Close();
                }
            }
            return Task.CompletedTask;
        }
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public void GetMessage()
        {
            Console.WriteLine("Get Message Function");
        }

        private Object SendMessageToCustomer()
        {
            Console.WriteLine("SendMessageToCustomer");
            using (var producer =
                    new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    return producer.ProduceAsync(topic_offerings_ch, new Message<Null, string> { Value = "Das ist die Antwort :D" })
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
}
