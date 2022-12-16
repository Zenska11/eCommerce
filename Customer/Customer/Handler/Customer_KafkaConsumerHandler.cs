using Confluent.Kafka;
namespace Customer_KafkaConsumer.Handlers
{

    public class Cusomter_KafkaConsumerHandler : IHostedService
    {
        private readonly string topic_simple = "simple_topic";
        private readonly string topic_offerings_ch = "offerings_ch";
        public Task StartAsync(CancellationToken cancellationToken)
        {

            var conf = new ConsumerConfig
            {
                GroupId = "super_consumer_group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var builder = new ConsumerBuilder<Ignore, string>(conf).Build())
            {

                builder.Subscribe(topic_offerings_ch);
                var cancelToken = new CancellationTokenSource();
                try
                {
                    while (true)
                    {
                        var consumer = builder.Consume(cancelToken.Token);

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

    }
}
