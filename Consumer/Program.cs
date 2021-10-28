using System;
using System.Threading;
using Confluent.Kafka;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var cancelled = false;
            var cancellationToken = new CancellationToken(cancelled);

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "foo",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<long, string>(config).Build())
            {
                consumer.Subscribe("topicName");

                while (!cancelled)
                {
                    var consumeResult = consumer.Consume(cancellationToken);

                    // handle consumed message.
                    Console.WriteLine($"Key: {consumeResult.Message.Key} Value: {consumeResult.Message.Value}");
                }

                consumer.Close();
            }
        }
    }
}
