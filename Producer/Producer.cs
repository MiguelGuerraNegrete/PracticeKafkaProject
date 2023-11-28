using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;

class Producer
{
    public static async Task Main(string[] args)
    {
        if (args.Length != 1)
        {
            Console.WriteLine("Please provide the configuration file path as a command line argument");
        }

        const string topic = "purchases";

        string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
        string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };

        var config = new ProducerConfig
        {
            BootstrapServers = "pkc-lzvrd.us-west4.gcp.confluent.cloud:9092", 
            SecurityProtocol= SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "R2KYS6HSNT7YCRU7",
            SaslPassword = "7VI3JJlpmnc7XTginA5+rE6BZfb83qxrz8CHvm/wfEmOJhKUt2InFGMYhH+YQG4F",
        };

        using (var producer = new ProducerBuilder<string, string>(config).Build())
        {
            var numProduced = 0;
            Random rnd = new();
            const int numMessages = 10;
            for (int i = 0; i < numMessages; ++i)
            {
                var user = users[rnd.Next(users.Length)];
                var item = items[rnd.Next(items.Length)];

                await producer.ProduceAsync(topic, new Message<string, string> { Key = user, Value = item })
                    .ContinueWith(task =>
                    {
                        if (task.IsFaulted)
                        {
                            Console.WriteLine($"Failed to deliver message: {task.Exception.Message}");
                        }
                        else
                        {
                            Console.WriteLine($"Produced event to topic {topic}: key = {user,-10} value = {item}");
                            numProduced += 1;
                        }
                    });
            }
            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
        }
    }
}
