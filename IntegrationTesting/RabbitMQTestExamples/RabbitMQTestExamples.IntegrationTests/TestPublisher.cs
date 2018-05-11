using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQTestExamples.IntegrationTests
{
    public class TestPublisher
    {
        public void Publish(string queueName, string message)
        {
            var factory = new ConnectionFactory() { HostName = "localhost", UserName="guest", Password="guest" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(exchange: "",
                                     routingKey: queueName,
                                     basicProperties: null,
                                     body: body);
            }
        }
    }
}
