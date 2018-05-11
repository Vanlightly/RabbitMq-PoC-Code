using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQTestExamples.IntegrationTests.Helpers
{
    public class QueueDestroyer
    {
        public static void DeleteQueue(string queueName, string virtualHost)
        {
            var connectionFactory = new ConnectionFactory();
            connectionFactory.HostName = "localhost";
            connectionFactory.UserName = "guest";
            connectionFactory.Password = "guest";
            connectionFactory.VirtualHost = virtualHost;
            var connection = connectionFactory.CreateConnection();
            var channel = connection.CreateModel();
            channel.QueueDelete(queueName);
            connection.Close();
        }

        public static void DeleteExchange(string exchangeName, string virtualHost)
        {
            var connectionFactory = new ConnectionFactory();
            connectionFactory.HostName = "localhost";
            connectionFactory.UserName = "guest";
            connectionFactory.Password = "guest";
            connectionFactory.VirtualHost = virtualHost;
            var connection = connectionFactory.CreateConnection();
            var channel = connection.CreateModel();
            channel.ExchangeDelete(exchangeName);
            connection.Close();
        }
    }
}
