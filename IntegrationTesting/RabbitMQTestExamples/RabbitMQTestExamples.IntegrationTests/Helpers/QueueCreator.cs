using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQTestExamples.IntegrationTests.Helpers
{
    public class QueueCreator
    {
        public static void CreateQueueAndBinding(string queueName, string exchangeName, string virtualHost)
        {
            var connectionFactory = new ConnectionFactory();
            connectionFactory.HostName = "localhost";
            connectionFactory.UserName = "guest";
            connectionFactory.Password = "guest";
            connectionFactory.VirtualHost = virtualHost;
            var connection = connectionFactory.CreateConnection();
            var channel = connection.CreateModel();
            channel.ExchangeDeclare(exchangeName, ExchangeType.Fanout);
            channel.QueueDeclare(queueName, true, false, false, null);
            channel.QueueBind(queueName, exchangeName, "");
            connection.Close();
        }

        public static void CreateQueueAndBinding(string queueName, string exchangeName, string virtualHost, string dlx, int messageTtl)
        {
            var connectionFactory = new ConnectionFactory();
            connectionFactory.HostName = "localhost";
            connectionFactory.VirtualHost = virtualHost;
            var connection = connectionFactory.CreateConnection();
            var channel = connection.CreateModel();
            channel.ExchangeDeclare(exchangeName, ExchangeType.Fanout, true);

            var queueProps = new Dictionary<string, object>();
            queueProps.Add("x-dead-letter-exchange", dlx);
            queueProps.Add("x-message-ttl", messageTtl);
            channel.QueueDeclare(queueName, true, false, false, queueProps);
            channel.QueueBind(queueName, exchangeName, "");
            connection.Close();
        }

        public static void CreateExchange(string exchangeName, string virtualHost)
        {
            var connectionFactory = new ConnectionFactory();
            connectionFactory.HostName = "localhost";
            connectionFactory.VirtualHost = virtualHost;
            var connection = connectionFactory.CreateConnection();
            var channel = connection.CreateModel();
            channel.ExchangeDeclare(exchangeName, ExchangeType.Fanout);
            connection.Close();
        }
    }
}
