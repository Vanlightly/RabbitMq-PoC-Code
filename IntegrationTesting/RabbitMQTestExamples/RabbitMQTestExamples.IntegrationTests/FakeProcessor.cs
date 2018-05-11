using RabbitMQTestExamples.ConsoleApp;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQTestExamples.IntegrationTests
{
    public class FakeProcessor : IMessageProcessor
    {
        public List<string> Messages { get; set; }

        public FakeProcessor()
        {
            Messages = new List<string>();
        }

        public void ProcessMessage(string message)
        {
            Messages.Add(message);
        }
    }
}
