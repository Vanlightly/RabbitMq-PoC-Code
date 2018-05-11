using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQTestExamples.ConsoleApp
{
    public interface IMessageProcessor
    {
        void ProcessMessage(string message);
    }
}
