using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMqMessageTracking
{
    public class MessageState<T>
    {
        public MessageState(T payload)
        {
            Payload = payload;
            MessageId = Guid.NewGuid().ToString();
        }

        public T Payload { get; set; }
        public SendStatus Status { get; set; }
        public string Description { get; set; }
        public string MessageId { get; set; }
        public int SendCount { get; set; }
    }
}
