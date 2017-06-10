using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMqMessageTracking
{
    public class MessageState<T> : IMessageState<T>
    {
        public MessageState(T payload)
        {
            MessagePayload = payload;
            MessageId = Guid.NewGuid().ToString();
        }

        public T MessagePayload { get; set; }
        public SendStatus Status { get; set; }
        public bool Acknowledged { get; set; }
        public ulong SequenceNumber { get; set; }
        public string Description { get; set; }
        public string MessageId { get; set; }
        public int SendCount { get; set; }
    }
}
