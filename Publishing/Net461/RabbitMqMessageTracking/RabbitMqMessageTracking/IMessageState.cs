using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMqMessageTracking
{
    public interface IMessageState<T>
    {
        T MessagePayload { get; }
        SendStatus Status { get;  }
        string Description { get; }
        string MessageId { get; }
        int SendCount { get;  }
    }
}
