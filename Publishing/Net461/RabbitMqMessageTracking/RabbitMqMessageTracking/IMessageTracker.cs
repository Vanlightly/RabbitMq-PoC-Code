using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMqMessageTracking
{
    public interface IMessageTracker<T>
    {
        List<MessageState<T>> GetMessageStates();
        bool PublishingInterrupted { get; }
        string InterruptionReason { get; }
        Exception UnexpectedException { get; }
    }
}
