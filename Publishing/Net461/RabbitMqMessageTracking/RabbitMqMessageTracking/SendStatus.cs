using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMqMessageTracking
{
    public enum SendStatus
    {
        PendingSend, // have not sent the message yet
        PendingResponse, // sent the message, waiting for an ack
        Success, // ack received
        Failed, // nack received
        Unroutable, // message returned
        NoExchangeFound // 404 reply code
    }
}
