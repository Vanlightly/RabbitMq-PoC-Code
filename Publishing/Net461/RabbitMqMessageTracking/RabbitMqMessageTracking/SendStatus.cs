using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMqMessageTracking
{
    public enum SendStatus
    {
        PendingSend,
        PendingResponse,
        Success,
        Failed,
        Unroutable,
        PossiblyLost,
        NoExchangeFound
    }
}
