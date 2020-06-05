using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlClient.ReadRepository;

namespace MyNoSqlClient
{

    public interface IMySignalRConnection : IMyNoSqlSubscriber
    {
        string Url { get; }
    }

}