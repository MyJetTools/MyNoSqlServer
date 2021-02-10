using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Api
{
    public interface IReaderConnection
    {
        
        public string Id { get;  }
        
        public string Name { get; }
        
        public string Ip { get; }
        
        public IEnumerable<string> Tables { get; }
        
        public DateTime ConnectedTime { get; }
        
        public DateTime LastIncomingTime { get; }
        
    }
}