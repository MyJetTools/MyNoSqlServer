using System.Collections.Generic;
using MyNoSqlServer.Api.DataReadersTcpServer;

namespace MyNoSqlServer.Api.Models
{
    public class ReaderApiModel
    {
        public string Name { get; set; }
        public string Ip { get; set; }
        
        public IEnumerable<string> Tables { get; set; }
        
        public string ConnectedTime { get; set; }
        
        public string LastIncomingTime { get; set; }
        
        public string Id { get; set; }

        public static ReaderApiModel Create(DataReaderTcpService connection)
        {
            return new ReaderApiModel
            {
                Id = connection.Id.ToString(),
                Name = connection.ContextName,
                Ip = connection.Ip,
                Tables = connection.Tables,
                ConnectedTime = connection.ConnectedTime.ToString("s"),
                LastIncomingTime = connection.LastIncomingTime.ToString("s"),
            };
        }
        
    }
}