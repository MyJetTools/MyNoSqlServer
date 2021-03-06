using System;
using System.Threading;
using MyTcpSockets;
using MyTcpSockets.Extensions;
using System.Threading.Tasks;

namespace MyNoSqlServer.TcpContracts
{
    public class MyNoSqlTcpSerializer : ITcpSerializer<IMyNoSqlTcpContract>
    {

        public ValueTask<IMyNoSqlTcpContract> DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct)
        {
            return SerializerDeserializer.DeserializeAsync(dataReader, ct);
        }

        public int BufferSize => SerializerDeserializer.BufferSize;

        public ReadOnlyMemory<byte> Serialize(IMyNoSqlTcpContract data)
        {
            return SerializerDeserializer.Serialize(data);
        }
        
    }
}