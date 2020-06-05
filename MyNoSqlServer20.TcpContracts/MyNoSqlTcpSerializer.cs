using System;
using MyTcpSockets;
using MyTcpSockets.Extensions;
using System.Threading.Tasks;

namespace MyNoSqlServer.TcpContracts
{
    public class MyNoSqlTcpSerializer : ITcpSerializer<IMyNoSqlTcpContract>
    {

        public ValueTask<IMyNoSqlTcpContract> DeserializeAsync(TcpDataReader dataReader)
        {
            return SerializerDeserializer.DeserializeAsync(dataReader);
        }

        public int BufferSize => SerializerDeserializer.BufferSize;

        public ReadOnlyMemory<byte> Serialize(IMyNoSqlTcpContract data)
        {
            return SerializerDeserializer.Serialize(data);
        }
        
    }
}