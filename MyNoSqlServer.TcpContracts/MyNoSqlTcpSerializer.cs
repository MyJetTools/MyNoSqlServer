using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using MyTcpSockets;
using MyTcpSockets.Extensions;

namespace MyNoSqlServer.TcpContracts
{
    public class MyNoSqlTcpSerializer : ITcpSerializer<IMyNoSqlTcpContract>
    {
        public async ValueTask<IMyNoSqlTcpContract> DeserializeAsync(ITcpDataReader dataReader,[EnumeratorCancellation] CancellationToken ct)
        {
            var result = await SerializerDeserializer.DeserializeAsync(dataReader, ct);
            return result;
        }

        public int BufferSize => SerializerDeserializer.BufferSize;

        public ReadOnlyMemory<byte> Serialize(IMyNoSqlTcpContract data)
        {
            return SerializerDeserializer.Serialize(data);
        }
        
    }
}