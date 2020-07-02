using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using MyTcpSockets;
using MyTcpSockets.Extensions;

namespace MyNoSqlServer.TcpContracts
{
    public class MyNoSqlTcpSerializer : ITcpSerializer<IMyNoSqlTcpContract>
    {
        public async IAsyncEnumerable<IMyNoSqlTcpContract> DeserializeAsync(TcpDataReader dataReader,[EnumeratorCancellation] CancellationToken ct)
        {
            var result = await SerializerDeserializer.DeserializeAsync(dataReader, ct);
            yield return result;
        }

        public int BufferSize => SerializerDeserializer.BufferSize;

        public ReadOnlyMemory<byte> Serialize(IMyNoSqlTcpContract data)
        {
            return SerializerDeserializer.Serialize(data);
        }
        
    }
}