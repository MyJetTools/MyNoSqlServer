using System;
using System.Collections.Generic;
using System.IO;
using MyTcpSockets;
using MyTcpSockets.Extensions;

namespace MyNoSqlServer.TcpContracts
{
    public class MyNoSqlTcpSerializer : ITcpSerializer<IMyNoSqlTcpContract>
    {
        public async IAsyncEnumerable<IMyNoSqlTcpContract> DeserializeAsync(TcpDataReader dataReader)
        {
            var result = await SerializerDeserializer.DeserializeAsync(dataReader);
            yield return result;
        }

        public int BufferSize => SerializerDeserializer.BufferSize;

        public ReadOnlyMemory<byte> Serialize(IMyNoSqlTcpContract data)
        {
            return SerializerDeserializer.Serialize(data);
        }
        
    }
}