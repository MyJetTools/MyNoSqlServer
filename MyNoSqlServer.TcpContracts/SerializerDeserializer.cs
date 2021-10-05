using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MyTcpSockets.Extensions;

namespace MyNoSqlServer.TcpContracts
{
    public class SerializerDeserializer
    {
        
        private static readonly Dictionary<CommandType, Func<IMyNoSqlTcpContract>> CommandToContractMapper
            = new Dictionary<CommandType, Func<IMyNoSqlTcpContract>>
            {
                [CommandType.Ping] =          () => PingContract.Instance,
                [CommandType.Pong] =          () => PongContract.Instance,
                [CommandType.Greeting] =      () => new GreetingContract(),
                [CommandType.Subscribe] =     () => new SubscribeContract(),
                [CommandType.InitTable] =     () => new InitTableContract(),
                [CommandType.InitPartition] = () => new InitPartitionContract(),
                [CommandType.UpdateRows] =    () => new UpdateRowsContract(),
                [CommandType.DeleteRow] =     () => new DeleteRowsContract()
            };

        private static readonly Dictionary<Type, CommandType> TypeToCommandType =
            new Dictionary<Type, CommandType>();

        static SerializerDeserializer()
        {
            foreach (var itm in CommandToContractMapper)
            {
                TypeToCommandType.Add(itm.Value().GetType(), itm.Key);
            }
        }

        public static async ValueTask<IMyNoSqlTcpContract> DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct)
        {
            var command = (CommandType)await dataReader.ReadByteAsync(ct);

            var instance = CommandToContractMapper[command]();
            
            await instance.DeserializeAsync(dataReader, ct);

            return instance;
        }

        public const int BufferSize = 32768;


        public static ReadOnlyMemory<byte> Serialize(IMyNoSqlTcpContract data)
        {
            var mem = new MemoryStream();

            var command = TypeToCommandType[data.GetType()];
            mem.WriteByte((byte)command);
            data.Serialize(mem);
            return mem.ToArray();
        }
    }
}