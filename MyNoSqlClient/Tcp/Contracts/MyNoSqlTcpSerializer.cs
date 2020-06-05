using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using MyTcpSockets;

namespace MyNoSqlClient.Tcp.Contracts
{
    public class MyNoSqlTcpSerializer : ITcpSerializer<IMyNoSqlTcpContract>
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

        static MyNoSqlTcpSerializer()
        {
            foreach (var itm in CommandToContractMapper)
            {
                TypeToCommandType.Add(itm.Value().GetType(), itm.Key);
            }
        }

        public async ValueTask<int> DeserializeAsync(Stream stream, Func<IMyNoSqlTcpContract, ValueTask> newDataIsReady)
        {
            var command = (CommandType)await stream.ReadByteFromSocket();

            var instance = CommandToContractMapper[command]();

            await instance.DeserializeAsync(stream);

            await newDataIsReady(instance);

            return 1;
        }

        public ReadOnlyMemory<byte> Serialize(IMyNoSqlTcpContract data)
        {
            var mem = new MemoryStream();

            var command = TypeToCommandType[data.GetType()];
            mem.WriteByte((byte)command);
            data.Serialize(mem);
            return mem.ToArray();
        }
        
        public static readonly MyNoSqlTcpSerializer Instance = new MyNoSqlTcpSerializer();
        
    }
}