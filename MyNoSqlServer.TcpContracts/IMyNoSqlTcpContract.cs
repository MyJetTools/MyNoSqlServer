using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using MyTcpSockets.Extensions;

namespace MyNoSqlServer.TcpContracts
{
    public interface IMyNoSqlTcpContract
    {
        void Serialize(Stream stream);
        ValueTask DeserializeAsync(TcpDataReader dataReader);
    }

    public class PingContract : IMyNoSqlTcpContract
    {

        public static readonly PingContract Instance = new PingContract();

        public void Serialize(Stream stream)
        {
        }

        public ValueTask DeserializeAsync(TcpDataReader dataReader)
        {
            return new ValueTask();
        }
    }

    public class PongContract : IMyNoSqlTcpContract
    {
        public static readonly PongContract Instance = new PongContract();
        private static readonly byte[] Data = { (byte)CommandType.Pong };

        public void Serialize(Stream stream)
        {

        }

        public ValueTask DeserializeAsync(TcpDataReader dataReader)
        {
            return new ValueTask();
        }
    }


    public class GreetingContract : IMyNoSqlTcpContract
    {
        public string Name { get; set; }

        public void Serialize(Stream stream)
        {
            stream.WritePascalString(Name);
        }

        public async ValueTask DeserializeAsync(TcpDataReader dataReader)
        {
            Name = await dataReader.ReadPascalStringAsync();
        }
    }


    public class InitTableContract : IMyNoSqlTcpContract
    {
        public string TableName { get; set; }
        public byte[] Data { get; set; }
        public void Serialize(Stream stream)
        {
            stream.WritePascalString(TableName);
            stream.WriteByteArray(Data);

        }

        public async ValueTask DeserializeAsync(TcpDataReader dataReader)
        {
            TableName = await dataReader.ReadPascalStringAsync();
            Data = (await dataReader.ReadByteArrayAsync()).ToArray();
        }

    }

    public class InitPartitionContract : IMyNoSqlTcpContract
    {
        public string TableName { get; set; }
        public string PartitionKey { get; set; }
        public byte[] Data { get; set; }
        public void Serialize(Stream stream)
        {
            stream.WritePascalString(TableName);
            stream.WritePascalString(PartitionKey);
            stream.WriteByteArray(Data);

        }

        public async ValueTask DeserializeAsync(TcpDataReader dataReader)
        {
            TableName = await dataReader.ReadPascalStringAsync();
            PartitionKey = await dataReader.ReadPascalStringAsync();
            Data = (await dataReader.ReadByteArrayAsync()).ToArray();
        }

    }

    public class UpdateRowsContract : IMyNoSqlTcpContract
    {

        public string TableName { get; set; }

        public byte[] Data { get; set; }
        public void Serialize(Stream stream)
        {
            stream.WritePascalString(TableName);
            stream.WriteByteArray(Data);
        }

        public async ValueTask DeserializeAsync(TcpDataReader dataReader)
        {
            TableName = await dataReader.ReadPascalStringAsync();
            Data = (await dataReader.ReadByteArrayAsync()).ToArray();
        }

    }
    
    public class SubscribeContract : IMyNoSqlTcpContract
    {
        public string TableName { get; set; }

        public void Serialize(Stream stream)
        {
            stream.WritePascalString(TableName);
        }

        public async ValueTask DeserializeAsync(TcpDataReader dataReader)
        {
            TableName = await dataReader.ReadPascalStringAsync();
        }
    }

    public class DeleteRowsContract : IMyNoSqlTcpContract
    {
        public string TableName { get; set; }

        public IReadOnlyList<(string PartitionKey, string RowKey)> RowsToDelete { get; set; }

        public void Serialize(Stream stream)
        {
            stream.WritePascalString(TableName);
            stream.WriteInt(RowsToDelete.Count);
            foreach (var (partitionKey, rowKey) in RowsToDelete)
            {
                stream.WritePascalString(partitionKey);
                stream.WritePascalString(rowKey);
            }
        }

        public async ValueTask DeserializeAsync(TcpDataReader dataReader)
        {
            TableName = await dataReader.ReadPascalStringAsync();

            var count = await dataReader.ReadIntAsync();

            var result = new List<(string partitionKey, string rowKey)>();
            for (var i = 0; i < count; i++)
            {
                var partitionKey = await dataReader.ReadPascalStringAsync();
                var rowKey = await dataReader.ReadPascalStringAsync();
                result.Add((partitionKey, rowKey));
            }

            RowsToDelete = result;
        }

        
    }
}