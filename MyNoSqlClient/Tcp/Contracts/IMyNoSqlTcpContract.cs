using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using MyTcpSockets;

namespace MyNoSqlClient.Tcp.Contracts
{
    public interface IMyNoSqlTcpContract
    {
        void Serialize(Stream stream);
        ValueTask DeserializeAsync(Stream stream);
    }
    
    public class PingContract : IMyNoSqlTcpContract
    {
        
        public static readonly PingContract Instance = new PingContract();
            
        public void Serialize(Stream stream)
        {
        }

        public ValueTask DeserializeAsync(Stream stream)
        {
            return new ValueTask(); 
        }
    }

    public class PongContract : IMyNoSqlTcpContract
    {
        public static readonly PongContract Instance = new PongContract();
        private static readonly byte[] Data = {(byte) CommandType.Pong}; 
        
        public void Serialize(Stream stream)
        {
         
        }

        public ValueTask DeserializeAsync(Stream stream)
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

        public async ValueTask DeserializeAsync(Stream stream)
        {
            Name = await stream.ReadPascalString();
        }
    }


    public class InitTableContract : IMyNoSqlTcpContract
    {
        public string TableName { get; set; }
        public byte[] Data { get; set; }
        public void Serialize(Stream stream)
        {
            stream.WritePascalString(TableName);
            stream.WriteReadOnlyMemory(Data);
            
        }

        public async ValueTask DeserializeAsync(Stream stream)
        {
            TableName = await stream.ReadPascalString();
            Data = await stream.ReadAsArrayOfBytesAsync();

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
            stream.WriteReadOnlyMemory(Data);
            
        }

        public async ValueTask DeserializeAsync(Stream stream)
        {
            TableName = await stream.ReadPascalString();
            PartitionKey = await stream.ReadPascalString();
            Data = await stream.ReadAsArrayOfBytesAsync();

        }
        
    }

    public class UpdateRowsContract : IMyNoSqlTcpContract
    {
        
        public string TableName { get; set; }
        
        public byte[] Data { get; set; }
        public void Serialize(Stream stream)
        {
            stream.WritePascalString(TableName);
            stream.WriteReadOnlyMemory(Data);
            
        }

        public async ValueTask DeserializeAsync(Stream stream)
        {
            TableName = await stream.ReadPascalString();
            Data = await stream.ReadAsArrayOfBytesAsync();

        }
        
    }

    public class DeleteRowsContract : IMyNoSqlTcpContract
    {
        public string TableName { get; set; }
        
        public IReadOnlyList<(string partitionKey, string RowKey)> RowsToDelete { get; set; }
        
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

        public async ValueTask DeserializeAsync(Stream stream)
        {
            TableName = await stream.ReadPascalString();
            var count = (int)await stream.ReadUintFromSocket();

            var result = new List<(string partitionKey, string rowKey)>();
            for (var i = 0; i < count; i++)
            {
                var partitionKey = await stream.ReadPascalString();
                var rowKey = await stream.ReadPascalString();
                result.Add((partitionKey, rowKey));
            }

            RowsToDelete = result;
        }
    }

    public class SubscribeContract: IMyNoSqlTcpContract
    {
        public string TableName { get; set; }
        public void Serialize(Stream stream)
        {
            stream.WritePascalString(TableName);
        }

        public async ValueTask DeserializeAsync(Stream stream)
        {
            TableName = await stream.ReadPascalString();
        }
    }
}