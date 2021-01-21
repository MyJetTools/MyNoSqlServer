using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MyTcpSockets.Extensions;

namespace MyNoSqlServer.TcpContracts
{
    public interface IMyNoSqlTcpContract
    {
        void Serialize(Stream stream);
        ValueTask DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct);
    }

    public class PingContract : IMyNoSqlTcpContract
    {

        public static readonly PingContract Instance = new PingContract();

        public void Serialize(Stream stream)
        {
        }

        public ValueTask DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct)
        {
            return new ValueTask();
        }
    }

    public class PongContract : IMyNoSqlTcpContract
    {
        public static readonly PongContract Instance = new PongContract();

        public void Serialize(Stream stream)
        {

        }

        public ValueTask DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct)
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

        public async ValueTask DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct)
        {
           Name = await dataReader.ReadPascalStringAsync(ct);
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

        public async ValueTask DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct)
        {

            TableName = await dataReader.ReadPascalStringAsync(ct);
            Data = await dataReader.ReadByteArrayAsync(ct);

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

        public async ValueTask DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct)
        {

            TableName = await dataReader.ReadPascalStringAsync(ct);
            PartitionKey = await dataReader.ReadPascalStringAsync(ct);
            Data = (await dataReader.ReadByteArrayAsync(ct)).ToArray();
      
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

        public async ValueTask DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct)
        {

            TableName = await dataReader.ReadPascalStringAsync(ct);
            Data = (await dataReader.ReadByteArrayAsync(ct)).ToArray();

        }

    }
    
    public class SubscribeContract : IMyNoSqlTcpContract
    {
        public string TableName { get; set; }

        public void Serialize(Stream stream)
        {
            stream.WritePascalString(TableName);
        }

        public async ValueTask DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct)
        {
            TableName = await dataReader.ReadPascalStringAsync(ct);
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

        public async ValueTask DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct)
        {

            TableName = await dataReader.ReadPascalStringAsync(ct);

            var count = await dataReader.ReadIntAsync(ct);

            var result = new List<(string partitionKey, string rowKey)>();
            for (var i = 0; i < count; i++)
            {
                var partitionKey = await dataReader.ReadPascalStringAsync(ct);
                var rowKey = await dataReader.ReadPascalStringAsync(ct);
                result.Add((partitionKey, rowKey));
            }

            RowsToDelete = result;
            
        }
    }


    public class UpdateExpiresTimeTcpContract : IMyNoSqlTcpContract
    {
        //Amount of fields we Carry in the payload
        public byte Version { get; private set; }
        
        public string TableName { get; set; }
        public DateTime? Expires { get; set; }
        
        public bool ClearExpirationTime { get; set; }
        public string PartitionKey { get; set; }
        
        public string[] RowKeys { get; set; }
        
        
        
        public void Serialize(Stream stream)
        {
            stream.WriteByte(4);
            
            stream.WritePascalString(TableName);
            stream.WriteExpirationDateTime(Expires);
            stream.WritePascalString(PartitionKey);
            stream.WritePascalStringArray(RowKeys);
            stream.WriteByte(ClearExpirationTime ? (byte)1 : (byte)0);
        }

        public async ValueTask DeserializeAsync(ITcpDataReader dataReader, CancellationToken ct)
        {
            Version = await dataReader.ReadAndCommitByteAsync(ct);
            TableName = await dataReader.ReadPascalStringAsync(ct);

            Expires = await dataReader.ReadDateTimeAsync(ct);
            PartitionKey = await dataReader.ReadPascalStringAsync(ct);
            RowKeys = await dataReader.ReadPascalStringArrayAsync(ct);

            ClearExpirationTime = await dataReader.ReadAndCommitByteAsync(ct) > 0;
        }
    }
}