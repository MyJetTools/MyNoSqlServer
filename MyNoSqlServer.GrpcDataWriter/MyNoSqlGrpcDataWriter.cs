using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Grpc;

namespace MyNoSqlServer.GrpcDataWriter
{
    public class MyNoSqlGrpcDataWriter
    {
        private readonly IMyNoSqlTransportGrpcService _myNoSqlTransportGrpcService;

        public MyNoSqlGrpcDataWriter(IMyNoSqlTransportGrpcService myNoSqlTransportGrpcService)
        {
            _myNoSqlTransportGrpcService = myNoSqlTransportGrpcService;
        }

        private readonly Dictionary<Type, string> _typesToTableNames = new ();

        public MyNoSqlGrpcDataWriter RegisterSupportedEntity<T>(string tableName) where T : IMyNoSqlDbEntity, new()
        {
            _typesToTableNames.Add(typeof(T), tableName);
            return this;
        }

        private string GetTableName(Type type)
        {
            if (_typesToTableNames.TryGetValue(type, out var result))
                return result;

            throw new Exception("There is not tableName registered for the TableEntity with Type: " + type);
        }
        
        private void CheckResponse(MyNoSqlResponse table, string tableName)
        {
            switch (table)
            {
                case MyNoSqlResponse.TableNotFound:
                    throw new Exception($"Table {tableName} is not found");
            }
        }
        
        public async IAsyncEnumerable<T> GetRowsAsync<T>(int? limit = null, int? skip = null)
            where T : IMyNoSqlDbEntity, new()
        {

            var dataResult = _myNoSqlTransportGrpcService.GetRowsAsync(new GetEntitiesGrpcRequest
            {
                TableName = GetTableName(typeof(T)),
                PartitionKey = null,
                RowKey = null,
                Limit = limit,
                Skip = skip
            });

            await foreach (var tableEntityContract in dataResult)
            {
                yield return tableEntityContract.DeserializeEntity<T>();
            }
        }
        
        public async IAsyncEnumerable<T> GetRowsAsync<T>(string partitionKey, int? limit = null, int? skip = null)
            where T : IMyNoSqlDbEntity, new()
        {

            var dataResult = _myNoSqlTransportGrpcService.GetRowsAsync(new GetEntitiesGrpcRequest
            {
                TableName = GetTableName(typeof(T)),
                PartitionKey = partitionKey,
                RowKey = null,
                Limit = limit,
                Skip = skip
            });

            await foreach (var tableEntityContract in dataResult)
            {
                yield return tableEntityContract.DeserializeEntity<T>();
            }
        }
        
        public async IAsyncEnumerable<T> GetRowsByRowKeyAsync<T>(string rowKey, int? limit = null, int? skip = null)
            where T : IMyNoSqlDbEntity, new()
        {

            var dataResult = _myNoSqlTransportGrpcService.GetRowsAsync(new GetEntitiesGrpcRequest
            {
                TableName = GetTableName(typeof(T)),
                PartitionKey = null,
                RowKey = rowKey,
                Limit = limit,
                Skip = skip
            });

            await foreach (var tableEntityContract in dataResult)
            {
                yield return tableEntityContract.DeserializeEntity<T>();
            }
        }

        public async ValueTask<T> GetRowAsync<T>(string partitionKey, string rowKey) where T : IMyNoSqlDbEntity, new()
        {
            var tableName = GetTableName(typeof(T));
            var result = await _myNoSqlTransportGrpcService.GetRowAsync(new GetEntityGrpcRequest
            {
                TableName = tableName,
                PartitionKey = partitionKey,
                RowKey = rowKey
            });
            
            CheckResponse(result.Response, tableName);

            return result.Entity.DeserializeEntity<T>();

        }


        public MyNoSqlTransaction BeginTransaction()
        {
            return new MyNoSqlTransaction(_myNoSqlTransportGrpcService, GetTableName);
        }
    }
}