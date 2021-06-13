﻿using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.NodePersistence
{

    public interface IMyNoSqlNodePersistenceSettings
    {
        bool CompressData { get; }
        int MaxPayloadSize { get; }
    }
    
    
    public class MyNoSqlServerNodePersistence : ISnapshotStorage
    {
        private readonly IMyNoSqlServerNodePersistenceGrpcService _grpcService;
        private readonly IMyNoSqlNodePersistenceSettings _settings;
        private readonly ISettingsLocation _location;

        public MyNoSqlServerNodePersistence(IMyNoSqlServerNodePersistenceGrpcService grpcService,
            IMyNoSqlNodePersistenceSettings settings, ISettingsLocation location)
        {
            _grpcService = grpcService;
            _settings = settings;
            _location = location;
        }
        
        public ValueTask SavePartitionSnapshotAsync(DbTable dbTable, PartitionSnapshot partitionSnapshot)
        {
            return _grpcService.SavePartitionSnapshotAsync(dbTable, partitionSnapshot, _settings, _location);
        }

        public ValueTask SaveTableSnapshotAsync(DbTable dbTable)
        {
            return _grpcService.SaveTableAsync(dbTable, _settings, _location);
        }

        public ValueTask DeleteTablePartitionAsync(DbTable dbTable, string partitionKey)
        {
            return _grpcService.DeleteTablePartitionsAsync(new DeleteTablePartitionGrpcRequest
            {
                Location = _location.Location,
                TableName = dbTable.Name,
                PartitionKeys = new[] { partitionKey }
            });
        }

        public async IAsyncEnumerable<ITableLoader> LoadTablesAsync()
        {
            await foreach (var table in _grpcService.GetTablesAsync())
            {
                yield return new MyNoSqlServerTableLoader(table.TableName, table.Persist, _grpcService, _settings);
            }
        }

        public ValueTask SetTableAttributesAsync(DbTable dbTable)
        {
            return _grpcService.SetTableAttributesAsync(new SetTableAttributesGrpcRequest
            {
                Location = _location.Location,
                TableName = dbTable.Name,
                Persist = dbTable.Persist,
                MaxPartitionsAmount = dbTable.MaxPartitionsAmount
            });
        }
    }
}