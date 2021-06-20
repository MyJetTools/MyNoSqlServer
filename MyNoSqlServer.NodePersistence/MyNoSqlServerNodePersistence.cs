using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.TransactionEvents;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.NodePersistence
{

    public interface IMyNoSqlNodePersistenceSettings
    {
        bool CompressData { get; }
        int MaxPayloadSize { get; }
    }
    
    
    public class MyNoSqlServerNodePersistence : ITablePersistenceStorage
    {
        private readonly IMyNoSqlServerNodePersistenceGrpcService _grpcService;
        private readonly IMyNoSqlNodePersistenceSettings _settings;
        private readonly ISettingsLocation _location;

        private string _remoteLocation;

        public MyNoSqlServerNodePersistence(IMyNoSqlServerNodePersistenceGrpcService grpcService,
            IMyNoSqlNodePersistenceSettings settings, ISettingsLocation location)
        {
            _grpcService = grpcService;
            _settings = settings;
            _location = location;
        }

        private async Task<string> LoadRemoteLocation()
        {
            var result = await _grpcService.PingAsync(new PingGrpcRequest
            {
                Location = _location.Location
            });

            _remoteLocation = result.Location;
            return _remoteLocation;
        }

        private ValueTask<string> GetRemoteLocation()
        {
            return _remoteLocation != null 
                ? new ValueTask<string>(_remoteLocation) 
                : new ValueTask<string>(LoadRemoteLocation());
        }


        public ValueTask SaveTableAttributesAsync(DbTable dbTable, UpdateTableAttributesTransactionEvent data)
        {
            return _grpcService.SetTableAttributesAsync(new SetTableAttributesGrpcRequest
            {
                Locations = data.Attributes.Locations,
                TableName = dbTable.Name,
                Persist = dbTable.Persist,
                MaxPartitionsAmount = dbTable.MaxPartitionsAmount,
                Headers = data.Attributes.Headers.ToGrpcHeaders()
            });
        }

        public ValueTask SaveTableSnapshotAsync(DbTable dbTable, InitTableTransactionEvent data)
        {
            var model = new WriteTableSnapshotGrpcModel
            {
                Locations = data.Attributes.Locations,
                TableName = data.TableName,
                TablePartitions =  data.Snapshot.DataRowsToGrpcContent(),
                Headers = data.Attributes.Headers.ToGrpcHeaders()
            };

            var payloads = model.SplitAndPublish(_settings.MaxPayloadSize, _settings.CompressData).ToAsyncEnumerable();

            return _grpcService.SaveTableSnapshotAsync(payloads);
        }

        public ValueTask SavePartitionSnapshotAsync(DbTable dbTable, InitPartitionsTransactionEvent data)
        {
            var model = new WritePartitionSnapshotGrpcModel
            {
                Locations = data.Attributes.Locations,
                TableName = data.TableName,
                PartitionsToBeInitialized = data.Partitions.DataRowsToGrpcContent(),
                Headers = data.Attributes.Headers.ToGrpcHeaders()
            };

            var payloads = model.SplitAndPublish(_settings.MaxPayloadSize, _settings.CompressData).ToAsyncEnumerable();

            return _grpcService.SavePartitionSnapshotAsync(payloads);
        }

        public ValueTask SaveRowUpdatesAsync(DbTable dbTable, UpdateRowsTransactionEvent eventData)
        {

            var model = new WriteRowsUpdateGrpcModel
            {
                Locations = eventData.Attributes.Locations,
                Headers = eventData.Attributes.Headers.ToGrpcHeaders(),
                TableName = eventData.TableName,
                Rows = eventData.RowsByPartition.DataRowsToGrpcContent()
            };
            
            var payloads = model.SplitAndPublish(_settings.MaxPayloadSize, _settings.CompressData).ToAsyncEnumerable();

            return _grpcService.SaveRowsSnapshotAsync(payloads);
        }

        public ValueTask SaveRowDeletesAsync(DbTable dbTable, DeleteRowsTransactionEvent eventData)
        {

            var model = new DeleteRowsGrpcRequest
            {
                Locations = eventData.Attributes.Locations,
                Headers = eventData.Attributes.Headers.ToGrpcHeaders(),
                TableName = eventData.TableName,
                RowsToDelete = eventData.Rows.Select(itm => new RowsToDeleteByPartitionGrpc
                {
                    PartitionKey = itm.Key,
                    RowKeys = itm.Value.ToArray()
                }).ToArray()
            };
            
            var payloads = model.SplitAndPublish(_settings.MaxPayloadSize, _settings.CompressData).ToAsyncEnumerable();

            return _grpcService.SaveRowsSnapshotAsync(payloads);
        }

        public ValueTask FlushIfNeededAsync()
        {
            return new ValueTask();
        }

        public async IAsyncEnumerable<ITableLoader> LoadTablesAsync()
        {
            await foreach (var table in _grpcService.GetTablesAsync())
            {
                yield return new MyNoSqlServerTableLoader(table.TableName, table.Attributes.Persist, _grpcService, _settings);
            }
        }

        public bool HasDataAtSaveProcess => false;
    }
}