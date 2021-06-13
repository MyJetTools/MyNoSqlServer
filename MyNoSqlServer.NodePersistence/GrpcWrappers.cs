using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.NodePersistence
{
    public static class GrpcWrappers
    {


        public static SyncGrpcHeader[] ToGrpcHeaders(this Dictionary<string, string> src)
        {
            if (src == null)
                return Array.Empty<SyncGrpcHeader>();
            
            
            if (src.Count == 0)
                return Array.Empty<SyncGrpcHeader>();

            return src.Select(itm => new SyncGrpcHeader
            {
                Key = itm.Key,
                Value = itm.Value
            }).ToArray();
        }


        public static ValueTask SavePartitionSnapshotAsync(this IMyNoSqlServerNodePersistenceGrpcService grpcService, DbTable table,  
            PartitionSnapshot partitionSnapshot,  IMyNoSqlNodePersistenceSettings settings, ISettingsLocation location, Dictionary<string, string> headers)
        {
            var model = new SavePartitionSnapshotGrpcModel
            {
                Location = location.Location,
                TableName = table.Name,
                PartitionKey = partitionSnapshot.PartitionKey,
                Content = partitionSnapshot.Snapshot,
                Headers = headers.ToGrpcHeaders()
            };

            var payloads = model.CompressAndSplitAsync(settings.MaxPayloadSize, settings.CompressData);

            return grpcService.SavePartitionSnapshotAsync(payloads);
        }
        
        public static ValueTask SaveTableAsync(this IMyNoSqlServerNodePersistenceGrpcService grpcService, DbTable table, 
            IMyNoSqlNodePersistenceSettings settings, ISettingsLocation location, Dictionary<string, string> headers)
        {
            var model = new SaveTableSnapshotGrpcModel
            {
                Location = location.Location,
                TableName = table.Name,
                Content = table.GetSnapshotAsArray(),
                Headers = headers.ToGrpcHeaders()
            };

            var payloads = model.CompressAndSplitAsync(settings.MaxPayloadSize, settings.CompressData);

            return grpcService.SavePartitionSnapshotAsync(payloads);
        }

        
    }
}