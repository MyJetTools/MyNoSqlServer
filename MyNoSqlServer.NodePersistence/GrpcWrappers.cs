using System.Threading.Tasks;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.NodePersistence
{
    public static class GrpcWrappers
    {


        public static ValueTask SavePartitionSnapshotAsync(this IMyNoSqlServerNodePersistenceGrpcService grpcService, DbTable table,  
            PartitionSnapshot partitionSnapshot, IMyNoSqlNodePersistenceSettings settings, ISettingsLocation location)
        {
            var model = new SavePartitionSnapshotGrpcModel
            {
                Location = location.Location,
                TableName = table.Name,
                PartitionKey = partitionSnapshot.PartitionKey,
                Content = partitionSnapshot.Snapshot
            };

            var payloads = model.CompressAndSplitAsync(settings.MaxPayloadSize, settings.CompressData);

            return grpcService.SavePartitionSnapshotAsync(payloads);
        }
        
        public static ValueTask SaveTableAsync(this IMyNoSqlServerNodePersistenceGrpcService grpcService, DbTable table, 
            IMyNoSqlNodePersistenceSettings settings, ISettingsLocation location)
        {
            var model = new SaveTableSnapshotGrpcModel
            {
                Location = location.Location,
                TableName = table.Name,
                Content = table.GetSnapshotAsArray()
            };

            var payloads = model.CompressAndSplitAsync(settings.MaxPayloadSize, settings.CompressData);

            return grpcService.SavePartitionSnapshotAsync(payloads);
        }

        
    }
}