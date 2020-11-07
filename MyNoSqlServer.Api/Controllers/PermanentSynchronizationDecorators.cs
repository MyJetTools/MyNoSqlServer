using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.Api.Controllers
{
    public static class PermanentSynchronizationDecorators
    {


        public static ValueTask SynchronizeDeletePartitionAsync(this DbTable dbTable,
            string partitionKey, DataSynchronizationPeriod period)
        {
            
            if (ServiceLocator.SnapshotStorage == null)
                return new ValueTask();

            if (period == DataSynchronizationPeriod.Immediately)
                return ServiceLocator
                    .SnapshotStorage
                    .DeleteTablePartitionAsync(dbTable.Name, partitionKey);

            ServiceLocator.SnapshotSaverScheduler.SynchronizePartition(dbTable, partitionKey, period);
            return new ValueTask();
        }
        
        
    }


}