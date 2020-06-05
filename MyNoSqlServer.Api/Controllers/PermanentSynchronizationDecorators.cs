using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.Api.Controllers
{
    public static class PermanentSynchronizationDecorators
    {
        
        public static ValueTask<IActionResult> SynchronizeTableAsync(this IActionResult result, DbTable dbTable, DataSynchronizationPeriod period)
        {
            if (ServiceLocator.SnapshotStorage == null)
                return new ValueTask<IActionResult>(result);
            
            if (period == DataSynchronizationPeriod.Immediately)
                return result.ResponseWithActionAsync(() => ServiceLocator
                    .SnapshotStorage
                    .SaveTableSnapshotAsync(dbTable));
            
            ServiceLocator.SnapshotSaverScheduler.SynchronizeTable(dbTable, period);
            return new ValueTask<IActionResult>(result);
        }
        
        public static ValueTask<IActionResult> SynchronizePartitionAsync(this IActionResult result, DbTable dbTable, DbPartition partitionToSave,
            DataSynchronizationPeriod period)
        {
            
            if (ServiceLocator.SnapshotStorage == null)
                return new ValueTask<IActionResult>(result);            

            if (period == DataSynchronizationPeriod.Immediately)
            {
                var partitionSnapshot = PartitionSnapshot.Create(dbTable, partitionToSave);

                return result.ResponseWithActionAsync(() => ServiceLocator
                    .SnapshotStorage
                    .SavePartitionSnapshotAsync(partitionSnapshot));
            }

            ServiceLocator.SnapshotSaverScheduler.SynchronizePartition(dbTable, partitionToSave, period);
            return new ValueTask<IActionResult>(result);
        }

        public static ValueTask<IActionResult> SynchronizeDeletePartitionAsync(this IActionResult result, DbTable dbTable,
            DbPartition dbPartition,
            DataSynchronizationPeriod period)
        {
            
            if (ServiceLocator.SnapshotStorage == null)
                return new ValueTask<IActionResult>(result);

            if (period == DataSynchronizationPeriod.Immediately)
                return result.ResponseWithActionAsync(() => ServiceLocator
                    .SnapshotStorage
                    .DeleteTablePartitionAsync(dbTable.Name, dbPartition.PartitionKey));

            ServiceLocator.SnapshotSaverScheduler.SynchronizePartition(dbTable, dbPartition, period);
            return new ValueTask<IActionResult>(result);
        }
        
        
    }


}