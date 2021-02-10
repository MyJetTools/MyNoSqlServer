using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]
    [Route("Bulk/[Action]")]
    public class BulkController : Controller
    {
        [HttpPost]
        public async ValueTask<IActionResult> InsertOrReplace([Required][FromQuery] string tableName, 
            [FromQuery]string syncPeriod)
        {
            
            
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;
            
            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            var theSyncPeriod = syncPeriod.ParseSynchronizationPeriodContract();

            if (theSyncPeriod == DataSynchronizationPeriod.Immediately)
                return Conflict("Bulk insert does not support immediate persistence");
            
            var table = ServiceLocator.DbInstance.TryGetTable(tableName);
            
            if (table == null)
                return this.GetResult(OperationResult.TableNotFound);

            var body = await Request.BodyAsIMemoryAsync();

            var entitiesToInsert = body.SplitJsonArrayToObjects();

            var (dbPartitions, dbRows) = table.BulkInsertOrReplace(entitiesToInsert);

            ServiceLocator.DataSynchronizer.SynchronizeUpdate(table, dbRows);
            
            foreach (var dbPartition in dbPartitions)
                ServiceLocator.SnapshotSaverScheduler.SynchronizePartition(table, dbPartition, theSyncPeriod);
            
            return this.ResponseOk();

        }

        private static void CleanPartitionAndBulkInsert(DbTable table, IEnumerable<IMyMemory> entitiesToInsert, string partitionKey, 
            DataSynchronizationPeriod syncPeriod)
        {
            var partitionsToSynchronize = table.CleanAndBulkInsert(partitionKey, entitiesToInsert);

            foreach (var dbPartition in partitionsToSynchronize)
            {
                ServiceLocator.SnapshotSaverScheduler.SynchronizePartition(table, dbPartition, syncPeriod);
                ServiceLocator.DataSynchronizer?.PublishInitPartition(table, dbPartition); 
            }
        }
        
        
        private static void CleanTableAndBulkInsert(DbTable table, IEnumerable<IMyMemory> entitiesToInsert, 
            DataSynchronizationPeriod syncPeriod)
        {
            table.CleanAndBulkInsert(entitiesToInsert);
            
            ServiceLocator.SnapshotSaverScheduler.SynchronizeTable(table, syncPeriod);
            ServiceLocator.DataSynchronizer?.PublishInitTable(table);
        }


        [HttpPost]
        public async ValueTask<IActionResult> CleanAndBulkInsert([Required] [FromQuery] string tableName,
            [FromQuery] string partitionKey,
            [FromQuery] string syncPeriod)
        {
            var (getTableResult, table) = this.GetTable(tableName, partitionKey);
            
            if (getTableResult != null)
                return getTableResult;
            
            var theSyncPeriod = syncPeriod.ParseSynchronizationPeriodContract();

            if (theSyncPeriod == DataSynchronizationPeriod.Immediately)
                return Conflict("CleanAndBulkInsert insert does not support immediate persistence");

            var body = await Request.BodyAsIMemoryAsync();
            
            var entitiesToInsert = body.SplitJsonArrayToObjects();

            if (string.IsNullOrEmpty(partitionKey))
                CleanTableAndBulkInsert(table, entitiesToInsert, theSyncPeriod);
            else
                CleanPartitionAndBulkInsert(table, entitiesToInsert, partitionKey, theSyncPeriod);

            return this.ResponseOk();
        }

    }

}