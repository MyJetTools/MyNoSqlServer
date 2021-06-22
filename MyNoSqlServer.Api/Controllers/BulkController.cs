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
    public class BulkController : Controller
    {
        [HttpPost("Bulk/InsertOrReplace")]
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

            var result =ServiceLocator.DbOperations.BulkInsertOrReplace(table, entitiesToInsert,
                HttpContext.GetRequestAttributes(syncPeriod));
            
            return this.GetResult(result);

        }

        private void CleanPartitionAndBulkInsert(DbTable table, IEnumerable<IMyMemory> entitiesToInsert, string partitionKey, 
            string syncPeriod)
        {
            ServiceLocator.DbOperations
                .CleanPartitionAndBulkInsert(table, partitionKey, entitiesToInsert, HttpContext.GetRequestAttributes(syncPeriod));
        }
        
        
        private void CleanTableAndBulkInsert(DbTable table, IEnumerable<IMyMemory> entitiesToInsert, 
            string syncPeriod)
        {
            ServiceLocator.DbOperations
                .CleanTableAndBulkInsert(table,entitiesToInsert, HttpContext.GetRequestAttributes(syncPeriod));
        }


        [HttpPost("Bulk/CleanAndBulkInsert")]
        public async ValueTask<IActionResult> CleanAndBulkInsert([Required] [FromQuery] string tableName,
            [FromQuery] string partitionKey,
            [FromQuery] string syncPeriod)
        {
            var (getTableResult, table) = this.GetTable(tableName, partitionKey);
            
            if (getTableResult != null)
                return getTableResult;
            
            var body = await Request.BodyAsIMemoryAsync();
            
            var entitiesToInsert = body.SplitJsonArrayToObjects();

            if (string.IsNullOrEmpty(partitionKey))
                CleanTableAndBulkInsert(table, entitiesToInsert, syncPeriod);
            else
                CleanPartitionAndBulkInsert(table, entitiesToInsert, partitionKey, syncPeriod);

            return this.ResponseOk();
        }

        [HttpPost("Bulk/Delete")]
        public IActionResult Delete([Required] [FromQuery] string tableName, [FromQuery] string syncPeriod,
            [FromBody] [Required] Dictionary<string, List<string>> partitionsAndRows)
        {
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;
            
            ServiceLocator.DbOperations.BulkDelete(table, partitionsAndRows, HttpContext.GetRequestAttributes(syncPeriod));

            return this.ResponseOk();
        }

    }

}