using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Models;
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

            var (result, dbTable) = this.GetTable(tableName);

            if (result != null)
                return result;

            var body = await Request.BodyAsIMemoryAsync();

            var entitiesToInsert = body
                .SplitJsonArrayToObjects()
                .Select(itm => itm.ParseDynamicEntity());

            await ServiceLocator.DbTableWriteOperations.BulkInsertOrReplaceAsync(dbTable, entitiesToInsert, 
                syncPeriod.ParseDataSynchronizationPeriod(DataSynchronizationPeriod.Sec5));
            
            return this.ResponseOk();
        }


        [HttpPost]
        public async ValueTask<IActionResult> CleanAndBulkInsert([Required] [FromQuery] string tableName,
            [FromQuery] string partitionKey,
            [FromQuery] string syncPeriod)
        {
            var (getTableResult, dbTable) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;
            
            var theSyncPeriod = syncPeriod.ParseSynchronizationPeriodContract();

            var body = await Request.BodyAsIMemoryAsync();
            
            var entitiesToInsert = body
                .SplitJsonArrayToObjects()
                .Select(arraySpan => arraySpan.ParseDynamicEntity())
                .ToList();

            if (string.IsNullOrEmpty(partitionKey))
                await ServiceLocator.DbTableWriteOperations.ClearTableAndBulkInsertAsync(dbTable, entitiesToInsert,
                    theSyncPeriod);
            else
                await ServiceLocator.DbTableWriteOperations.ClearPartitionAndBulkInsertOrUpdateAsync(dbTable,
                    partitionKey, entitiesToInsert, theSyncPeriod);

            return this.ResponseOk();
        }

    }

}