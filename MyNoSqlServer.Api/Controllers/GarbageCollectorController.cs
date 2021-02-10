using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Models;

namespace MyNoSqlServer.Api.Controllers
{

    [ApiController]
    [Route("GarbageCollector")]
    public class GarbageCollectorController : Controller
    {
        [HttpPost("CleanAndKeepMaxPartitions")]
        public async ValueTask<IActionResult> CleanAndKeepMaxPartitions([FromQuery] [Required] string tableName,
            [FromQuery] [Required] int maxAmount)
        {
            
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;

            var result = table.KeepMaxPartitions(maxAmount);

            foreach (var dbPartition in result)
            {
                ServiceLocator.DataSynchronizer.PublishInitPartition(table, dbPartition);
                await ServiceLocator.PersistenceHandler.SynchronizePartitionAsync(table, dbPartition.PartitionKey, DataSynchronizationPeriod.Sec5);
            }

            return Ok("Ok");
        }


        [HttpPost("CleanAndKeepMaxRecords")]
        public async ValueTask<IActionResult> CleanAndKeepMaxRecords(
            [FromQuery][Required] string tableName,
            [FromQuery][Required]string partitionKey, [FromQuery][Required]int maxAmount,
            [FromQuery] string syncPeriod)
        {

            var (getTableResult, table) = this.GetTable(tableName, partitionKey);
            
            if (getTableResult != null)
                return getTableResult;


            var (dbPartition, dbRows) = table.CleanAndKeepLastRecords(partitionKey, maxAmount);

            if (dbPartition != null)
            {
                ServiceLocator.DataSynchronizer.SynchronizeDelete(table, dbRows);

                await ServiceLocator.PersistenceHandler.SynchronizePartitionAsync(table, dbPartition.PartitionKey, 
                    syncPeriod.ParseDataSynchronizationPeriod(DataSynchronizationPeriod.Sec5));
            }

            return this.ResponseOk();
        }
    }
}