using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Api.Controllers
{

    [ApiController]
    [Route("GarbageCollector")]
    public class GarbageCollectorController : Controller
    {
        [HttpPost("CleanAndKeepMaxPartitions")]
        public IActionResult CleanAndKeepMaxPartitions([FromQuery] [Required] string tableName,
            [FromQuery] [Required] int maxAmount)
        {
            
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;

            var result = table.KeepMaxPartitions(maxAmount);

            foreach (var dbPartition in result)
            {
                ServiceLocator.DataSynchronizer.PublishInitPartition(table, dbPartition);
                ServiceLocator.PersistenceHandler.SynchronizePartition(table, dbPartition.PartitionKey, DataSynchronizationPeriod.Sec5);
            }

            return Ok("Ok");
        }


        [HttpPost("CleanAndKeepMaxRecords")]
        public IActionResult CleanAndKeepMaxRecords(
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

                ServiceLocator.PersistenceHandler.SynchronizePartition(table, dbPartition.PartitionKey, 
                    syncPeriod.ParseDataSynchronizationPeriod(DataSynchronizationPeriod.Sec5));
            }

            return this.ResponseOk();
        }
    }
}