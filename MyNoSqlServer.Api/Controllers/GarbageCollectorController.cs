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
        public IActionResult CleanAndKeepMaxPartitions([FromQuery] [Required] string tableName,
            [FromQuery] [Required] int maxAmount)
        {
            
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;

            var result = table.KeepMaxPartitions(maxAmount);

            var response = Ok("Ok");

            foreach (var dbPartition in result)
                response.SynchronizeDeletePartitionAsync(table, dbPartition, DataSynchronizationPeriod.Sec1);

            return response;
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

                return await this.ResponseOk()
                    .SynchronizePartitionAsync(table, dbPartition, syncPeriod.ParseSynchronizationPeriodContract());
            }

            return this.ResponseOk();
        }
    }
}