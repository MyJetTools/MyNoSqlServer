using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.SnapshotSaver;

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
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return this.TableNotFound(tableName);

            var result = table.KeepMaxPartitions(maxAmount);

            var response = Ok("Ok");

            foreach (var dbPartition in result)
                response.SynchronizeDeletePartitionAsync(table, dbPartition, DataSynchronizationPeriod.Sec1);

            return response;
        }


        [HttpPost("CleanAndKeepMaxRecords")]
        public ValueTask<IActionResult> CleanAndKeepMaxRecords(
            [FromQuery][Required] string tableName,
            [FromQuery][Required]string partitionKey, [FromQuery][Required]int maxAmount,
            [FromQuery] string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return new ValueTask<IActionResult>(shutDown);

            if (string.IsNullOrEmpty(tableName))
                return new ValueTask<IActionResult>(this.TableNameIsNull());

            if (string.IsNullOrEmpty(partitionKey))
                return new ValueTask<IActionResult>(this.PartitionKeyIsNull());

            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return new ValueTask<IActionResult>(this.TableNotFound(tableName));


            var (dbPartition, dbRows) = table.CleanAndKeepLastRecords(partitionKey, maxAmount);

            if (dbPartition != null)
            {
                ServiceLocator.DataSynchronizer.SynchronizeDelete(table, dbRows);

                return this.ResponseOk()
                    .SynchronizePartitionAsync(table, dbPartition, syncPeriod.ParseSynchronizationPeriod());
            }

            return new ValueTask<IActionResult>(this.ResponseOk());
        }
    }
}