using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.Domains.Db.Operations;

namespace MyNoSqlServer.Api.Controllers
{

    [ApiController]
    [Route("GarbageCollector")]
    public class GarbageCollectorController : Controller
    {
        [HttpPost("CleanAndKeepMaxPartitions")]
        public ValueTask<IActionResult> CleanAndKeepMaxPartitions([FromQuery] [Required] string tableName,
            [FromQuery] [Required] int maxAmount)
        {
            
            var (getTableResult, dbTable) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return new ValueTask<IActionResult>(getTableResult);


            return ServiceLocator.DbTableWriteOperations
                .KeepMaxPartitionsAmountAsync(dbTable, maxAmount)
                .GetResponseOkAsync(this);

        }


        [HttpPost("CleanAndKeepMaxRecords")]
        public ValueTask<IActionResult> CleanAndKeepMaxRecords(
            [FromQuery] [Required] string tableName,
            [FromQuery] [Required] string partitionKey, [FromQuery] [Required] int maxAmount,
            [FromQuery] string syncPeriod)
        {
            var (getTableResult, table) = this.GetTable(tableName, partitionKey);

            if (getTableResult != null)
                return new ValueTask<IActionResult>(getTableResult);

            return
                ServiceLocator
                    .DbTableWriteOperations
                    .CleanAndKeepLastRecordsAsync(table, partitionKey, maxAmount,
                        syncPeriod.ParseSynchronizationPeriodContract())
                    .GetResponseOkAsync(this);

        }

        [HttpPost("PushRowsExpirations")]
        public async ValueTask<IActionResult> PushRowsExpirations()
        {
            await ServiceLocator.ExpiredEntitiesGarbageCollector.DetectAndExpireAsync(DateTime.UtcNow);
            return this.GetResult(OperationResult.Ok);
        }
    }
}