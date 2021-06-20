using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;

namespace MyNoSqlServer.Api.Controllers
{

    [ApiController]
    [Route("GarbageCollector")]
    public class GarbageCollectorController : Controller
    {
        [HttpPost("CleanAndKeepMaxPartitions")]
        public IActionResult CleanAndKeepMaxPartitions([FromQuery] [Required] string tableName,
            [FromQuery] [Required] int maxAmount, [FromQuery] string syncPeriod)
        {
            
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;

            table.KeepMaxPartitions(maxAmount, HttpContext.GetRequestAttributes(syncPeriod));

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

            table.CleanAndKeepLastRecords(partitionKey, maxAmount, HttpContext.GetRequestAttributes(syncPeriod));

            return this.ResponseOk();
        }
    }
}