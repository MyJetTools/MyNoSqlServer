using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Api.Models;

namespace MyNoSqlServer.Api.Controllers
{
    [ApiController]
    public class MultiPartController : Controller
    {

        [HttpGet("/Multipart/First")]
        public IActionResult GetFirst([Required] string tableName)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            var (errorResult, table) = this.GetTable(tableName);

            if (errorResult != null)
                return errorResult;

            var partitions = table.GetAllPartitionNames();

            var requestId = ServiceLocator.MultiPartGetSnapshots.Init(tableName, partitions);

            ServiceLocator.Logger.WriteInfo("New Transaction Start. Id:" + requestId, "Count: " + partitions.Count);

            var result = new MultiPartFirstRequestModel
            {
                SnapshotId = requestId
            };

            return Json(result);
        }

        [HttpGet("/Multipart/Next")]
        public IActionResult GetNext([Required] string requestId)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            var (tableName, partitionKey) = ServiceLocator.MultiPartGetSnapshots.GetNextPartitionId(requestId);

            if (tableName == null)
                return NotFound();

            var (errorResult, table) = this.GetTable(tableName);

            if (errorResult != null)
                return errorResult;

            var entities = table.GetRecords(partitionKey, null, null);
            return this.ToDbRowsResult(entities);
        }
    }
}