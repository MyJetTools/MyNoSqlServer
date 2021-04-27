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

            var records = table.GetAllRecords(null);

            var requestId = ServiceLocator.MultiPartGetSnapshots.Init(tableName, records);

            var result = new MultiPartFirstRequestModel
            {
                SnapshotId = requestId
            };

            return Json(result);
        }

        [HttpGet("/Multipart/Next")]
        public IActionResult GetNext([Required] string requestId, [Required] int maxRecordsCount)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            var dbRows = ServiceLocator.MultiPartGetSnapshots.GetNextPartitionId(requestId, maxRecordsCount);

            if (dbRows.Count == 0)
                return NotFound();

            return this.ToDbRowsResult(dbRows);
        }
    }
}