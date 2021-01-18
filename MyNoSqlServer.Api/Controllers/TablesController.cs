using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Models;

namespace MyNoSqlServer.Api.Controllers
{

    [ApiController]
    public class TablesController : Controller
    {
        [HttpGet("/Tables/List")]
        public IActionResult List()
        {
            var tableNames = ServiceLocator.DbInstance.TableNames;
            return Json(tableNames);
        }

        [HttpGet("/Tables")]
        public IActionResult Get()
        {
            var tableNames = ServiceLocator.DbInstance.TableNames;

            var result = tableNames.Select(name =>
                {
                    var dbTable = ServiceLocator.DbInstance.TryGetTable(name);
                    return new
                    {
                        Name = name,
                        RecordsCount = dbTable.GetRecordsCount(),
                        PartitionsCount = dbTable.GetPartitionsCount(),
                        DataSize = dbTable.GetDataSize()
                    };
                }
            );

            return Json(result);
        }

        [HttpPost("Tables/CreateIfNotExists")]
        public async ValueTask<IActionResult> CreateIfNotExists([Required] [FromQuery] string tableName, 
            [FromQuery]string persist)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);
            
            var persistIt = persist != null;

            if (persist != null)
                persistIt = persist == "1" || persist.ToLower() == "true";

            await ServiceLocator.DbInstance.CreateTableIfNotExistsAsync(tableName, persistIt, DateTime.UtcNow);
            return this.ResponseOk();
        }

        [HttpPost("Tables/Create")]
        public async ValueTask<IActionResult> Create([Required] [FromQuery] string tableName, [FromQuery]string persist)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            var persistIt = persist != null;

            if (persist != null)
                persistIt = persist == "1" || persist.ToLower() == "true";

            var result = await ServiceLocator.DbInstance.CreateTableAsync(tableName, persistIt, DateTime.UtcNow);
            return this.GetResult(result);
        }

        [HttpDelete("Tables/Clean")]
        public async ValueTask<IActionResult> Clean([Required] [FromQuery] string tableName, [FromQuery] string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;

            await ServiceLocator.DbTableWriteOperations.ClearTableAsync(table,
                syncPeriod.ParseSynchronizationPeriodContract());

            return this.ResponseOk();

        }    

        
                
        [HttpDelete("/Tables")]
        public async ValueTask<IActionResult> DeleteTableAsync([Required][FromQuery] string tableName)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            var opResult = await ServiceLocator.DbInstance.DeleteTableAsync(tableName);
            return this.GetResult(opResult);
        }  

    }
    
}
