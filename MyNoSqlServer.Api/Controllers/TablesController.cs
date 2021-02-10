using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Models;

namespace MyNoSqlServer.Api.Controllers
{

    [ApiController]
    public class TablesController : Controller
    {
        [HttpGet("Tables/List")]
        public IActionResult List()
        {
            var list = ServiceLocator.DbInstance.GetTablesList();
            return Json(list);
        }

        [HttpPost("Tables/CreateIfNotExists")]
        public IActionResult CreateIfNotExists([Required] [FromQuery] string tableName, [FromQuery]string persist)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            ServiceLocator.DbInstance.CreateTableIfNotExists(tableName, persist != "0");
            return this.ResponseOk();
        }

        [HttpPost("Tables/Create")]
        public IActionResult Create([Required] [FromQuery] string tableName, [FromQuery]string persist)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            if (ServiceLocator.DbInstance.CreateTable(tableName, persist != "0"))
                return this.ResponseOk();

            return this.GetResult(OperationResult.CanNotCreateObject);
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

            table.Clean();

            ServiceLocator.DataSynchronizer.PublishInitTable(table);

            return await Ok().SynchronizeTableAsync(table, syncPeriod.ParseSynchronizationPeriodContract());

        }    

    }
    
}
