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
        public IActionResult CreateIfNotExists([Required] [FromQuery] string tableName)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.ResponseConflict(OperationResult.TableNameIsEmpty);

            ServiceLocator.DbInstance.CreateTableIfNotExists(tableName);
            return this.ResponseOk();
        }

        [HttpPost("Tables/Create")]
        public IActionResult Create([Required] [FromQuery] string tableName)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.ResponseConflict(OperationResult.TableNameIsEmpty);

            if (ServiceLocator.DbInstance.CreateTable(tableName))
                return this.ResponseOk();

            return this.ResponseConflict(OperationResult.CanNotCreateObject);
        }

        [HttpDelete("Tables/Clean")]
        public ValueTask<IActionResult> Clean([Required] [FromQuery] string tableName, [FromQuery] string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return new ValueTask<IActionResult>(shutDown);

            if (string.IsNullOrEmpty(tableName))
                return new ValueTask<IActionResult>(this.ResponseConflict(OperationResult.TableNameIsEmpty));


            var table = ServiceLocator.DbInstance.TryGetTable(tableName);
            if (table == null)
                return new ValueTask<IActionResult>(this.TableNotFound());

            table.Clean();

            ServiceLocator.DataSynchronizer.PublishInitTable(table);

            return Ok().SynchronizeTableAsync(table, syncPeriod.ParseSynchronizationPeriodContract());

        }    

    }
    
}
