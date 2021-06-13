using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Api.Controllers
{

    [ApiController]
    public class TablesController : Controller
    {
        [HttpGet("Tables/List")]
        public IActionResult List()
        {
            var list = ServiceLocator.DbInstance.GetTables();
            return Json(list.Select(itm => new
            {
                itm.Name,
                itm.Persist,
                MaxPartitionsAmount =  itm.MaxPartitionsAmount == 0 ? "Unlimited" : itm.MaxPartitionsAmount.ToString()
            }));
        }

        [HttpPost("Tables/CreateIfNotExists")]
        public IActionResult CreateIfNotExists([Required][FromQuery] string tableName, [FromQuery] string persist,
            [FromQuery] string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            ServiceLocator.DbInstance.CreateTableIfNotExists(tableName, persist != "0", HttpContext.GetRequestAttributes(syncPeriod));
            return this.ResponseOk();
        }

        [HttpPost("Tables/Create")]
        public IActionResult Create([Required][FromQuery] string tableName, [FromQuery] string persist,
            [FromQuery] string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            tableName = tableName.ToLower();

            if (ServiceLocator.DbInstance.CreateTable(tableName, persist != "0", HttpContext.GetRequestAttributes(syncPeriod)))
                return this.ResponseOk();

            return this.GetResult(OperationResult.CanNotCreateObject);
        }

        [HttpDelete("Tables/Clean")]
        public IActionResult Clean([Required][FromQuery] string tableName, [FromQuery] string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            var (getTableResult, table) = this.GetTable(tableName);

            if (getTableResult != null)
                return getTableResult;

            table.Clear(HttpContext.GetRequestAttributes(syncPeriod));

            return Ok();

        }
        [HttpPost("Tables/UpdatePersist")]
        public IActionResult UpdatePersist([Required][FromQuery] string tableName,
            [FromQuery] string persist, [FromQuery] string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            var (getTableResult, table) = this.GetTable(tableName);

            if (getTableResult != null)
                return getTableResult;

            var persistAsBool = persist != "0";

            table.UpdatePersist(persistAsBool,HttpContext.GetRequestAttributes(syncPeriod));

            return Ok();
        }


        [HttpGet("Tables/PartitionsCount")]
        public IActionResult PartitionsCount([Required][FromQuery] string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            var (getTableResult, table) = this.GetTable(tableName);

            if (getTableResult != null)
                return getTableResult;

            var result = table.GetPartitionsCount();

            return Content(result.ToString());
        }

    }

}
