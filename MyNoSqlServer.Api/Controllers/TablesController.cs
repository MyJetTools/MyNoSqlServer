using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;

namespace MyNoSqlServer.Api.Controllers
{

    [ApiController]
    public class TablesController : Controller
    {
        [HttpGet("Tables/List")]
        public IActionResult List()
        {
            var list = DbInstance.GetTablesList();
            return Json(list);
        }

        [HttpPost("Tables/CreateIfNotExists")]
        public IActionResult CreateIfNotExists([Required] [FromQuery] string tableName)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.ResponseConflict("Please specify table name");

            DbInstance.CreateTableIfNotExists(tableName);
            return this.ResponseOk();
        }

        [HttpPost("Tables/Create")]
        public IActionResult Create([Required] [FromQuery] string tableName)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            if (string.IsNullOrEmpty(tableName))
                return this.ResponseConflict("Please specify table name");

            if (DbInstance.CreateTable(tableName))
                return this.ResponseOk();

            return this.ResponseConflict("Can not create table: " + tableName);
        }

        [HttpDelete("Tables/Clean")]
        public ValueTask<IActionResult> Clean([Required] [FromQuery] string tableName, [FromQuery] string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return new ValueTask<IActionResult>(shutDown);

            if (string.IsNullOrEmpty(tableName))
                return new ValueTask<IActionResult>(this.ResponseConflict("Please specify table name"));


            var table = DbInstance.GetTable(tableName);
            if (table == null)
                return new ValueTask<IActionResult>(this.TableNotFound(tableName));

            table.Clean();

            ServiceLocator.DataSynchronizer.PublishInitTable(table);

            return Ok().SynchronizeTableAsync(table, syncPeriod.ParseSynchronizationPeriod());

        }    

    }
    
}
