using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Query;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]

    public class QueryController : Controller
    {

        [HttpGet("Query")]
        public IActionResult Index([Required][FromQuery] string tableName,[Required][FromQuery] string query)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();
            
            if (string.IsNullOrEmpty(query))
                return this.QueryIsNull();
            
            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return this.TableNotFound(tableName);

            var conditions = query.ParseQueryConditions();

            var result = table.ApplyQuery(conditions);
            
            return this.ToDbRowsResult(result);
        }
        
    }
}