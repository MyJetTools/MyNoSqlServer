using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Query;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]

    public class QueryController : Controller
    {

        [HttpGet("Query")]
        public IActionResult Index([Required][FromQuery] string tableName,[Required][FromQuery] string query)
        {
            
            if (string.IsNullOrEmpty(query))
                return this.GetResult(OperationResult.QueryIsNull);
            
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;

            var conditions = query.ParseQueryConditions();

            var result = table.ApplyQuery(conditions);
            
            return this.ToDbRowsResult(result);
        }
        
    }
}