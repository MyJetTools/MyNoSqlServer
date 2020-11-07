using System;
using System.ComponentModel.DataAnnotations;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.Domains.Db.Operations;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Api.Controllers
{
    public class RowsController : Controller
    {
        
        [HttpPost("Rows/SinglePartitionMultipleRows")]
        public IActionResult SinglePartitionMultipleRows([Required][FromQuery] string tableName, [Required][FromQuery] string partitionKey,
            [Required][FromBody] string[] rowKeys, [FromQuery] DateTime? updateExpiresAt)
        {
            
            var (getTableResult, dbTable) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;

            if (rowKeys == null || rowKeys.Length == 0)
                return this.ToDbRowsResult(Array.Empty<DbRow>());

            var result =
                updateExpiresAt == null
                    ? dbTable.GetRows(partitionKey, rowKeys)
                    : ServiceLocator.DbTableReadOperations.GetRows(dbTable, partitionKey, rowKeys, updateExpiresAt.Value);
            
            return this.ToDbRowsResult(result);
        }
        
        
        [HttpGet("Rows/HighestRowAndBelow")]
        public IActionResult HighestRowAndBelow([Required][FromQuery] string tableName, [Required][FromQuery] string partitionKey, 
            [Required][FromQuery] string rowKey, [Required][FromQuery] int maxAmount, [FromQuery] DateTime? updateExpiresAt)
        {
            var (getTableResult, dbTable) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;
            
            if (string.IsNullOrEmpty(rowKey))
                return this.GetResult(OperationResult.RowKeyIsNull);

            var result =
                updateExpiresAt == null
                    ? dbTable.GetHighestRowAndBelow(partitionKey, rowKey, maxAmount)
                    : ServiceLocator.DbTableReadOperations.GetHighestRowAndBelow(dbTable, partitionKey, rowKey, maxAmount, updateExpiresAt.Value);
            
            return this.ToDbRowsResult(result);
        }
        
        
        [HttpGet("Rows/UpdateExpiresTime")]
        public async ValueTask<IActionResult> HighestRowAndBelow([Required][FromQuery] string tableName, 
            [Required][FromQuery] string partitionKey, 
            [FromQuery] DateTime updateExpiresAt)
        {
            var (getTableResult, dbTable) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;
            
            var body = await Request.BodyAsIMemoryAsync();

            var json = Encoding.UTF8.GetString(body.Span); 

            var bodyModel = Newtonsoft.Json.JsonConvert.DeserializeObject<UpdateExpiresModel>(json); 

            ServiceLocator.DbTableWriteOperations.UpdateExpirationTime(dbTable, partitionKey, bodyModel.RowKeys, updateExpiresAt);
            
            return this.ResponseOk();
        }
        
    }
    
}