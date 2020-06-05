using System;
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Api.Controllers
{
    public class RowsController : Controller
    {
        
        [HttpPost("Rows/SinglePartitionMultipleRows")]
        public IActionResult SinglePartitionMultipleRows([Required][FromQuery] string tableName, [Required][FromQuery] string partitionKey,
            [Required][FromBody] string[] rowKeys)
        {
            
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            if (string.IsNullOrEmpty(partitionKey))
                return this.PartitionKeyIsNull();

            if (rowKeys == null || rowKeys.Length == 0)
                return this.ToDbRowsResult(Array.Empty<DbRow>());

            var table = DbInstance.GetTable(tableName);

            var result = table.GetMultipleRows(partitionKey, rowKeys);
            
            return this.ToDbRowsResult(result);
        }
        
        
        [HttpGet("Rows/HighestRowAndBelow")]
        public IActionResult HighestRowAndBelow([Required][FromQuery] string tableName, [Required][FromQuery] string partitionKey, 
            [Required][FromQuery] string rowKey, [Required][FromQuery] int maxAmount)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            if (string.IsNullOrEmpty(partitionKey))
                return this.PartitionKeyIsNull();
            
            if (string.IsNullOrEmpty(rowKey))
                return this.RowKeyIsNull();

            var table = DbInstance.GetTable(tableName);

            var result = table.GetHighestRowAndBelow(partitionKey, rowKey, maxAmount);
            
            return this.ToDbRowsResult(result);
        }
        
    }
    
}