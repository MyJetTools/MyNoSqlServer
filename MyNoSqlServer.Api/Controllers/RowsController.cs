using System;
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Api.Controllers
{
    public class RowsController : Controller
    {
        
        [HttpPost("Rows/SinglePartitionMultipleRows")]
        public IActionResult SinglePartitionMultipleRows([Required][FromQuery] string tableName, [Required][FromQuery] string partitionKey,
            [Required][FromBody] string[] rowKeys)
        {
            
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;

            if (rowKeys == null || rowKeys.Length == 0)
                return this.ToDbRowsResult(Array.Empty<DbRow>());

            var result = table.GetMultipleRows(partitionKey, rowKeys);
            
            return this.ToDbRowsResult(result);
        }
        
        
        [HttpGet("Rows/HighestRowAndBelow")]
        public IActionResult HighestRowAndBelow([Required][FromQuery] string tableName, [Required][FromQuery] string partitionKey, 
            [Required][FromQuery] string rowKey, [Required][FromQuery] int maxAmount)
        {
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;
            
            if (string.IsNullOrEmpty(rowKey))
                return this.GetResult(OperationResult.RowKeyIsNull);

            var result = table.GetHighestRowAndBelow(partitionKey, rowKey, maxAmount);
            
            return this.ToDbRowsResult(result);
        }
        
    }
    
}