using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Models;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]
    public class RowController : Controller
    {
        [HttpGet("Row")]
        public IActionResult List([Required][FromQuery] string tableName, [FromQuery] string partitionKey,
            [FromQuery] string rowKey, [FromQuery] int? limit, [FromQuery] int? skip)
        {
            
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;

            if (partitionKey != null)
            {
                if (rowKey == null)
                {
                    var entities = table.GetRecords(partitionKey, limit, skip);
                    return this.ToDbRowsResult(entities);
                }

                var entity = table.GetEntity(partitionKey, rowKey);

                return entity == null 
                    ? this.GetResult(OperationResult.RowNotFound) 
                    : this.ToDbRowResult(entity);
            }

            // PartitionKey == null and RowKey == null
            if (rowKey == null)
            {
                var entities = table.GetAllRecords(skip, limit);
                return this.ToDbRowsResult(entities);
            }

            return Conflict("Not Supported when PartitionKey==null and RowKey!=null");
        }

        [HttpPost("Row/Insert")]
        public async ValueTask<IActionResult> InsertEntity([Required][FromQuery] string tableName, 
            [FromQuery]string syncPeriod)
        {
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;
            
            var body = await Request.BodyAsIMemoryAsync();

            var result = await ServiceLocator.DbOperations.InsertAsync(table, body, syncPeriod.ParseSynchronizationPeriodContract(), DateTime.UtcNow);
            
            return this.GetResult(result);
            
        }
        
        [HttpPut("Row/Replace")]
        public async ValueTask<IActionResult> ReplaceAsync([Required][FromQuery] string tableName, 
            [FromQuery]string syncPeriod)
        {
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;
            
            var body = await Request.BodyAsIMemoryAsync();

            var result = await ServiceLocator.DbOperations.ReplaceAsync(table, body, 
                syncPeriod.ParseSynchronizationPeriodContract(), DateTime.UtcNow);

            return this.GetResult(result);

        }

        [HttpPut("Row/Merge")]
        public async ValueTask<IActionResult> MergeAsync([Required] [FromQuery] string tableName,
            [FromQuery] string syncPeriod)
        {
            var (getTableResult, table) = this.GetTable(tableName);

            if (getTableResult != null)
                return getTableResult;

            var body = await Request.BodyAsIMemoryAsync();

            var result = await ServiceLocator.DbOperations.MergeAsync(table, body,
                syncPeriod.ParseSynchronizationPeriodContract(), DateTime.UtcNow);

            return this.GetResult(result);

        }

        [HttpPost("Row/InsertOrReplace")]
        public async ValueTask<IActionResult> InsertOrReplaceEntity([Required][FromQuery] string tableName, 
            [FromQuery]string syncPeriod)
        {
            
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;
            
            var body = await Request.BodyAsIMemoryAsync();
            
            var result = await ServiceLocator.DbOperations.InsertOrReplaceAsync(table, 
                body, syncPeriod.ParseSynchronizationPeriodContract(), DateTime.UtcNow);
            
            return this.GetResult(result);
        }

        [HttpDelete("Row")]
        public async ValueTask<IActionResult> Delete([Required][FromQuery] string tableName, 
            [Required][FromQuery] string partitionKey, [Required][FromQuery] string rowKey, 
            [FromQuery]string syncPeriod)
        {
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;

            if (string.IsNullOrEmpty(partitionKey))
                return this.GetResult(OperationResult.PartitionKeyIsNull);

            if (string.IsNullOrEmpty(rowKey))
                return this.GetResult(OperationResult.RowKeyIsNull);

            var result = await ServiceLocator.DbOperations.DeleteAsync(table, partitionKey, rowKey,
                syncPeriod.ParseSynchronizationPeriodContract());

            return this.GetResult(result);
        }


        [HttpDelete("CleanAndKeepLastRecords")]
        public async ValueTask<IActionResult> CleanAndKeepLastRecords([Required] [FromQuery] string tableName,
            [Required] [FromQuery] string partitionKey, [Required] [FromQuery] int amount,
            [FromQuery] string syncPeriod)
        {
            
            var (getTableResult, table) = this.GetTable(tableName, partitionKey);
            
            if (getTableResult != null)
                return getTableResult;

            var result = await ServiceLocator.DbOperations.CleanAndKeepLastRecordsAsync(table, partitionKey, amount,
                syncPeriod.ParseSynchronizationPeriodContract());
            
            return this.GetResult(result);
            
        }

        [HttpGet("Count")]
        public IActionResult Count([Required][FromQuery] string tableName, [FromQuery] string partitionKey)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);

            if (string.IsNullOrEmpty(partitionKey))
                return this.GetResult(OperationResult.PartitionKeyIsNull);
            
            var table = ServiceLocator.DbInstance.TryGetTable(tableName);

            if (table == null)
                return this.GetResult(OperationResult.TableNotFound);

            var count = table.GetRecordsCount(partitionKey);

            return Content(count.ToString());
        }

    }
}