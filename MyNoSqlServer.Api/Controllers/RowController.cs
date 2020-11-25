using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.Domains.Db.Operations;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]
    public class RowController : Controller
    {
        [HttpGet("Row")]
        public IActionResult List([Required][FromQuery] string tableName, [FromQuery] string partitionKey,
            [FromQuery] string rowKey, [FromQuery] int? limit, [FromQuery] int? skip, 
            [FromQuery] DateTime? updateExpiresAt)
        {
            
            var (getTableResult, dbTable) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;


            if (updateExpiresAt == null)
            {
                if (partitionKey != null)
                    return rowKey == null
                        ? dbTable.GetRows(partitionKey, limit, skip).ToDbRowsResult(this)
                        : dbTable.TryGetRow(partitionKey, rowKey).ToDbRowResult(this);
            
            
                if (rowKey == null)
                    return dbTable.GetRows(limit, skip).ToDbRowsResult(this);

                return dbTable
                    .GetRowsByRowKey(rowKey, limit, skip)
                    .ToDbRowsResult(this);    
            }
            
            if (partitionKey != null)
                return rowKey == null
                    ?  ServiceLocator.DbTableReadOperations.GetRows(dbTable, partitionKey, limit, skip, updateExpiresAt.Value)
                        .ToDbRowsResult(this)
                    : ServiceLocator.DbTableReadOperations.TryGetRow(dbTable, partitionKey, rowKey, updateExpiresAt.Value)
                        .ToDbRowResult(this);
            
            
            if (rowKey == null)
                return ServiceLocator.DbTableReadOperations.GetRows(dbTable, limit, skip, updateExpiresAt.Value)
                    .ToDbRowsResult(this);

            return ServiceLocator.DbTableReadOperations.GetRows(dbTable, rowKey, limit, skip, updateExpiresAt.Value)
                .ToDbRowsResult(this);    

        }

        [HttpPost("Row/Insert")]
        public async ValueTask<IActionResult> InsertEntity([Required][FromQuery] string tableName, 
            [FromQuery]string syncPeriod)
        {
            var (getTableResult, table) = this.GetTable(tableName);
            
            if (getTableResult != null)
                return getTableResult;
            
            var body = await Request.BodyAsIMemoryAsync();

            var result = await ServiceLocator.DbTableWriteOperations.InsertAsync(table, body.ParseDynamicEntity(), syncPeriod.ParseSynchronizationPeriodContract(), DateTime.UtcNow);
            
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

            var result = await ServiceLocator.DbTableWriteOperations.ReplaceAsync(table, body.ParseDynamicEntity(), 
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

            var result = await ServiceLocator.DbTableWriteOperations.MergeAsync(table, body.ParseDynamicEntity(),
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
            
            var result = await ServiceLocator.DbTableWriteOperations.InsertOrReplaceAsync(table, 
                body.ParseDynamicEntity(), syncPeriod.ParseSynchronizationPeriodContract(), DateTime.UtcNow);
            
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

            var result = await ServiceLocator.DbTableWriteOperations.DeleteAsync(table, partitionKey, rowKey,
                syncPeriod.ParseSynchronizationPeriodContract());

            return this.GetResult(result);
        }


        [HttpDelete("CleanAndKeepLastRecords")]
        public ValueTask<IActionResult> CleanAndKeepLastRecords([Required] [FromQuery] string tableName,
            [Required] [FromQuery] string partitionKey, [Required] [FromQuery] int amount,
            [FromQuery] string syncPeriod)
        {
            var (getTableResult, table) = this.GetTable(tableName, partitionKey);
            
            if (getTableResult != null)
                return new ValueTask<IActionResult>(getTableResult);

            return ServiceLocator.DbTableWriteOperations.CleanAndKeepLastRecordsAsync(table, partitionKey, amount,
                syncPeriod.ParseSynchronizationPeriodContract())
                .GetResponseOkAsync(this);
        }

        [HttpGet("Count")]
        public IActionResult Count([Required][FromQuery] string tableName, [FromQuery] string partitionKey)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.GetResult(OperationResult.TableNameIsEmpty);
            
            

            
            var table = ServiceLocator.DbInstance.TryGetTable(tableName);

            if (table == null)
                return this.GetResult(OperationResult.TableNotFound);


            if (partitionKey == null)
                return Content(table.GetRecordsCount().ToString());

            var count = table.GetRecordsCount(partitionKey);

            return Content(count.ToString());
        }

    }
}