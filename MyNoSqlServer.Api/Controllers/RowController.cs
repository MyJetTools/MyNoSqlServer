using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]
    public class RowController : Controller
    {
        [HttpGet("Row")]
        public IActionResult List([Required][FromQuery] string tableName, [FromQuery] string partitionKey,
            [FromQuery] string rowKey, [FromQuery] int? limit, [FromQuery] int? skip)
        {
            
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return this.TableNotFound(tableName);

            if (partitionKey != null)
            {
                if (rowKey == null)
                {
                    var entities = table.GetRecords(partitionKey, limit, skip);
                    return this.ToDbRowsResult(entities);
                }

                var entity = table.GetEntity(partitionKey, rowKey);

                return entity == null 
                    ? this.RowNotFound(tableName, partitionKey, rowKey) 
                    : this.ToDbRowResult(entity);
            }

            // PartitionKey == null and RowKey == null
            if (rowKey == null)
            {
                var entities = table.GetAllRecords(limit);
                return this.ToDbRowsResult(entities);
            }

            return Conflict("Not Supported when PartitionKey==null and RowKey!=null");
        }

        [HttpPost("Row/Insert")]
        public async ValueTask<IActionResult> InsertEntity([Required][FromQuery] string tableName, 
            [FromQuery]string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown; 
            
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            var table = DbInstance.CreateTableIfNotExists(tableName);
            
            
            var body = await Request.BodyAsIMemoryAsync();

            var fields = body.ParseFirstLevelOfJson();

            var entityInfo = fields.GetEntityInfo(); 

            if (string.IsNullOrEmpty(entityInfo.PartitionKey))
                return this.PartitionKeyIsNull();

            if (string.IsNullOrEmpty(entityInfo.RowKey))
                return this.RowKeyIsNull();

            if (table.HasRecord(entityInfo))
                this.ResponseConflict("Record with the same PartitionKey and RowKey is already exists");
            
            var (dbPartition, dbRow) = table.Insert(entityInfo, fields);

            if (dbPartition == null) 
                return this.ResponseConflict("Can not insert entity");
            
            ServiceLocator.DataSynchronizer.SynchronizeUpdate(table, new[] {dbRow});
            
            return await this.ResponseOk()
                .SynchronizePartitionAsync(table, dbPartition, syncPeriod.ParseSynchronizationPeriod());

        }
        
        [HttpPost("Row/InsertOrReplace")]
        public async ValueTask<IActionResult> InsertOrReplaceEntity([Required][FromQuery] string tableName, 
            [FromQuery]string syncPeriod)
        {
            
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;
            
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            var table = DbInstance.CreateTableIfNotExists(tableName);
            
            var body = await Request.BodyAsIMemoryAsync();

            var fields = body.ParseFirstLevelOfJson();

            var entityInfo = fields.GetEntityInfo(); 

            if (string.IsNullOrEmpty(entityInfo.PartitionKey))
                return this.PartitionKeyIsNull();

            if (string.IsNullOrEmpty(entityInfo.RowKey))
                return this.RowKeyIsNull();
            
            var (dbPartition, dbRow) = table.InsertOrReplace(entityInfo, fields);
            
            ServiceLocator.DataSynchronizer
                .SynchronizeUpdate(table, new[]{dbRow});
            

            return await this.ResponseOk()
                .SynchronizePartitionAsync(table, dbPartition, syncPeriod.ParseSynchronizationPeriod());
        }

        [HttpDelete("Row")]
        public ValueTask<IActionResult> Delete([Required][FromQuery] string tableName, [Required][FromQuery] string partitionKey,
            [Required][FromQuery] string rowKey, 
            [FromQuery]string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return new ValueTask<IActionResult>(shutDown);
            
            if (string.IsNullOrEmpty(tableName))
                return new ValueTask<IActionResult>(this.TableNameIsNull());

            if (string.IsNullOrEmpty(partitionKey))
                return new ValueTask<IActionResult>(this.PartitionKeyIsNull());

            if (string.IsNullOrEmpty(rowKey))
                return new ValueTask<IActionResult>(this.RowKeyIsNull());

            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return new ValueTask<IActionResult>(this.TableNotFound(tableName));

            var (dbPartition, dbRow) = table.DeleteRow(partitionKey, rowKey);

            if (dbPartition == null) 
                return new ValueTask<IActionResult>(this.RowNotFound(tableName, partitionKey, rowKey));
         
            ServiceLocator.DataSynchronizer.SynchronizeDelete(table, new[]{dbRow});
            
            return this.ResponseOk().SynchronizeDeletePartitionAsync(table, dbPartition, syncPeriod.ParseSynchronizationPeriod());
        }


        [HttpDelete("CleanAndKeepLastRecords")]
        public ValueTask<IActionResult> CleanAndKeepLastRecords([Required] [FromQuery] string tableName,
            [Required] [FromQuery] string partitionKey, [Required] [FromQuery] int amount,
            [FromQuery] string syncPeriod)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return new ValueTask<IActionResult>(shutDown);

            if (string.IsNullOrEmpty(tableName))
                return new ValueTask<IActionResult>(this.TableNameIsNull());

            if (string.IsNullOrEmpty(partitionKey))
                return new ValueTask<IActionResult>(this.PartitionKeyIsNull());

            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return new ValueTask<IActionResult>(this.TableNotFound(tableName));


            var (dbPartition, dbRows) = table.CleanAndKeepLastRecords(partitionKey, amount);

            if (dbPartition != null)
            {
                ServiceLocator.DataSynchronizer.SynchronizeDelete(table, dbRows);

                return this.ResponseOk()
                    .SynchronizePartitionAsync(table, dbPartition, syncPeriod.ParseSynchronizationPeriod());
            }

            return new ValueTask<IActionResult>(this.ResponseOk());
        }

        [HttpGet("Count")]
        public IActionResult Count([Required][FromQuery] string tableName, [FromQuery] string partitionKey)
        {
            if (string.IsNullOrEmpty(tableName))
                return this.TableNameIsNull();

            if (string.IsNullOrEmpty(partitionKey))
                return this.PartitionKeyIsNull();
            
            var table = DbInstance.GetTable(tableName);

            if (table == null)
                return this.TableNotFound(tableName);

            var count = table.GetRecordsCount(partitionKey);

            return Content(count.ToString());
        }

    }
}