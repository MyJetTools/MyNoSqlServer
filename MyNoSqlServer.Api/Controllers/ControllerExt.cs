using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Api.Controllers
{
    public static class ControllerExt
    {
        private const string AppJsonContentType = "application/json";
        public static IActionResult ToDbRowResult(this Controller ctx, DbRow dbRow)
        {
            return ctx.File(dbRow.Data, AppJsonContentType);
        }
        
        public static IActionResult ToDbRowsResult(this Controller ctx, IEnumerable<DbRow> dbRows)
        {
            var response = dbRows.ToJsonArray().AsArray();
            return ctx.File(response, AppJsonContentType);
        }
        
        public static async ValueTask<IMyMemory> BodyAsIMemoryAsync(
            this HttpRequest request)
        {
            var mem = new MemoryStream();
            await request.Body.CopyToAsync(mem);
            return new MyMemoryAsByteArray(mem.ToArray());
        }
   
        public static IActionResult CheckOnShuttingDown(this Controller ctx)
        {
            if (ServiceLocator.GlobalVariables.IsShuttingDown)
                return ctx.ApplicationIsShuttingDown();
            
            return null;
        }

        public static (IActionResult result, DbTable dbTable) GetTable(this Controller ctx, string tableName)
        {
            var shutDown = ctx.CheckOnShuttingDown();
            if (shutDown != null)
                return (shutDown, null); 
            
            if (string.IsNullOrEmpty(tableName))
                return (ctx.TableNameIsNull(), null);

            tableName = tableName.ToLowerInvariant();
            var table = ServiceLocator.DbInstance.CreateTableIfNotExists(tableName);
            return (null, table);
        }



        public static IActionResult GetResult(this Controller ctx, OperationResult result)
        {
            switch (result)
            {
                case OperationResult.Ok:
                    return ctx.Ok();

                case OperationResult.TableNotFound:
                    return ctx.TableNotFound();
                
                case OperationResult.RecordExists:
                    return ctx.ResponseConflict("Record with the same PartitionKey and RowKey is already exists");

                case OperationResult.ShuttingDown:
                    return ctx.ApplicationIsShuttingDown();
                
                case OperationResult.RecordNotFound:
                    return ctx.RecordIsNotFound();
                
                case OperationResult.TableNameIsEmpty:
                    return ctx.TableNameIsNull();
                
                case OperationResult.RecordChangedConcurrently:
                    return ctx.Conflict("Record changed");

                case OperationResult.PartitionKeyIsNull:
                    return ctx.PartitionKeyIsNull();

                case OperationResult.RowKeyIsNull:
                    return ctx.RowKeyIsNull();
                
                case OperationResult.RowNotFound:
                    return ctx.RowNotFound();
                
            }
            
            throw new Exception("Technical Error. Unknown Result: "+result);
        }
    

    }
}