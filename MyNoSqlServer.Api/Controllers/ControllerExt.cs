using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Api.Controllers
{
    public static class ControllerExt
    {
        private const string AppJsonContentType = "application/json";
        public static IActionResult ToDbRowResult(this DbRow dbRow, Controller ctx)
        {
            return dbRow == null 
                ? ctx.GetResult(OperationResult.RowNotFound) 
                : ctx.File(dbRow.Data, AppJsonContentType);
        }
        
        public static IActionResult ToDbRowsResult(this Controller ctx, IEnumerable<DbRow> dbRows)
        {
            var response = dbRows.ToJsonArray().AsArray();
            return ctx.File(response, AppJsonContentType);
        }
        
        
        public static IActionResult ToDbRowsResult(this IEnumerable<DbRow> dbRows, Controller ctx)
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
                return ctx.GetResult(OperationResult.ShuttingDown);
            
            return null;
        }

        public static (IActionResult result, DbTable dbTable) GetTable(this Controller ctx, string tableName, 
            string partitionKey)
        {
            var shutDown = ctx.CheckOnShuttingDown();
            if (shutDown != null)
                return (shutDown, null); 
            
            if (string.IsNullOrEmpty(tableName))
                return (ctx.GetResult(OperationResult.TableNameIsEmpty), null);
            
            if (string.IsNullOrEmpty(partitionKey))
                return (ctx.GetResult(OperationResult.PartitionKeyIsNull), null);

            tableName = tableName.ToLowerInvariant();
            var table = ServiceLocator.DbInstance.CreateTableIfNotExists(tableName);
            return (null, table);
        }


        public static UpdateExpirationTime GetUpdateExpirationTime(this Controller ctx, string updateExpiresAt)
        {

            if (string.IsNullOrEmpty(updateExpiresAt))
            {
                return new UpdateExpirationTime
                {
                    ExpiresDate = null,
                    ClearExpiresDate = false
                };
            }

            if (updateExpiresAt == "del" || updateExpiresAt == "delete")
            {
                return new UpdateExpirationTime
                {
                    ExpiresDate = null,
                    ClearExpiresDate = true
                };
            }

            if (updateExpiresAt.Contains("T"))
            {
                if (DateTime.TryParse(updateExpiresAt, out var value))
                {
                    return new UpdateExpirationTime
                    {
                        ExpiresDate = value,
                        ClearExpiresDate = true
                    };
                }
            }


            
            return new UpdateExpirationTime
            {
                ExpiresDate = null,
                ClearExpiresDate = false
            };
        }
        
        public static (IActionResult result, DbTable dbTable) GetTable(this Controller ctx, string tableName)
        {
            var shutDown = ctx.CheckOnShuttingDown();
            if (shutDown != null)
                return (shutDown, null); 
            
            if (string.IsNullOrEmpty(tableName))
                return (ctx.GetResult(OperationResult.TableNameIsEmpty), null);

            tableName = tableName.ToLowerInvariant();
            var table = ServiceLocator.DbInstance.CreateTableIfNotExists(tableName);
            return (null, table);
        }


        public static IActionResult GetResult(this Controller ctx, OperationResult result)
        {
            return result switch
            {
                OperationResult.Ok => ctx.ResponseOk(),
                OperationResult.TableNotFound => ctx.NotFound("Table not found"),
                OperationResult.PartitionKeyIsNull => ctx.ValidationProblem("Partition key is null"),
                OperationResult.RowKeyIsNull => ctx.ValidationProblem("Row key is null"),
                OperationResult.QueryIsNull => ctx.ValidationProblem("Query is null"),
                OperationResult.RowNotFound => ctx.NotFound("Row not found"),
                OperationResult.TableNameIsEmpty => ctx.ValidationProblem("Table name is empty"),
                _ => ctx.ResponseConflict(result)
            };
        }
    

    }
}