using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Formatters;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.SnapshotSaver;

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
            
            /*
            
            var res = await request.BodyReader.ReadAsync();
            request.BodyReader.AdvanceTo(res.Buffer.Start);
            var pos = res.Buffer.Start;

            var result = new ChunkedStream();

            while (res.Buffer.TryGet(ref pos, out var mem))
            {
                result.Write(mem.ToArray());
            }
            
            result.Position = 0;
            return result;
            */
            
        }
   
        public static IActionResult CheckOnShuttingDown(this Controller ctx)
        {
            if (ServiceLocator.ShuttingDown)
                return ctx.Conflict("Application is shutting down");
            
            return null;
        }
        
        
    

    }
}