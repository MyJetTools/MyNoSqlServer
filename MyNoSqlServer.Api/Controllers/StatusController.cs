using System;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Api.DataReadersTcpServer;
using MyNoSqlServer.Api.Models;

namespace MyNoSqlServer.Api.Controllers
{
    [ApiController]
    public class StatusController: Controller
    {
    
    
        [HttpGet("/api/status")]
        public IActionResult Status()
        {
            var connections = ServiceLocator.TcpServer.GetConnections();

            var nodeSessions = ServiceLocator.NodeSessionsList.GetAll();

            var dt = DateTime.UtcNow;
            
            var result = new
            {
                nodes = nodeSessions.Select(itm => new
                {
                    location = itm.Location,
                    lastAccessed =  (dt - itm.LastAccessed).ToString("c")
                    
                }),
                readers = connections.Select(itm => ReaderApiModel.Create(itm as DataReaderTcpService))
                    .OrderBy(iym => iym.Id)
            };
            return Json(result);
        }
        
    }
}