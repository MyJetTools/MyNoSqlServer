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
                masterNode = ServiceLocator.NodeClient?.RemoteLocation,
                location = new
                {
                   id = Startup.Settings.Location,
                   compress = Startup.Settings.CompressData
                },
                nodes = nodeSessions.Select(itm => new
                {
                    location = itm.RemoteLocation,
                    lastAccessed =  (dt - itm.LastAccessed).ToString("c"),
                    connected = (dt - itm.Created).ToString("c"),
                    latency = itm.Latency.ToString("c"),
                    compress = itm.Compression
                    
                }),
                readers = connections.Select(itm => ReaderApiModel.Create(itm as DataReaderTcpService))
                    .OrderBy(iym => iym.Id)
            };
            return Json(result);
        }
        
    }
}