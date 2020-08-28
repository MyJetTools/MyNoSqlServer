using System;
using System.Linq;
using Microsoft.AspNetCore.Mvc;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]
    public class ApiController: Controller
    {

        private static readonly Lazy<object> Version = new Lazy<object>(() => new
        {
            Name = ServiceLocator.AppName,
            Version = ServiceLocator.AppVersion,
            ServiceLocator.StartedAt,
            ServiceLocator.Host,
            Environment = ServiceLocator.AspNetEnvironment
        });

        private static IActionResult _isAliveResult;
        
        [HttpGet("api/isalive")]
        public IActionResult IsAlive()
        {
            _isAliveResult ??= Json(Version.Value);
            return _isAliveResult;
        }

        [HttpGet("/api/status")]
        public IActionResult Status()
        {
            var tableNames = ServiceLocator.ChangesSubscribers.GetAll();

            var result = tableNames.Select(itm =>
                new
                {
                    id = itm.Id,
                    name = itm.Name,
                    ip = itm.Ip,
                    tables = itm,
                    connectedTime = itm.Created.ToString("s"),
                    lastIncomingTime = itm.Created.ToString("s"),
                }).OrderBy(iym => iym.id);
            
            return Json(result);
        }
        
    }
}