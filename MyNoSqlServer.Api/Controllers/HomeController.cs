using System;
using Microsoft.AspNetCore.Mvc;

namespace MyNoSqlServer.Api.Controllers
{
    
    public class HomeController : Controller
    {

        [HttpGet("/")]
        public IActionResult Index()
        {
            var version = typeof(HomeController).Assembly.GetName().Version?.ToString() ?? "";
            return Content("<html><head><title>"+version+" MyNoSQLServer</title><link href=\"/css/bootstrap.css\" rel=\"stylesheet\" type=\"text/css\" /><script src=\"/js/jquery-3.4.1.min.js\"></script><script src=\"/js/main.min.js?ver="+Guid.NewGuid()+"\"></script></head><body></body></html>", "text/html; charset=UTF-8");
        }
        
    }
}