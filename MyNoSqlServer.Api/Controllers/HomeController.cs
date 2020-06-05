using System;
using System.Net.Http.Headers;
using Microsoft.AspNetCore.Mvc;

namespace MyNoSqlServer.Api.Controllers
{
    
    public class HomeController : Controller
    {

        [HttpGet("/")]
        public IActionResult Index()
        {
            return Content("<html><head><link href=\"/css/bootstrap.css\" rel=\"stylesheet\" type=\"text/css\" /><script src=\"/js/jquery-3.4.1.min.js\"></script><script src=\"/js/main.js?ver="+Guid.NewGuid()+"\"></script></head><body></body></html>", "text/html; charset=UTF-8");
        }
        
    }
}