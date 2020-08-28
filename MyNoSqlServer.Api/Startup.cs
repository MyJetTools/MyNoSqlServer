using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MyDependencies;
using MyNoSqlServer.Api.Grpc;
using MyNoSqlServer.AzureStorage;
using MyNoSqlServer.Domains;
using ProtoBuf.Grpc.Server;


namespace MyNoSqlServer.Api
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }
        
        public static MyIoc IoC = new MyIoc();

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {

            services.AddApplicationInsightsTelemetry(Configuration);

            services.AddControllers();

            services.AddSignalR();

            services.AddCodeFirstGrpc();

            services.AddSwaggerDocument(o => { o.Title = "MyNoSqlServer"; });

            var settings = SettingsLoader.LoadSettings();
            
            IoC.BindDomainsServices();
            IoC.BindAzureStorage(settings.BackupAzureConnectString);
            
            ServiceLocator.Init(IoC);

        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostApplicationLifetime applicationLifetime)
        {

            // Enable middleware to serve generated Swagger as a JSON endpoint.
//                  app.UseSwagger();

            // Enable middleware to serve swagger-ui (HTML, JS, CSS, etc.), 
            // specifying the Swagger JSON endpoint.
               

            //app.UseHttpsRedirection();


            applicationLifetime.ApplicationStopping.Register(OnShutdown);

            app.Use((context, next) =>
            {
                if (context.Request.Headers.ContainsKey("X-Forwarded-Proto"))
                    context.Request.Scheme = context.Request.Headers["X-Forwarded-Proto"];
                return next();
            });

            //  app.UseForwardedHeaders();

            
            app.UseStaticFiles();

            app.UseOpenApi();
            app.UseSwaggerUi3();

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
                endpoints.MapGrpcService<MyNoSqlServerReaderGrpcConnection>();
            });
            
            ServiceLocator.Start();
        }

        private void OnShutdown()
        {
            ServiceLocator.GlobalVariables.IsShuttingDown = true;
            Task.Delay(500).Wait();

            ServiceLocator.SnapshotSaverEngine.Stop();
        }
    }
}