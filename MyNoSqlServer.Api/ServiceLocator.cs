using System;
using System.Linq;
using System.Net;
using System.Reflection;
using DotNetCoreDecorators;
using MyDependencies;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Operations;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.GarbageCollection;
using MyNoSqlServer.Domains.SnapshotSaver;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlServer.Api
{
    public static class ServiceLocator
    {
        static ServiceLocator()
        {
            StartedAt = DateTime.UtcNow;

            var name = Assembly.GetEntryAssembly()?.GetName();

            var appName = name?.Name ?? string.Empty;

            var nameSegments = appName.Split('.', StringSplitOptions.RemoveEmptyEntries);

            if (nameSegments.Length > 2)
            {
                appName = string.Join('.', nameSegments.Skip(1));
            }

            AppName = appName;
            AppVersion = name?.Version?.ToString();

            AspNetEnvironment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            Host = Environment.GetEnvironmentVariable("HOSTNAME");

            Console.WriteLine($"AppName: {AppName}");
            Console.WriteLine($"AppVersion: {AppVersion}");
            Console.WriteLine($"AspNetEnvironment: {AspNetEnvironment}");
            Console.WriteLine($"Host: {Host}");
            Console.WriteLine($"StartedAt: {StartedAt}");
            Console.WriteLine("Port http: 5123");
            Console.WriteLine("Port tcp: 5125");
            Console.WriteLine();
        }

        public static string AppName { get; }
        public static string AppVersion { get; }
        public static DateTime StartedAt { get;}
        public static string AspNetEnvironment { get; }
        public static string Host { get; }
        public static DbInstance DbInstance { get; private set; }
        
        public static DbTableOperations DbTableOperations { get; private set; }
        public static GlobalVariables GlobalVariables { get; private set; }
        public static SnapshotSaverEngine SnapshotSaverEngine { get; private set; }
        public static ExpiredEntitiesGarbageCollector ExpiredEntitiesGarbageCollector { get; set; }

        private static readonly TaskTimer ExpiredEntitiesGcTimer = new TaskTimer(TimeSpan.FromSeconds(30));
        
        private static readonly TaskTimer PersistenceTime = new TaskTimer(TimeSpan.FromSeconds(5));
        public static DbTableWriteOperations DbTableWriteOperations { get; private set; }
        public static DbTableReadOperationsWithExpiration DbTableReadOperations { get; private set; }
        
        public static readonly MyServerTcpSocket<IMyNoSqlTcpContract> TcpServer = 
            new MyServerTcpSocket<IMyNoSqlTcpContract>(new IPEndPoint(IPAddress.Any, 5125))
                .RegisterSerializer(()=> new MyNoSqlTcpSerializer())
                .SetService(()=>new ChangesTcpService())
                .AddLog((c, d)=>
                {
                    if (c == null)
                        Console.WriteLine($"DateTime: {DateTime.UtcNow}. "+d);
                    else
                        Console.WriteLine($"DateTime: {DateTime.UtcNow}. ConnectionId:{c.Id}. "+d);
                });

        public static void Init(IServiceResolver sr)
        {
            DbInstance = sr.GetService<DbInstance>();
            
            SnapshotSaverEngine = sr.GetService<SnapshotSaverEngine>();

            GlobalVariables = sr.GetService<GlobalVariables>();

            DbTableWriteOperations = sr.GetService<DbTableWriteOperations>();

            DbTableReadOperations = sr.GetService<DbTableReadOperationsWithExpiration>();

            DbTableOperations = sr.GetService<DbTableOperations>();

            ExpiredEntitiesGarbageCollector = sr.GetService<ExpiredEntitiesGarbageCollector>();
        }

        public static void Start()
        {
            SnapshotSaverEngine.LoadSnapshotsAsync().Wait();
            
            ExpiredEntitiesGcTimer.Register("Expired Entities GC", 
                ()=>ExpiredEntitiesGarbageCollector.DetectAndExpireAsync(DateTime.UtcNow));

            PersistenceTime.Register("Persistence Timer", 
                () => SnapshotSaverEngine.SynchronizeAsync(null));
            
            TcpServer.Start();
            ExpiredEntitiesGcTimer.Start();
            PersistenceTime.Start();
        }
    }
}