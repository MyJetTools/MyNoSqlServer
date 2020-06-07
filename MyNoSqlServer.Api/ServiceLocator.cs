using System;
using System.Net;
using System.Reflection;
using MyDependencies;
using MyNoSqlServer.Api.Services;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.SnapshotSaver;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlServer.Api
{
    public static class ServiceLocator
    {
        public static string Version { get; private set; }
        
        public static DbInstance DbInstance { get; private set; }
        
        public static GlobalVariables GlobalVariables { get; private set; }

        public static ISnapshotStorage SnapshotStorage { get; private set; }
        
        public static IReplicaSynchronizationService DataSynchronizer { get; private set; }

        public static SnapshotSaverEngine SnapshotSaverEngine { get; private set; }
        
        public static readonly ISnapshotSaverScheduler SnapshotSaverScheduler = new SnapshotSaverScheduler();
        
        public static DbOperations DbOperations { get; private set; }
        
        public static readonly MyServerTcpSocket<IMyNoSqlTcpContract> TcpServer = 
            new MyServerTcpSocket<IMyNoSqlTcpContract>(new IPEndPoint(IPAddress.Any, 5125))
                .RegisterSerializer(()=> new MyNoSqlTcpSerializer())
                .SetService(()=>new ChangesTcpService())
                .AddLog(Console.WriteLine);

        public static void Init(IServiceResolver sr)
        {
            DbInstance = sr.GetService<DbInstance>();
            SnapshotStorage = sr.GetService<ISnapshotStorage>();
            
            DataSynchronizer = new ChangesPublisherToSocket();
            SnapshotSaverEngine = sr.GetService<SnapshotSaverEngine>();

            GlobalVariables = sr.GetService<GlobalVariables>();

            DbOperations = sr.GetService<DbOperations>();

            PopulateAssembly();
        }

        private static void PopulateAssembly()
        {
            var assembly = Assembly.GetExecutingAssembly();
            var version = assembly.GetCustomAttribute<AssemblyFileVersionAttribute>();
            if (version != null)
            {
                Version = version.ToString();
            }
            else
            {
                Version = "unknown";
            }
        }


        public static void Start()
        {
            SnapshotSaverEngine.Start();
            TcpServer.Start();
        }
    }
}