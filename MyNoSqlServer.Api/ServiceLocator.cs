using System.Reflection;

namespace MyNoSqlServer.Api
{
    public static class ApiServiceLocator
    {
        public static string Version { get; private set; }

        public static void Init()
        {
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
    }
}