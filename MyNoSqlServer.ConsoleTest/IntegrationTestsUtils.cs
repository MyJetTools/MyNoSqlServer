using System;

namespace MyNoSqlServer.ConsoleTest
{
    public static class Assert
    {

        public static void AreEqual<T>(T src, T dest)
        {
            if (!src.Equals(dest))
                throw new Exception("Objects are not Equal");
        }
        
    }
}