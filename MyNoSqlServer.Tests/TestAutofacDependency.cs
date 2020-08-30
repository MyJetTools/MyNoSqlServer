using Autofac.Core;
using MyAutofacExtensions;
using MyNoSqlServer.Api.Models;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class TestAutofacDependency
    {
        [Test]
        public void TestResolveAll()
        {
            Assert.DoesNotThrow(() =>
            {
                Assert.DoesNotThrow(() =>
                {
                    ScopeExtensions.TestAutofacDependency(new IModule[]
                    {
                        new ServiceModule("---") 
                    });
                });
            });
        }
    }
}