using System;

namespace GfdbFramework.SqlServer.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            DataContext dataContext = new DataContext();

            if (!dataContext.ExistsDatabase("TestDB2"))
            {
                //需要对程序运行目录下的 Databases 目录有权限，否则创建报拒绝访问错误，你也可以手动指定创建数据库文件到其他目录
                dataContext.CreateDatabase(new Core.DatabaseInfo()
                {
                    Name = "TestDB2"
                });
            }

            if (!dataContext.ExistsTable(dataContext.Commodities))
            {
                dataContext.CreateTable(dataContext.Commodities);
                dataContext.CreateTable(dataContext.Users);
                dataContext.CreateTable(dataContext.Units);
                dataContext.CreateTable(dataContext.Classifies);
                dataContext.CreateTable(dataContext.Brands);
            }
        }
    }
}
