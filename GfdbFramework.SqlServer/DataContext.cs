using System;

namespace GfdbFramework.SqlServer
{
    /// <summary>
    /// SqlServer 数据库的数据操作上下文类。
    /// </summary>
    public class DataContext : Realize.DataContext
    {
        private static readonly Type _NullableType = typeof(int?).GetGenericTypeDefinition();

        /// <summary>
        /// 使用指定的内部版本号以及数据库连接字符串初始化一个新的 <see cref="DataContext"/> 类实例。
        /// </summary>
        /// <param name="buildNumber">数据库内部版本号。</param>
        /// <param name="connectionString">连接字符串。</param>
        public DataContext(int buildNumber, string connectionString)
            : base(new DatabaseOperation(connectionString), new SqlFactory())
        {
            BuildNumber = buildNumber;

            if (buildNumber >= 895)
            {
                Version = "15.0.X.X";
                ReleaseName = "SQL Server 2019";
            }
            else if (buildNumber >= 868)
            {
                Version = "14.0.X.X";
                ReleaseName = "SQL Server 2017";
            }
            else if (buildNumber >= 852)
            {
                Version = "13.0.X.X";
                ReleaseName = "SQL Server 2016";
            }
            else if (buildNumber >= 782)
            {
                Version = "12.0.X.X";
                ReleaseName = "SQL Server 2014";
            }
            else if (buildNumber >= 684)
            {
                Version = "11.0.X.X";
                ReleaseName = "SQL Server 2012";
            }
            else if (buildNumber >= 661)
            {
                Version = "10.50.X.X";
                ReleaseName = "SQL Server 2008 R2";
            }
            else if (buildNumber >= 655)
            {
                Version = "10.00.X.X";
                ReleaseName = "SQL Server 2008";
            }
            else if (buildNumber >= 611)
            {
                Version = "9.00.X.X";
                ReleaseName = "SQL Server 2005 SP2";
            }
            else if (buildNumber >= 539)
            {
                Version = "8.00.X.X";
                ReleaseName = "SQL Server 2000";
            }
            else if (buildNumber >= 515)
            {
                Version = "7.00.X.X";
                ReleaseName = "SQL Server 7";
            }
        }

        /// <summary>
        /// 使用指定的发行版本号以及数据库连接字符串初始化一个新的 <see cref="DataContext"/> 类实例。
        /// </summary>
        /// <param name="releaseName">数据库发行版本号。</param>
        /// <param name="connectionString">连接字符串。</param>
        public DataContext(string releaseName, string connectionString)
            : base(new DatabaseOperation(connectionString), new SqlFactory())
        {
            ReleaseName = releaseName;

            releaseName = releaseName.Trim().ToLower();

            if (releaseName.StartsWith("sql server 7"))
            {
                Version = "7.00.X.X";
                BuildNumber = 515;
            }
            else if (releaseName.StartsWith("sql server 2000"))
            {
                Version = "8.00.X.X";
                BuildNumber = 539;
            }
            else if (releaseName.StartsWith("sql server 2005"))
            {
                Version = "9.00.X.X";
                BuildNumber = 612;
            }
            else if (releaseName.StartsWith("sql server 2008 r2"))
            {
                Version = "10.50.X.X";
                BuildNumber = 665;
            }
            else if (releaseName.StartsWith("sql server 2008"))
            {
                Version = "10.00.X.X";
                BuildNumber = 655;
            }
            else if (releaseName.StartsWith("sql server 2012"))
            {
                Version = "11.0.X.X";
                BuildNumber = 706;
            }
            else if (releaseName.StartsWith("sql server 2014"))
            {
                Version = "12.0.X.X";
                BuildNumber = 782;
            }
            else if (releaseName.StartsWith("sql server 2016"))
            {
                Version = "13.0.X.X";
                BuildNumber = 852;
            }
            else if (releaseName.StartsWith("sql server 2017"))
            {
                Version = "14.0.X.X";
                BuildNumber = 869;
            }
            else if (releaseName.StartsWith("sql server 2019"))
            {
                Version = "15.0.X.X";
                BuildNumber = 902;
            }
        }

        /// <summary>
        /// 使用指定的内部版本号、版本号、发行版本名称以及数据库连接字符串初始化一个新的 <see cref="DataContext"/> 类实例。
        /// </summary>
        /// <param name="buildNumber">数据库内部版本号。</param>
        /// <param name="version">数据库版本号。</param>
        /// <param name="releaseName">数据库发行版本号。</param>
        /// <param name="connectionString">连接字符串。</param>
        public DataContext(int buildNumber, string version, string releaseName, string connectionString)
            : base(new DatabaseOperation(connectionString), new SqlFactory())
        {
            BuildNumber = buildNumber;
            Version = version;
            ReleaseName = releaseName;
        }

        /// <summary>
        /// 获取当前所操作数据库的内部版本号。
        /// </summary>
        public override double BuildNumber { get; }

        /// <summary>
        /// 获取当前所操作数据库的版本号。
        /// </summary>
        public override string Version { get; }

        /// <summary>
        /// 获取当前所操作数据库的发行版本名称。
        /// </summary>
        public override string ReleaseName { get; }

        /// <summary>
        /// 将指定的 .NET 基础数据类型转换成映射到数据库后的默认数据类型（如：System.Int32 应当返回 int，System.String 可返回 varchar(255)）。
        /// </summary>
        /// <param name="type">待转换成数据库数据类型的框架类型。</param>
        /// <returns>该框架类型映射到数据库的默认数据类型。</returns>
        public override string NetTypeToDBType(Type type)
        {
            switch (type.FullName)
            {
                case "System.Int16":
                    return "smallint";
                case "System.Int32":
                    return "int";
                case "System.Int64":
                    return "bigint";
                case "System.DateTime":
                    return "datetime";
                case "System.Guid":
                    return "uniqueidentifier";
                case "System.Single":
                    return "real";
                case "System.Double":
                    return "float";
                case "System.DateTimeOffset":
                    return "datetimeoffset(7)";
                case "System.TimeSpan":
                    return "time(7)";
                case "System.Decimal":
                    return "decimal(23,5)";
                case "System.Boolean":
                    return "bit";
                case "System.Byte":
                    return "tinyint";
                case "System.String":
                    return "varchar(255)";
            }

            if (type.IsEnum)
                return "int";
            else if (type.IsGenericType && type.GetGenericTypeDefinition() == _NullableType)
                return NetTypeToDBType(type.GetGenericArguments()[0]);
            else
                throw new Exception(string.Format("未能将 .NET 框架中 {0} 类型转换成 Sql Server 对应的数据类型", type.FullName));
        }
    }
}
