using GfdbFramework.Core;
using GfdbFramework.DataSource;
using GfdbFramework.Interface;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;

namespace GfdbFramework.SqlServer
{
    /// <summary>
    /// Sql Server 数据库的数据操作上下文类。
    /// </summary>
    public class DataContext : Core.DataContext
    {
        private static readonly Type _NullableType = typeof(int?).GetGenericTypeDefinition();
        private readonly string _ConnectionString = null;

        /// <summary>
        /// 使用指定的内部版本号以及数据库连接字符串初始化一个新的 <see cref="DataContext"/> 类实例。
        /// </summary>
        /// <param name="buildNumber">数据库内部版本号。</param>
        /// <param name="connectionString">连接字符串。</param>
        public DataContext(int buildNumber, string connectionString)
            : base(new DatabaseOperation(connectionString), new SqlFactory())
        {
            _ConnectionString = connectionString;

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
            _ConnectionString = connectionString;

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
            _ConnectionString = connectionString;
            BuildNumber = buildNumber;
            Version = version;
            ReleaseName = releaseName;
        }

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
                throw new Exception($"未能将 .NET 框架中 {type.FullName} 类型转换成 Sql Server 对应的数据类型");
        }

        /// <summary>
        /// 校验指定的数据库是否存在。
        /// </summary>
        /// <param name="databaseInfo">需要校验是否存在的数据库信息。</param>
        /// <returns>若存在则返回 true，否则返回 false。</returns>
        public override bool ExistsDatabase(DatabaseInfo databaseInfo)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            return (int)GetMasterOperation().ExecuteScalar(((SqlFactory)SqlFactory).GenerateExistsDatabaseSql(parameterContext, databaseInfo), CommandType.Text, parameterContext.ToList()) == 1;
        }

        /// <summary>
        /// 删除指定的数据库。
        /// </summary>
        /// <param name="databaseInfo">需要删除的数据库信息。</param>
        /// <returns>删除成功返回 true，否则返回 false。</returns>
        public override bool DeleteDatabase(DatabaseInfo databaseInfo)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            GetMasterOperation().ExecuteNonQuery(((SqlFactory)SqlFactory).GenerateDeleteDatabaseSql(parameterContext, databaseInfo), CommandType.Text, parameterContext.ToList());

            return true;
        }

        /// <summary>
        /// 创建指定的数据库。
        /// </summary>
        /// <param name="databaseInfo">需要创建的数据库信息。</param>
        /// <returns>创建成功返回 true，否则返回 false。</returns>
        public override bool CreateDatabase(DatabaseInfo databaseInfo)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            GetMasterOperation().ExecuteNonQuery(((SqlFactory)SqlFactory).GenerateCreateDatabaseSql(parameterContext, databaseInfo), CommandType.Text, parameterContext.ToList());

            return true;
        }

        /// <summary>
        /// 校验指定的数据库表是否存在。
        /// </summary>
        /// <param name="tableSource">所需校验的表数据源。</param>
        /// <returns>若存在则返回 true，否则返回 false。</returns>
        protected override bool ExistsTable(TableDataSource tableSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            object result = ((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateExistsTableSql(parameterContext, tableSource.Name), CommandType.Text, parameterContext.ToList());

            return result != null && result != DBNull.Value;
        }

        /// <summary>
        /// 校验指定的数据库视图是否存在。
        /// </summary>
        /// <param name="viewSource">所需校验的视图数据源。</param>
        /// <returns>若存在则返回 true，否则返回 false。</returns>
        protected override bool ExistsView(ViewDataSource viewSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            return (int)((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateExistsViewSql(parameterContext, viewSource.Name), CommandType.Text, parameterContext.ToList()) == 1;
        }

        /// <summary>
        /// 创建指定的数据库表。
        /// </summary>
        /// <param name="tableSource">需创建表的数据源对象。</param>
        /// <returns>创建成功返回 true，否则返回 false。</returns>
        protected override bool CreateTable(TableDataSource tableSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            ((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateCreateTableSql(parameterContext, tableSource), CommandType.Text, parameterContext.ToList());

            return true;
        }

        /// <summary>
        /// 创建指定的数据库视图。
        /// </summary>
        /// <param name="viewSource">需创建视图的数据源。</param>
        /// <returns>创建成功返回 true，否则返回 false。</returns>
        protected override bool CreateView(ViewDataSource viewSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            ((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateCreateViewSql(parameterContext, viewSource), CommandType.Text, parameterContext.ToList());

            return true;
        }

        /// <summary>
        /// 删除指定的数据库表。
        /// </summary>
        /// <param name="tableSource">需删除表的数据源对象。</param>
        /// <returns>删除成功返回 true，否则返回 false。</returns>
        protected override bool DeleteTable(TableDataSource tableSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            ((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateDeleteTableSql(parameterContext, tableSource), CommandType.Text, parameterContext.ToList());

            return true;
        }

        /// <summary>
        /// 删除指定的数据库视图。
        /// </summary>
        /// <param name="viewSource">需删除视图的数据源对象。</param>
        /// <returns>删除成功返回 true，否则返回 false。</returns>
        protected override bool DeleteView(ViewDataSource viewSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            ((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateDeleteViewSql(parameterContext, viewSource), CommandType.Text, parameterContext.ToList());

            return true;
        }

        /// <summary>
        /// 获取所操作数据库中所有的视图名称集合。
        /// </summary>
        /// <returns>当前上下文所操作数据库中所有存在的视图名称集合。</returns>
        public override ReadOnlyList<string> GetAllViews()
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            List<string> result = new List<string>();

            ((IDataContext)this).DatabaseOperation.ExecuteReader(((SqlFactory)SqlFactory).GenerateSelectAllViewNameSql(parameterContext), CommandType.Text, parameterContext.ToList(), dr =>
            {
                result.Add((string)dr.GetValue(0));

                return true;
            });

            return result;
        }

        /// <summary>
        /// 获取所操作数据库中所有的表名称集合。
        /// </summary>
        /// <returns>当前上下文所操作数据库中所有存在的表名称集合。</returns>
        public override ReadOnlyList<string> GetAllTables()
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            List<string> result = new List<string>();

            ((IDataContext)this).DatabaseOperation.ExecuteReader(((SqlFactory)SqlFactory).GenerateSelectAllTableNameSql(parameterContext), CommandType.Text, parameterContext.ToList(), dr =>
            {
                result.Add((string)dr.GetValue(0));

                return true;
            });

            return result;
        }

        /// <summary>
        /// 创建一个新的参数上下文对象。
        /// </summary>
        /// <param name="enableParametric">是否应当启用参数化操作。</param>
        /// <returns>创建好的参数上下文。</returns>
        public override IParameterContext CreateParameterContext(bool enableParametric)
        {
            return new ParameterContext(enableParametric);
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
        /// 获取系统内置 Master 数据库的操作对象。
        /// </summary>
        /// <returns>用于操作 Master 数据库的对象。</returns>
        private DatabaseOperation GetMasterOperation()
        {
            string connectionString = new Regex(@"(?:((?:initial\s+catalog)|(?:database)))\s*=\s*\S+?(;|$)", RegexOptions.IgnoreCase).Replace(_ConnectionString, "$1=master$2");

            return new DatabaseOperation(connectionString);
        }
    }
}
