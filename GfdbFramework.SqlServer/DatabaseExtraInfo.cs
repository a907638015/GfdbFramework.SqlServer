using System;
using System.Collections.Generic;
using System.Text;

namespace GfdbFramework.SqlServer
{
    /// <summary>
    /// 数据库额外信息。
    /// </summary>
    public class DatabaseExtraInfo
    {
        /// <summary>
        /// 获取或设置数据库的排序规则。
        /// </summary>
        public string SortRule { get; set; }

        /// <summary>
        /// 获取或设置数据库的恢复模式。
        /// </summary>
        public RecoveryMode RecoveryMode { get; set; }

        /// <summary>
        /// 获取或设置该数据库的兼容级别（100、Sql Server 2008；110、Sql Server 2012；120、Sql Server 2014；130、Sql Server 2016；140、Sql Server 2017；150、Sql Server 2019，兼容模式请自行查阅资料）。
        /// </summary>
        public int CompatibilityLevel { get; set; }

        /// <summary>
        /// 获取或设置该数据库的所有者。
        /// </summary>
        public string Owner { get; set; }
    }
}
