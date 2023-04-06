using System;
using System.Collections.Generic;
using System.Text;

namespace GfdbFramework.SqlServer
{
    /// <summary>
    /// Sql Server 数据库恢复模式枚举。
    /// </summary>
    public enum RecoveryMode
    {
        /// <summary>
        /// 默认。
        /// </summary>
        Default = 0,
        /// <summary>
        /// 简单模式。
        /// </summary>
        Simple = 1,
        /// <summary>
        /// 大容量日志。
        /// </summary>
        BulkLog = 2,
        /// <summary>
        /// 完整模式。
        /// </summary>
        Full = 3
    }
}
