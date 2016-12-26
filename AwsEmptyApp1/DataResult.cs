using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsEmptyApp1
{
    /// <summary>
    /// 数据操作结果
    /// </summary>
    public class DataResult
    {
        public DataResult()
        {
            // 默认操作状态码为200，操作结果为成功
            this.Status = 200;
            this.Message = "success";
        }
        /// <summary>
        /// 状态
        /// 200=OK(200是操作成功的唯一状态码)
        /// 201=未找到表对象
        /// 202=操作的对象为null
        /// 204=连接失败
        /// 205=其他错误
        /// </summary>
        public int Status { get; set; }
        /// <summary>
        /// 错误信息
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// 操作返回的Json格式数据
        /// </summary>
        public string JsonResult { get; set; }
        /// <summary>
        /// 异常信息
        /// </summary>
        public Exception Exception { get; set; }
    }
}
