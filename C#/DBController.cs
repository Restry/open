
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Web.Http;
using System.Net;
using System.Threading.Tasks;
using System.Net.Http;
using BOC.DynamicBaseLine.Models;

namespace BOC.DynamicBaseLine.Controllers
{
    public static class NewToJsonExtend
    {
        public static JArray AddtoSelf(this JArray jarry,JArray other)
        {
            if (jarry == null) throw new ArgumentNullException("jarry");
            if (other == null || other.Count < 1) return jarry;

            foreach (var item in other)
            {
                jarry.Add(item);
            }
            return jarry;
        }
    }
    public class DBController : ApiController
    {
        class Parameter
        {
            public string Name { get; set; }
            public SqlDbType DbType { get; set; }
            public int Length { get; set; }
            public byte Scaler { get; set; }
        }

        class Result
        {
            public static Result Error = new Result(-1);
            public static Result Success = new Result(0);
            private readonly int result;
            Result(int result)
            {
                this.result = result;
            }
            public JValue ToJValue()
            {
                return new JValue(result);
            }
        }

        private readonly string _connectionString = ConfigurationManager.ConnectionStrings["DBLMLSingle"].ConnectionString;
        private static readonly ConcurrentDictionary<string, List<Parameter>> outputParameterCollection = new ConcurrentDictionary<string, List<Parameter>>();

        private static readonly ConcurrentDictionary<string, Dictionary<string, DataTable>> tableCollection =
            new ConcurrentDictionary<string, Dictionary<string, DataTable>>();

        #region Public Method

        public IHttpActionResult Options()
        {
            return Ok(); // HTTP 200 response with empty body
        }

        [HttpGet]
        public object Get(string spName, string pars)
        {
            return ExecuteStoreProcedure(spName, pars);
        }


        [HttpPost]
        public object Post(string spName, [FromBody]string pars)
        {
            return ExecuteStoreProcedure(spName, pars);
        }

        [HttpPost]
        public object Post(string spName, string userName, bool needLog, [FromBody]string pars)
        {
            object obj = ExecuteStoreProcedure(spName, pars);

            if (needLog)
            {
                string remark = "存储过程：" + spName + "; 参数" + pars + "; 结果" + obj.ToString();
                //OperationLogHelper.InsertOperationLog(userName, "ResourceManage", spName, remark);
            }
            return obj;
        }

        [HttpPut]
        public object Put(string spName, [FromBody]string pars)
        {
            return ExecuteStoreProcedure(spName, pars);
        }

        [HttpPut]
        public object Put(string spName, string userName, bool needLog, [FromBody]string pars)
        {
            object obj = ExecuteStoreProcedure(spName, pars);

            if (needLog)
            {
                string remark = "存储过程：" + spName + "; 参数" + pars + "; 结果" + obj.ToString();
               // OperationLogHelper.InsertOperationLog(userName, "ResourceManage", spName, remark);
            }
            return obj;
        }

        [HttpDelete]
        public object Delete(string spName, string pars)
        {
            return ExecuteStoreProcedure(spName, pars);
        }

        [HttpDelete]
        public object Delete(string spName, string userName, bool needLog, [FromBody]string pars)
        {
            object obj = ExecuteStoreProcedure(spName, pars);
            if (needLog)
            {
                string remark = "存储过程：" + spName + "; 参数" + pars + "; 结果" + obj.ToString();
               // OperationLogHelper.InsertOperationLog(userName, "ResourceManage", spName, remark);
            }
            return obj;
        }

        public object SavePackage(string spName, string pars, bool needLog, string userName)
        {
            object obj = ExecuteStoreProcedure(spName, pars);

            if (needLog)
            {
                string remark = "存储过程：" + spName + "; 参数" + pars + "; 结果" + obj.ToString();
              //  OperationLogHelper.InsertOperationLog(userName, "ResourceManage", spName, remark);
            }

            return obj;
        }


        #endregion

        #region Private Method



        private object ExecuteStoreProcedure(string spName, string pars)
        {
            JObject jobj = new JObject();
            if (string.IsNullOrEmpty(spName))
            {
                jobj.Add("Result", Result.Error.ToJValue());
                jobj.Add("Message", "存储过程名称不能为空");
            }
            else
            {
                try
                {
                    var cmd = Prepare(spName, pars);

                    jobj = ExecuteCommand(cmd);
                }
                catch (Exception ex)
                {
                    jobj.Add("Result", Result.Error.ToJValue());
                    jobj.Add("Message", ex.ToString());

                    //Log.DataAccessFailed(ex.ToString());
                }
            }
            return jobj;
        }

        private JObject ExecuteCommand(SqlCommand cmd)
        {
            JObject result = new JObject();
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                cmd.Connection = conn;
                try
                {
                    conn.Open();
                    SqlDataReader dbReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                    var outpars = GetOutputParameters(cmd.CommandText);
                    JArray jData = new JArray();
                    do
                    {
                        jData.AddtoSelf(DataReaderToJarray(dbReader));
                    } while (dbReader.NextResult());
                    result.Add("Data", jData);
                    dbReader.Close();//不先关闭SqlDataReader,获取不到返回参数
                    foreach (var item in outpars)
                    {
                        result.Add(item.Name, new JValue(cmd.Parameters[item.Name].Value));
                    }
                    if (result.Property("Result") == null)
                    {
                        result.Add("Result", Result.Success.ToJValue());
                    }
                    else if (result.Property("Result").Value.Type.Equals(JTokenType.Null))
                    {
                        result.Property("Result").Value = Result.Success.ToJValue();
                    }
                }
                catch (SqlException sqlerr)
                {
                    switch (sqlerr.Number)
                    {
                        case 547:   // ForeignKey Violation
                        case 2627:  //Unique Index/Constriant Violation
                        case 2601:  //Unique Index/Constriant Violation
                            result = new JObject { { "Result", Result.Error.ToJValue() }, { "Message", "系统已存在该数据" } };
                            break;
                        default:
                            result = new JObject { { "Result", Result.Error.ToJValue() }, { "Message", sqlerr.ToString() } };
                            break;
                    }
                }
                catch (Exception ex)
                {
                    result = new JObject { { "Result", Result.Error.ToJValue() }, { "Message", ex.ToString() } };
                    throw;
                }
            }
            return result;
        }

        private SqlCommand Prepare(string storeName, string pars)
        {
            SqlCommand cmd = new SqlCommand { CommandType = CommandType.StoredProcedure, CommandText = storeName };
            JObject result = new JObject();

            //Sharepoint站点中会附加 0#.w|，因此处理一下
            //cmd.Parameters.AddWithValue("UserName", User.Identity.Name.Split('|').Last());
            try
            {
                IEnumerable<Parameter> outpars = GetOutputParameters(storeName);
                foreach (var par in outpars)
                {
                    if (cmd.Parameters.Contains(par.Name))
                        cmd.Parameters[par.Name].Direction = ParameterDirection.Output;
                    else
                        cmd.Parameters.Add(new SqlParameter
                        {
                            ParameterName = par.Name,
                            SqlDbType = par.DbType,
                            Direction = ParameterDirection.Output,
                            Size = par.Length,
                            Scale = par.Scaler
                        });
                }

                if (!string.IsNullOrWhiteSpace(pars))
                {
                    using (JsonTextReader jsonReader = new JsonTextReader(new StringReader(pars)))
                    {
                        while (jsonReader.Read())
                        {
                            if (jsonReader.TokenType == JsonToken.PropertyName)
                            {
                                string key = jsonReader.Value.ToString();

                                jsonReader.Read();
                                var value = jsonReader.Value ?? DBNull.Value;
                                cmd.Parameters.AddWithValue(key, value);

                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                result = new JObject { { "Result", Result.Error.ToJValue() }, { "Message", "参数错误。" + ex.ToString() } };
                throw;
            }
            return cmd;
        }

        private IEnumerable<Parameter> GetOutputParameters(string storeName)
        {
            List<Parameter> outputPars;

            if (outputParameterCollection.TryGetValue(storeName, out outputPars))
                return outputPars;
            outputPars = new List<Parameter>();
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                string cmdText = "select PARAMETER_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_SCALE from INFORMATION_SCHEMA.PARAMETERS where SPECIFIC_NAME=@name and PARAMETER_MODE='INOUT' ";
                SqlCommand cmd = new SqlCommand(cmdText, conn);

                cmd.Parameters.AddWithValue("@name", storeName);
                conn.Open();
                var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                while (reader.Read())
                {
                    outputPars.Add(new Parameter
                    {
                        Name = reader.GetString(0).TrimStart('@'),
                        DbType = (SqlDbType)Enum.Parse(typeof(SqlDbType), reader.GetString(1), true),
                        Length = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                        Scaler = reader.IsDBNull(3) ? (byte)0 : Convert.ToByte(reader.GetInt32(3))//不可以直接reader.GetByte(),
                    });
                }
            }
            outputParameterCollection.TryAdd(storeName, outputPars);
            return outputPars;
        }

        private JArray DataReaderToJarray(SqlDataReader dbReader)
        {
            JArray jarray = new JArray();
            while (dbReader.Read())
            {
                JObject jobj = new JObject();
                for (int i = 0; i < dbReader.FieldCount; i++)
                {
                    string name = dbReader.GetName(i);
                    jobj.Add(name, new JValue(dbReader.GetValue(i)));
                }
                jarray.Add(jobj);
            }
            return jarray;
        }

        #endregion
    }
}
