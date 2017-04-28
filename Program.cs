using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisToDB
{
    class Program
    {
         IDatabase db = SeRedis.redis.GetDatabase();
        static void Main(string[] args)
        {
            if (ConfigurationManager.AppSettings["AutoRun"].ToString().Trim() == "Y")
            {
               // redistotable();
                Program p = new Program();

                int count = Convert.ToInt32(ConfigurationManager.AppSettings["count"].ToString());
                for (int i = 0; i < count; i++)
                {
                    p.redistotable();
                }                
            }
        }

        private  void redistotable()
        {
            string sql = string.Empty;
            if (db.KeyExists("declareall"))
            {
                string json = db.ListLeftPop("declareall");
                if (!string.IsNullOrEmpty(json))
                {
                    try
                    {
                        JObject jo = (JObject)JsonConvert.DeserializeObject(json);
                        sql = @"insert into redis_declareall (ID,DECLARATIONCODE,TRADECODE,TRANSNAME,GOODSNUM,GOODSGW,SHEETNUM,COMMODITYNUM,
                              CUSTOMSSTATUS,MODIFYFLAG,PREDECLCODE,CUSNO,OLDDECLARATIONCODE,ISDEL,DIVIDEREDISKEY) values (REDIS_DECLAREALL_ID.Nextval,
                              '{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')";
                        sql = string.Format(sql, jo.Value<string>("DECLARATIONCODE"), jo.Value<string>("TRADECODE"), jo.Value<string>("TRANSNAME"),
                            jo.Value<string>("GOODSNUM"), jo.Value<string>("GOODSGW"), jo.Value<string>("SHEETNUM"), jo.Value<string>("COMMODITYNUM"),
                            jo.Value<string>("CUSTOMSSTATUS"), jo.Value<string>("MODIFYFLAG"), jo.Value<string>("PREDECLCODE"), jo.Value<string>("CUSNO"),
                            jo.Value<string>("OLDDECLARATIONCODE"), jo.Value<string>("ISDEL"), jo.Value<string>("DIVIDEREDISKEY"));
                        DBMgr.ExecuteNonQuery(sql);
                    }
                    catch (Exception ex)
                    {
                        db.ListRightPush("declareall", json);
                    }
                }
            }
            if (db.KeyExists("inspectionall"))
            {
                string json = db.ListLeftPop("inspectionall");
                if (!string.IsNullOrEmpty(json))
                {
                    try
                    {
                        JObject jo = (JObject)JsonConvert.DeserializeObject(json);
                        sql = @"insert into REDIS_INSPECTIONALL (ID,APPROVALCODE,INSPECTIONCODE,TRADEWAY,CLEARANCECODE,SHEETNUM,
                            COMMODITYNUM,INSPSTATUS,MODIFYFLAG,PREINSPCODE,CUSNO, OLDINSPECTIONCODE ,ISDEL,ISNEEDCLEARANCE,
                            LAWFLAG,DIVIDEREDISKEY) values (REDIS_INSPECTIONALL_ID.Nextval,
                            '{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}')";
                        sql = string.Format(sql, jo.Value<string>("APPROVALCODE"), jo.Value<string>("INSPECTIONCODE"), jo.Value<string>("TRADEWAY"),
                            jo.Value<string>("CLEARANCECODE"), jo.Value<string>("SHEETNUM"), jo.Value<string>("COMMODITYNUM"), jo.Value<string>("INSPSTATUS"),
                            jo.Value<string>("MODIFYFLAG"), jo.Value<string>("PREINSPCODE"), jo.Value<string>("CUSNO"), jo.Value<string>("OLDINSPECTIONCODE"),
                            jo.Value<string>("ISDEL"), jo.Value<string>("ISNEEDCLEARANCE"), jo.Value<string>("LAWFLAG"), jo.Value<string>("DIVIDEREDISKEY"));
                        DBMgr.ExecuteNonQuery(sql);
                    }
                    catch (Exception ex)
                    {
                        db.ListRightPush("inspectionall", json);
                    }
                }
            }
            if (db.KeyExists("statuslogall"))
            {
                string json = db.ListLeftPop("statuslogall");
                if (!string.IsNullOrEmpty(json))
                {
                    try
                    {
                        JObject jo = (JObject)JsonConvert.DeserializeObject(json);
                        sql = @"insert into redis_statuslogall (ID,TYPE,CUSNO,STATUSCODE,STATUSVALUE,DIVIDEREDISKEY) values (REDIS_STATUSLOGALL_ID.Nextval,
                            '{0}','{1}','{2}','{3}','{4}')";
                        sql = string.Format(sql, jo.Value<string>("TYPE"), jo.Value<string>("CUSNO"), jo.Value<string>("STATUSCODE"),
                            jo.Value<string>("STATUSVALUE"), jo.Value<string>("DIVIDEREDISKEY"));
                        DBMgr.ExecuteNonQuery(sql);
                    }
                    catch (Exception ex)
                    {
                        db.ListRightPush("statuslogall", json);
                    }
                }
            }
        }



    }
}
