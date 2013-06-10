using System;
using System.Configuration;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using log4net.Appender;
using log4net.Core;

namespace Log4Mongo
{
    sealed class MongoDbRef
    {
        public static MongoDbRef Instance { get { return Inner.SINGLETON; } }
        class Inner { static internal readonly MongoDbRef SINGLETON = new MongoDbRef(); }

        const string DEFAULT_DB = "log4net";
        const string DEFAULT_CONNECTION = "mongodb://localhost/";

        readonly ConcurrentDictionary<Uri, MongoDatabase> _dbmap = new ConcurrentDictionary<Uri, MongoDatabase>();
        readonly ConcurrentDictionary<string, bool> _colmap = new ConcurrentDictionary<string, bool>();
        private MongoDbRef()
        {
            
        }

        static Uri GetConnectionString(MongoDBAppender appender)
        {
            if (appender == null)
                throw new ArgumentNullException("appender");

            Uri u = new Uri(string.IsNullOrWhiteSpace(appender.ConnectionString) ? DEFAULT_CONNECTION + DEFAULT_DB : appender.ConnectionString);
            return u;
        }

        public MongoDatabase GetDB(MongoDBAppender appender)
        {
            Uri u = GetConnectionString(appender);
            MongoDatabase db;
            if (_dbmap.TryGetValue(u, out db) && db != null)
                return db;
            else
            {
                MongoUrl url = MongoUrl.Create(u.ToString());
                MongoServerSettings settings = MongoServerSettings.FromUrl(url);
                lock (_dbmap) //lock only during creation...
                {
                    var conn = new MongoServer(settings);
                    string dbname = string.IsNullOrWhiteSpace(url.DatabaseName) ? DEFAULT_DB : url.DatabaseName;
                    db = conn.GetDatabase(dbname);
                    EnableSharding(db);
                    return _dbmap.AddOrUpdate(u, db, (uri, currentDb) => db); //replaces existing ones...
                }
            }
        }

        public MongoCollection GetCollection(MongoDBAppender appender)
        {
            MongoDatabase db = GetDB(appender);
            if (db == null)
                throw new ApplicationException("can not retreive db");

            string colName = string.IsNullOrWhiteSpace(appender.CollectionName) ? "logs" : appender.CollectionName;
            MongoCollection collection = db.GetCollection(colName);

            if (!string.IsNullOrWhiteSpace(appender.ShardKey))
            {
                Uri u = GetConnectionString(appender);
                string col = u + "#" + colName;
                bool ok;
                if (!_colmap.TryGetValue(col, out ok) || !ok)
                {
                    lock (_colmap)
                    {
                        _colmap.AddOrUpdate(col, true, (s, b) => true);
                        ShardCollection(collection, appender);
                    }
                }
            }
            return collection;
        }

        #region sharding option

        static void EnableSharding(MongoDatabase db)
        {
            var cmd = new CommandDocument("enablesharding", db.Name);
            try
            {
                var dbCfg = new MongoDatabaseSettings(db.Server, "admin");
                var adminDb = new MongoDatabase(db.Server, dbCfg);
                db.Server.Connect();
                adminDb.RunCommand(cmd);
            }
            catch (MongoCommandException mex)
            {
                if (!mex.Message.ToLower().Contains("already enabled"))
                    throw;
                else
                    Console.WriteLine("Sharding already enabled!");
            }
        }

        static void ShardCollection(MongoCollection col, MongoDBAppender appender)
        {
            var cmd = new CommandDocument
            {
                { "shardCollection", col.Database.Name + '.' + col.Name },
                { "key", new BsonDocument(appender.ShardKey, 1) },
            };
            try
            {
                CreateIndex(col, appender.ShardKey);

                MongoServer server = col.Database.Server;
                var dbCfg = new MongoDatabaseSettings(server, "admin");
                var adminDb = new MongoDatabase(server, dbCfg);
                server.Connect();
                CommandResult shcr = adminDb.RunCommand(cmd);
            }
            catch (MongoCommandException mex)
            {
                if (!mex.Message.ToLower().Contains("already "))
                    throw;
                else
                    Console.WriteLine("Sharding already enabled!");
            }
        }

        static void CreateIndex(MongoCollection col, string field)
        {
            if (field == "_id") //create proper index first!
                return;

            col.EnsureIndex(IndexKeys.Ascending(field), IndexOptions.SetBackground(true));
        }

        #endregion
    }
}
